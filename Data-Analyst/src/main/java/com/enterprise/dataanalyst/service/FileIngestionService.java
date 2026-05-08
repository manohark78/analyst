package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.DatasetInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles file upload, ingestion into DuckDB, and dataset registry.
 * Supports CSV and multi-sheet Excel files.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FileIngestionService {

    private final DuckDBService duckDBService;
    private final ExcelToCsvConverter excelConverter;

    @Value("${app.storage.upload-dir}")
    private String uploadDir;

    // In-memory dataset registry (persisted to DuckDB in Phase 4)
    private final Map<String, DatasetInfo> datasetRegistry = new ConcurrentHashMap<>();

    /**
     * Ingest a file. For Excel files, ALL sheets are ingested as separate tables.
     * Returns a list of DatasetInfo (one per sheet for Excel, one for CSV).
     */
    public List<DatasetInfo> ingestFile(MultipartFile file) throws IOException, SQLException {
        String originalFilename = file.getOriginalFilename();
        String fileExtension = getFileExtension(originalFilename);
        String fileId = UUID.randomUUID().toString().substring(0, 8);

        Path uploadPath = Paths.get(uploadDir);
        if (!Files.exists(uploadPath)) {
            Files.createDirectories(uploadPath);
        }

        Path filePath = uploadPath.resolve(fileId + "_" + originalFilename);
        Files.copy(file.getInputStream(), filePath);
        log.info("Saved uploaded file: {}", filePath);

        List<DatasetInfo> datasets = new ArrayList<>();

        if (fileExtension.equalsIgnoreCase("csv")) {
            String tableName = "data_" + fileId;
            ingestCsvToDuckDB(filePath, tableName);
            DatasetInfo info = buildDatasetMetadata(tableName, originalFilename, filePath.toString(), fileExtension);
            datasetRegistry.put(info.getId(), info);
            datasets.add(info);

        } else if (fileExtension.equalsIgnoreCase("xlsx") || fileExtension.equalsIgnoreCase("xls")) {
            List<ExcelToCsvConverter.SheetCsv> sheets = excelConverter.convertAllSheets(filePath);

            for (ExcelToCsvConverter.SheetCsv sheet : sheets) {
                String tableName = "data_" + fileId + "_" + sheet.sheetName();
                ingestCsvToDuckDB(sheet.csvPath(), tableName);

                String displayName = originalFilename + " → " + sheet.sheetName();
                DatasetInfo info = buildDatasetMetadata(tableName, displayName, filePath.toString(), fileExtension);
                datasetRegistry.put(info.getId(), info);
                datasets.add(info);

                // Clean up temp CSV
                Files.deleteIfExists(sheet.csvPath());
            }

        } else {
            Files.deleteIfExists(filePath);
            throw new IllegalArgumentException("Unsupported file type: " + fileExtension + ". Supported: csv, xlsx, xls");
        }

        log.info("Ingested {} dataset(s) from {}", datasets.size(), originalFilename);
        return datasets;
    }

    private void ingestCsvToDuckDB(Path csvPath, String tableName) throws SQLException {
        String escapedPath = csvPath.toString().replace("\\", "/");
        String sql = String.format(
                "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true, ignore_errors=true)",
                tableName, escapedPath);
        duckDBService.execute(sql);
        log.info("Created DuckDB table: {}", tableName);
    }

    private DatasetInfo buildDatasetMetadata(String tableName, String displayName, String filePath, String type) throws SQLException {
        long rowCount = 0;
        try (Statement stmt = duckDBService.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            if (rs.next()) rowCount = rs.getLong(1);
        }

        List<DatasetInfo.ColumnInfo> columns = new ArrayList<>();
        try (Statement stmt = duckDBService.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 0")) {
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(DatasetInfo.ColumnInfo.builder()
                        .name(meta.getColumnName(i))
                        .type(meta.getColumnTypeName(i))
                        .nullable(true)
                        .build());
            }
        }

        // Fetch sample values for each column (first 5 distinct values)
        List<Map<String, Object>> sampleData = duckDBService.query(
                "SELECT * FROM " + tableName + " LIMIT 5");

        return DatasetInfo.builder()
                .id(UUID.randomUUID().toString().substring(0, 8))
                .name(displayName)
                .tableName(tableName)
                .filePath(filePath)
                .fileType(type)
                .rowCount(rowCount)
                .columnCount(columns.size())
                .columns(columns)
                .sampleData(sampleData)
                .uploadedAt(LocalDateTime.now())
                .build();
    }

    // --- Registry Access ---

    public DatasetInfo getDataset(String id) {
        return datasetRegistry.get(id);
    }

    public Collection<DatasetInfo> getAllDatasets() {
        return datasetRegistry.values();
    }

    public DatasetInfo getDatasetByTable(String tableName) {
        return datasetRegistry.values().stream()
                .filter(d -> d.getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    public void removeDataset(String id) throws SQLException {
        DatasetInfo info = datasetRegistry.remove(id);
        if (info != null) {
            duckDBService.execute("DROP TABLE IF EXISTS " + info.getTableName());
            log.info("Dropped table and removed dataset: {}", info.getName());
        }
    }

    private String getFileExtension(String filename) {
        if (filename == null || !filename.contains(".")) return "";
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
}
