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
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FileIngestionService {

    private final DuckDBService duckDBService;
    private final ExcelToCsvConverter excelConverter;

    @Value("${app.storage.upload-dir}")
    private String uploadDir;

    private final Map<String, DatasetInfo> datasetRegistry = new ConcurrentHashMap<>();

    /**
     * Ingest a file. For Excel files, ALL sheets are ingested as separate tables within ONE DatasetInfo.
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

        List<DatasetInfo.TableInfo> tables = new ArrayList<>();

        if (fileExtension.equalsIgnoreCase("csv")) {
            String tableName = "data_" + fileId;
            ingestCsvToDuckDB(filePath, tableName);
            tables.add(buildTableMetadata(tableName, "Default"));

        } else if (fileExtension.equalsIgnoreCase("xlsx") || fileExtension.equalsIgnoreCase("xls")) {
            List<ExcelToCsvConverter.SheetCsv> sheets = excelConverter.convertAllSheets(filePath);
            for (ExcelToCsvConverter.SheetCsv sheet : sheets) {
                String tableName = "data_" + fileId + "_" + sheet.sheetName().replaceAll("[^a-zA-Z0-9]", "_");
                ingestCsvToDuckDB(sheet.csvPath(), tableName);
                tables.add(buildTableMetadata(tableName, sheet.sheetName()));
                Files.deleteIfExists(sheet.csvPath());
            }
        } else {
            Files.deleteIfExists(filePath);
            throw new IllegalArgumentException("Unsupported file type: " + fileExtension);
        }

        DatasetInfo dataset = DatasetInfo.builder()
                .id(fileId)
                .name(originalFilename)
                .filePath(filePath.toString())
                .fileType(fileExtension)
                .uploadedAt(LocalDateTime.now())
                .tables(tables)
                .build();

        datasetRegistry.put(dataset.getId(), dataset);
        log.info("Ingested dataset '{}' with {} table(s)", originalFilename, tables.size());
        
        return List.of(dataset);
    }

    private void ingestCsvToDuckDB(Path csvPath, String tableName) throws SQLException {
        String escapedPath = csvPath.toString().replace("\\", "/");
        String sql = String.format(
                "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true, ignore_errors=true)",
                tableName, escapedPath);
        duckDBService.execute(sql);
    }

    private DatasetInfo.TableInfo buildTableMetadata(String tableName, String sheetName) throws SQLException {
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

        List<Map<String, Object>> sampleData = duckDBService.query("SELECT * FROM " + tableName + " LIMIT 5");

        return DatasetInfo.TableInfo.builder()
                .tableName(tableName)
                .sheetName(sheetName)
                .rowCount(rowCount)
                .columnCount(columns.size())
                .columns(columns)
                .sampleData(sampleData)
                .build();
    }

    public DatasetInfo getDataset(String id) { return datasetRegistry.get(id); }
    public List<DatasetInfo> getDatasets(Collection<String> ids) {
        if (ids == null) return List.of();
        return ids.stream()
                .map(datasetRegistry::get)
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toList());
    }
    public Collection<DatasetInfo> getAllDatasets() { return datasetRegistry.values(); }

    public void removeDataset(String id) throws SQLException {
        DatasetInfo info = datasetRegistry.remove(id);
        if (info != null) {
            for (DatasetInfo.TableInfo table : info.getTables()) {
                duckDBService.execute("DROP TABLE IF EXISTS " + table.getTableName());
            }
        }
    }

    private String getFileExtension(String filename) {
        if (filename == null || !filename.contains(".")) return "";
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
}
