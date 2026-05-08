package com.enterprise.dataanalyst.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts ALL sheets in an Excel file to individual CSV files.
 * Each sheet becomes a separate CSV → separate DuckDB table.
 */
@Slf4j
@Service
public class ExcelToCsvConverter {

    public record SheetCsv(String sheetName, Path csvPath) {}

    /**
     * Converts every sheet in the given Excel file to a separate CSV.
     * Returns a list of (sheetName, csvPath) pairs.
     */
    public List<SheetCsv> convertAllSheets(Path excelPath) throws IOException {
        List<SheetCsv> results = new ArrayList<>();

        try (Workbook workbook = WorkbookFactory.create(excelPath.toFile())) {
            int sheetCount = workbook.getNumberOfSheets();
            log.info("Excel file {} has {} sheet(s)", excelPath.getFileName(), sheetCount);

            for (int s = 0; s < sheetCount; s++) {
                Sheet sheet = workbook.getSheetAt(s);
                String sheetName = sanitizeSheetName(sheet.getSheetName());

                // Skip empty sheets
                if (sheet.getPhysicalNumberOfRows() <= 1) {
                    log.info("Skipping empty sheet: {}", sheetName);
                    continue;
                }

                Path csvPath = excelPath.resolveSibling(
                        excelPath.getFileName().toString() + "_" + sheetName + ".csv");

                convertSheet(sheet, csvPath);
                results.add(new SheetCsv(sheetName, csvPath));
                log.info("Converted sheet '{}' → {}", sheetName, csvPath.getFileName());
            }
        }

        return results;
    }

    private void convertSheet(Sheet sheet, Path csvPath) throws IOException {
        DataFormatter formatter = new DataFormatter();

        try (BufferedWriter writer = Files.newBufferedWriter(csvPath);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            for (Row row : sheet) {
                int lastCol = row.getLastCellNum();
                if (lastCol < 0) continue;

                List<String> values = new ArrayList<>();
                for (int i = 0; i < lastCol; i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    values.add(formatter.formatCellValue(cell));
                }
                csvPrinter.printRecord(values);
            }
            csvPrinter.flush();
        }
    }

    /**
     * Sanitize sheet name for use as a DuckDB table suffix.
     */
    private String sanitizeSheetName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }
}
