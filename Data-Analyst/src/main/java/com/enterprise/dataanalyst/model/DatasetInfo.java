package com.enterprise.dataanalyst.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Represents an uploaded file and all its logical tables.
 * For CSVs, it contains 1 table. For Excel, it can contain many (one per sheet).
 */
@Getter
@Setter
@Builder
public class DatasetInfo {
    private String id;
    private String name;
    private String filePath;
    private String fileType;
    private LocalDateTime uploadedAt;
    
    private List<TableInfo> tables;

    @Getter
    @Setter
    @Builder
    public static class TableInfo {
        private String tableName;
        private String sheetName;
        private long rowCount;
        private int columnCount;
        private List<ColumnInfo> columns;
        private List<Map<String, Object>> sampleData;
    }
    @Getter
    @Setter
    @Builder
    public static class ColumnInfo {
        private String name;
        private String type;
        private boolean nullable;
    }

    // --- Convenience Accessors (for single-table logic) ---

    public TableInfo getMainTable() {
        if (tables == null || tables.isEmpty()) return null;
        return tables.get(0);
    }

    public List<ColumnInfo> getColumns() {
        TableInfo main = getMainTable();
        return main != null ? main.getColumns() : List.of();
    }

    public String getTableName() {
        TableInfo main = getMainTable();
        return main != null ? main.getTableName() : null;
    }

    public long getRowCount() {
        TableInfo main = getMainTable();
        return main != null ? main.getRowCount() : 0;
    }

    public int getColumnCount() {
        TableInfo main = getMainTable();
        return main != null ? main.getColumnCount() : 0;
    }

    public List<Map<String, Object>> getSampleData() {
        TableInfo main = getMainTable();
        return main != null ? main.getSampleData() : List.of();
    }
}
