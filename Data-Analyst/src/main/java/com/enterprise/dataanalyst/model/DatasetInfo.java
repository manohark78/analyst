package com.enterprise.dataanalyst.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class DatasetInfo {
    private String id;
    private String name;
    private String tableName;
    private String filePath;
    private String fileType;
    private long rowCount;
    private int columnCount;
    private List<ColumnInfo> columns;
    private List<Map<String, Object>> sampleData;
    private LocalDateTime uploadedAt;

    @Data
    @Builder
    public static class ColumnInfo {
        private String name;
        private String type;
        private boolean nullable;
    }
}
