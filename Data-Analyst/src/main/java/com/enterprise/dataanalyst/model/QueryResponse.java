package com.enterprise.dataanalyst.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class QueryResponse {
    private List<String> columns;
    private List<List<String>> rows;
    private int rowCount;
    private String generatedSql;
    private String interpretationSummary;
    private long executionTimeMs;
    private String message;
}
