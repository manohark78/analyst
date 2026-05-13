package com.enterprise.dataanalyst.state;

import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

@Data
@Builder
public class AnalyticalContext {
    private String conversationId;
    @Builder.Default
    private Set<String> activeDatasetIds = new HashSet<>();
    private String activeTableName; // E.g., specific sheet or table
    private String lastSuccessfulSql;
    
    @Builder.Default
    private List<String> currentFilters = new ArrayList<>();
    
    private String currentSort;
    private Integer currentLimit;
    
    public void resetAnalyticalState() {
        this.lastSuccessfulSql = null;
        this.currentFilters.clear();
        this.currentSort = null;
        this.currentLimit = null;
    }
}
