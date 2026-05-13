package com.enterprise.dataanalyst.semantic;

import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.service.LlmClient;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema Grounder — maps user terms to the correct tables and columns.
 * Supports multi-table datasets (e.g., multi-sheet Excel).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SchemaGrounder {

    private final LlmClient llmClient;

    @Data
    @Builder
    public static class GroundedSchema {
        private DatasetInfo.TableInfo table;
        private List<DatasetInfo.ColumnInfo> columns;
    }

    public GroundedSchema ground(String query, List<DatasetInfo> datasets) {
        if (!llmClient.isModelLoaded() || datasets.isEmpty()) {
            DatasetInfo first = datasets.isEmpty() ? null : datasets.get(0);
            return GroundedSchema.builder()
                    .table(first != null ? first.getMainTable() : null)
                    .columns(first != null ? first.getColumns() : List.of())
                    .build();
        }

        // 1. Describe all tables in the workspace to the LLM
        StringBuilder schemaSummary = new StringBuilder();
        List<DatasetInfo.TableInfo> allTables = new ArrayList<>();

        for (DatasetInfo dataset : datasets) {
            for (DatasetInfo.TableInfo table : dataset.getTables()) {
                allTables.add(table);
                schemaSummary.append(String.format("File: %s | Table: %s (Sheet: %s)\nColumns: %s\n\n",
                        dataset.getName(),
                        table.getTableName(),
                        table.getSheetName(),
                        table.getColumns().stream().map(DatasetInfo.ColumnInfo::getName).collect(Collectors.joining(", "))));
            }
        }

        String prompt = String.format("""
                Identify which table and columns are most relevant to the user's question from the analytical workspace.
                
                USER QUESTION: "%s"
                
                AVAILABLE SCHEMA:
                %s
                
                Return ONLY a comma-separated list where the first item is the Table name, and the rest are Column names.
                Example: data_123_Sales, Date, Revenue, Region
                
                RESPONSE:""", query, schemaSummary.toString());

        try {
            String result = llmClient.complete(List.of(
                    Map.of("role", "system", "content", "You are a schema mapper. Return only TableName, Col1, Col2..."),
                    Map.of("role", "user", "content", prompt)
            )).trim();

            List<String> parts = Arrays.stream(result.split(",")).map(String::trim).collect(Collectors.toList());
            if (parts.isEmpty()) throw new RuntimeException("Empty grounding response");

            String tableName = parts.get(0);
            List<String> colNames = parts.subList(1, parts.size());

            DatasetInfo.TableInfo matchedTable = allTables.stream()
                    .filter(t -> t.getTableName().equalsIgnoreCase(tableName) || t.getSheetName().equalsIgnoreCase(tableName))
                    .findFirst()
                    .orElse(allTables.get(0));

            List<DatasetInfo.ColumnInfo> matchedColumns = matchedTable.getColumns().stream()
                    .filter(c -> colNames.stream().anyMatch(name -> c.getName().equalsIgnoreCase(name)))
                    .collect(Collectors.toList());

            if (matchedColumns.isEmpty()) matchedColumns = matchedTable.getColumns();

            return GroundedSchema.builder().table(matchedTable).columns(matchedColumns).build();

        } catch (Exception e) {
            log.error("Multi-dataset grounding failed", e);
            DatasetInfo.TableInfo fallbackTable = allTables.get(0);
            return GroundedSchema.builder().table(fallbackTable).columns(fallbackTable.getColumns()).build();
        }
    }
}
