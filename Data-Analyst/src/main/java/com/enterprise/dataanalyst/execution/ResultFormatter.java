package com.enterprise.dataanalyst.execution;

import com.enterprise.dataanalyst.model.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Result Formatter — handles the conversion of raw SQL results into a UI-friendly tabular format.
 */
@Slf4j
@Service
public class ResultFormatter {

    public QueryResponse format(List<Map<String, Object>> rawResults, String sql, long timeMs) {
        if (rawResults == null || rawResults.isEmpty()) {
            return QueryResponse.builder()
                    .columns(List.of())
                    .rows(List.of())
                    .rowCount(0)
                    .generatedSql(sql)
                    .executionTimeMs(timeMs)
                    .message("No results found.")
                    .build();
        }

        // Get columns from the first row keys
        List<String> columns = new ArrayList<>(rawResults.get(0).keySet());

        // Convert rows to List of Strings
        List<List<String>> rows = new ArrayList<>();
        for (Map<String, Object> rawRow : rawResults) {
            List<String> row = new ArrayList<>();
            for (String col : columns) {
                row.add(formatValue(rawRow.get(col)));
            }
            rows.add(row);
        }

        return QueryResponse.builder()
                .columns(columns)
                .rows(rows)
                .rowCount(rows.size())
                .generatedSql(sql)
                .executionTimeMs(timeMs)
                .build();
    }

    private String formatValue(Object value) {
        if (value == null) return "NULL";
        if (value instanceof Double) {
            double d = (Double) value;
            // Clean up whole numbers displayed as .0
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return String.valueOf((long) d);
            }
        }
        return String.valueOf(value);
    }
}
