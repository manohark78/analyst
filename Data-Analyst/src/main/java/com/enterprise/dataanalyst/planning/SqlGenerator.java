package com.enterprise.dataanalyst.planning;

import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.service.LlmClient;
import com.enterprise.dataanalyst.state.AnalyticalContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL Generator — responsible for drafting DuckDB queries.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SqlGenerator {

    private final LlmClient llmClient;

    public String generate(String query, DatasetInfo.TableInfo table, List<DatasetInfo.ColumnInfo> groundedColumns, AnalyticalContext context) {
        String schema = groundedColumns.stream()
                .map(c -> String.format("  - %s (%s)", c.getName(), c.getType()))
                .collect(Collectors.joining("\n"));

        String lastSql = context.getLastSuccessfulSql();
        boolean hasContext = lastSql != null && !lastSql.isBlank();

        String sampleData = "";
        if (table.getSampleData() != null && !table.getSampleData().isEmpty()) {
            sampleData = "\nSAMPLE DATA (first 3 rows):\n" + 
                table.getSampleData().stream().limit(3).map(Object::toString).collect(Collectors.joining("\n"));
        }

        String prompt;
        if (hasContext) {
            prompt = String.format("""
                Generate a DuckDB SQL query to answer the user's follow-up question.
                
                PREVIOUS QUERY:
                ```sql
                %s
                ```
                
                USER FOLLOW-UP: "%s"
                %s
                
                INSTRUCTIONS:
                1. Wrap the PREVIOUS QUERY in a CTE named 'base_context'.
                2. Select from 'base_context' to apply the new filters/sorting.
                3. Use only these columns:
                %s
                4. DIRTY DATA RULE: If a column contains symbols like '₹', '$', or ',', use `regexp_replace(col, '[^0-9.]', '', 'g')::DOUBLE` before numeric comparisons.
                5. Return ONLY the raw SQL.
                
                SQL:""", lastSql, query, sampleData, schema);
        } else {
            prompt = String.format("""
                Generate a DuckDB SQL query.
                
                USER REQUEST: "%s"
                TABLE NAME: %s
                COLUMNS:
                %s
                %s
                
                RULES:
                1. Use ONLY the columns listed above.
                2. Use DuckDB syntax.
                3. DIRTY DATA RULE: If a column contains symbols like '₹', '$', or ',', use `regexp_replace(col, '[^0-9.]', '', 'g')::DOUBLE` before numeric comparisons.
                4. Return ONLY raw SQL.
                
                SQL:""", query, table.getTableName(), schema, sampleData);
        }

        String sql = llmClient.complete(List.of(
                Map.of("role", "system", "content", "You are a SQL generator."),
                Map.of("role", "user", "content", prompt)
        )).trim();

        if (sql.startsWith("```")) {
            sql = sql.replaceAll("(?s)```(sql)?(.*?)```", "$2").trim();
        }
        return sql;
    }
}
