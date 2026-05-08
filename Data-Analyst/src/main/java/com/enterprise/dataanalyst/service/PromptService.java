package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.DatasetInfo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Centralized prompt engineering for all LLM interactions.
 * Each prompt is carefully designed to prevent hallucination and enforce safety.
 */
@Service
public class PromptService {

    public String getSystemPrompt() {
        return """
            You are a Senior AI Data Analyst assistant. You help users understand their datasets.
            You are running fully offline inside a local desktop application.
            Be professional, precise, and analytical. Use markdown formatting for clarity.
            When presenting numbers, use proper formatting (commas, percentages, etc).
            Always base your answers on the actual data provided — never guess or hallucinate.
            """;
    }

    public String getChitchatSystemPrompt() {
        return """
            You are a friendly AI Data Analyst assistant. Respond naturally to greetings and casual conversation.
            Keep responses short and warm. If the user hasn't uploaded data yet, gently suggest they upload a CSV or Excel file.
            You work fully offline and all data stays private on their machine.
            """;
    }

    public String getSqlGenerationPrompt(String userQuery, DatasetInfo dataset) {
        String schema = dataset.getColumns().stream()
                .map(c -> String.format("  - %s (%s)", c.getName(), c.getType()))
                .collect(Collectors.joining("\n"));

        String sampleRows = "";
        if (dataset.getSampleData() != null && !dataset.getSampleData().isEmpty()) {
            sampleRows = "\nSample data (first 3 rows):\n" +
                    dataset.getSampleData().stream()
                            .limit(3)
                            .map(Object::toString)
                            .collect(Collectors.joining("\n"));
        }

        return String.format("""
            Generate a DuckDB SQL query for the following request.
            
            TABLE NAME: %s
            ROW COUNT: %d
            COLUMNS:
            %s
            %s
            
            STRICT RULES:
            1. Use ONLY the exact column names listed above. Do NOT invent columns.
            2. Use DuckDB SQL syntax.
            3. Return ONLY the raw SQL query — no explanations, no markdown, no backticks.
            4. Only SELECT statements allowed. No INSERT, UPDATE, DELETE, DROP.
            5. Use case-insensitive comparisons with ILIKE when filtering text.
            6. For aggregations, always include meaningful aliases.
            7. Limit results to 100 rows unless the user specifies otherwise.
            8. If the request is ambiguous, make a reasonable assumption and include a LIMIT.
            
            USER REQUEST: "%s"
            
            SQL:""", dataset.getTableName(), dataset.getRowCount(), schema, sampleRows, userQuery);
    }

    public String getInterpretationPrompt(String userQuery, String sql, String dataJson, int rowCount) {
        return String.format("""
            The user asked a question about their data. A database query was ALREADY executed successfully.
            Your job is to answer the user's question using ONLY the provided JSON data results.
            
            USER QUESTION: "%s"
            
            --- QUERY RESULTS START ---
            ROWS MATCHED: %d
            JSON DATA:
            %s
            --- QUERY RESULTS END ---
            
            CRITICAL RULES:
            1. DO NOT output SQL queries. The query was already executed.
            2. DO NOT tell the user to run a query.
            3. Answer the user's question directly using the JSON data provided above.
            4. If the JSON data contains the answer, present it clearly (use a markdown table or bullet points).
            5. If the JSON data is empty (ROWS MATCHED: 0), tell the user no matching data was found.
            """, userQuery, rowCount, dataJson);
    }

    public String getDataDiscoveryPrompt(DatasetInfo dataset) {
        String schema = dataset.getColumns().stream()
                .map(c -> String.format("| %s | %s |", c.getName(), c.getType()))
                .collect(Collectors.joining("\n"));

        return String.format("""
            Describe this dataset to the user in a clear, professional way.
            
            Dataset: %s
            Table: %s
            Rows: %d
            Columns: %d
            
            | Column Name | Type |
            |-------------|------|
            %s
            
            Provide:
            1. A brief overview of what this dataset likely contains.
            2. The column listing in a clean markdown table.
            3. Suggested questions the user could ask about this data.
            """, dataset.getName(), dataset.getTableName(), dataset.getRowCount(),
                dataset.getColumnCount(), schema);
    }

    public String getVisualizationPrompt(String userQuery, DatasetInfo dataset) {
        String columns = dataset.getColumns().stream()
                .map(c -> c.getName() + " (" + c.getType() + ")")
                .collect(Collectors.joining(", "));

        return String.format("""
            The user wants a visualization. Generate a DuckDB SQL query that produces data suitable for charting.
            
            TABLE: %s
            COLUMNS: %s
            
            USER REQUEST: "%s"
            
            RULES:
            1. Return ONLY the SQL query.
            2. The result should have 2-3 columns: labels/categories and numeric values.
            3. Order results meaningfully (by value DESC or by date ASC).
            4. Limit to 20 data points for clean charts.
            5. Use aliases that describe the axis (e.g., "category", "total_revenue").
            
            SQL:""", dataset.getTableName(), columns, userQuery);
    }

    public String getFollowUpPrompt(String userQuery, List<Map<String, String>> conversationContext) {
        StringBuilder ctx = new StringBuilder();
        for (Map<String, String> msg : conversationContext) {
            ctx.append(msg.get("role").toUpperCase()).append(": ").append(msg.get("content")).append("\n");
        }

        return String.format("""
            The user is asking a follow-up question based on the previous conversation.
            
            CONVERSATION CONTEXT:
            %s
            
            CURRENT QUESTION: "%s"
            
            Resolve any references like "it", "them", "those", "the same", "previous" using the conversation context.
            Then provide a clear, analytical response.
            """, ctx.toString(), userQuery);
    }
}
