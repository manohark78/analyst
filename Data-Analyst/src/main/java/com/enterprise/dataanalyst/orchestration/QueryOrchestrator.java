package com.enterprise.dataanalyst.orchestration;

import com.enterprise.dataanalyst.execution.ResultFormatter;
import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.model.IntentType;
import com.enterprise.dataanalyst.model.QueryResponse;
import com.enterprise.dataanalyst.planning.ExecutionPlanner;
import com.enterprise.dataanalyst.semantic.SemanticRouter;
import com.enterprise.dataanalyst.service.DuckDBService;
import com.enterprise.dataanalyst.service.LlmClient;
import com.enterprise.dataanalyst.service.PromptService;
import com.enterprise.dataanalyst.state.AnalyticalContext;
import com.enterprise.dataanalyst.state.AnalyticalMemoryService;
import com.enterprise.dataanalyst.validation.SqlValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Query Orchestrator — the central nervous system of the new architecture.
 * Implements the deterministic workflow: Intent -> Context -> Grounding -> Planning -> Validation -> Execution.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryOrchestrator {

    private final SemanticRouter semanticRouter;
    private final DeterministicRouter deterministicRouter;
    private final SessionContextResolver sessionContextResolver;
    private final ExecutionPlanner executionPlanner;
    private final SqlValidator sqlValidator;
    private final AnalyticalMemoryService analyticalMemory;
    private final DuckDBService duckDBService;
    private final LlmClient llmClient;
    private final PromptService promptService;
    private final ResultFormatter resultFormatter;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Orchestrates a single user request through the analytical pipeline.
     */
    public Flux<String> orchestrate(String message, String conversationId, String activeDatasetId) {
        // Step 1: Deterministic Fast Path (Instant Router)
        Optional<IntentType> deterministicIntent = deterministicRouter.route(message);
        
        // Step 2: Context Resolution (Workspace Loading)
        SessionContextResolver.SessionWorkspace workspace = sessionContextResolver.resolve(conversationId, activeDatasetId);
        AnalyticalContext context = workspace.context();
        List<DatasetInfo> datasets = workspace.datasets();

        // Step 3: Semantic Intent Classification (Only if deterministic fails)
        IntentType intent = deterministicIntent.orElseGet(() -> semanticRouter.route(message));
        
        log.info("Orchestrating request. Intent: {}, Session: {}, Workspace Datasets: {}", 
                intent, conversationId, datasets.size());

        // Step 4: Workflow Routing
        return switch (intent) {
            case DATA_ANALYSIS, SEMANTIC_REASONING, VISUALIZATION_REQUEST -> 
                    handleDataAnalysisWorkflow(message, datasets, context);
            
            case DATA_DISCOVERY -> handleDataDiscovery(datasets);
            
            case SYSTEM_COMMAND -> handleSystemCommand(message, conversationId);
            
            case CHITCHAT, EXPLANATION_REQUEST, FOLLOW_UP_CONTEXTUAL -> 
                    handleGeneralConversation(message, intent);
            
            default -> handleGeneralConversation(message, IntentType.CHITCHAT);
        };
    }

    private Flux<String> handleDataAnalysisWorkflow(String message, List<DatasetInfo> datasets, AnalyticalContext context) {
        if (datasets == null || datasets.isEmpty()) {
            return Flux.just("I'd love to help with that analysis, but I don't have any active datasets in this session. Please upload a CSV or Excel file first.");
        }

        try {
            // Step 5 & 6: Grounding & Planning (Multi-dataset aware)
            String rawSql = executionPlanner.planSql(message, datasets, context);

            // Step 7: Safe SQL Validation
            // We validate against all possible tables in the workspace
            String sql = sqlValidator.validateAndFix(rawSql, String.valueOf(datasets.stream()
                    .flatMap(d -> d.getTables().stream())
                    .map(DatasetInfo.TableInfo::getTableName)
                    .collect(Collectors.toList())));
            
            log.info("Executing validated SQL: {}", sql);

            // Step 8: Execution
            long startTime = System.currentTimeMillis();
            List<Map<String, Object>> rawResults = duckDBService.query(sql);
            long executionTime = System.currentTimeMillis() - startTime;
            
            // Step 9: Context Update
            context.setLastSuccessfulSql(sql);
            analyticalMemory.updateContext(context);

            // Step 10: Structured Response Generation
            QueryResponse tableData = resultFormatter.format(rawResults, sql, executionTime);
            String tableJson = objectMapper.writeValueAsString(tableData);

            String resultsSubsetJson = objectMapper.writeValueAsString(
                    rawResults.size() > 50 ? rawResults.subList(0, 50) : rawResults);
            
            String interpretationPrompt = promptService.getInterpretationPrompt(message, sql, resultsSubsetJson, rawResults.size());

            Flux<String> interpretation = llmClient.streamChat(List.of(
                    Map.of("role", "system", "content", promptService.getSystemPrompt()),
                    Map.of("role", "user", "content", interpretationPrompt)
            ));

            return Flux.concat(
                    interpretation, 
                    Flux.just("\n\n<sql>" + sql + "</sql>"),
                    Flux.just("\n<table_data>" + tableJson + "</table_data>\n")
            );

        } catch (SecurityException se) {
            log.warn("SQL Validation Blocked: {}", se.getMessage());
            return Flux.just("⚠️ I generated a query that was flagged as unsafe and blocked it for your protection.\n\n**Reason:** " + se.getMessage());
        } catch (Exception e) {
            log.error("Orchestration pipeline failed", e);
            return Flux.just("❌ I encountered an error while analyzing your data: " + e.getMessage());
        }
    }

    private Flux<String> handleDataDiscovery(List<DatasetInfo> datasets) {
        if (datasets == null || datasets.isEmpty()) {
            return Flux.just("No datasets are currently active in this workspace. Upload a file to see its structure.");
        }

        // For discovery, we summarize all datasets
        StringBuilder discoverySummary = new StringBuilder();
        discoverySummary.append("### Workspace Overview\n");
        discoverySummary.append(String.format("You have %d file(s) available in this session.\n\n", datasets.size()));

        for (DatasetInfo ds : datasets) {
            discoverySummary.append(String.format("#### File: %s\n", ds.getName()));
            for (DatasetInfo.TableInfo table : ds.getTables()) {
                discoverySummary.append(String.format("- Table: `%s` (%d columns, %d rows)\n", 
                        table.getTableName(), table.getColumnCount(), table.getRowCount()));
            }
            discoverySummary.append("\n");
        }

        // Get LLM Overview for the whole workspace
        Flux<String> interpretation = llmClient.streamChat(List.of(
                Map.of("role", "system", "content", promptService.getSystemPrompt()),
                Map.of("role", "user", "content", "Analyze the workspace structure and suggest 3 high-level analytical questions:\n" + discoverySummary.toString())
        ));

        // Prepare Structured Schema Table for all tables
        try {
            List<String> header = List.of("Table", "Column Name", "Data Type");
            List<List<String>> rows = new ArrayList<>();
            for (DatasetInfo ds : datasets) {
                for (DatasetInfo.TableInfo table : ds.getTables()) {
                    for (DatasetInfo.ColumnInfo col : table.getColumns()) {
                        rows.add(List.of(table.getTableName(), col.getName(), col.getType()));
                    }
                }
            }

            QueryResponse schemaTable = QueryResponse.builder()
                    .columns(header)
                    .rows(rows)
                    .rowCount(rows.size())
                    .message("Workspace Schema Registry")
                    .build();

            String tableJson = objectMapper.writeValueAsString(schemaTable);
            Flux<String> structuredData = Flux.just("\n<table_data>" + tableJson + "</table_data>\n");

            return Flux.concat(Flux.just(discoverySummary.toString() + "\n"), interpretation, structuredData);

        } catch (Exception e) {
            log.error("Failed to generate structured schema table", e);
            return interpretation;
        }
    }

    private Flux<String> handleSystemCommand(String message, String conversationId) {
        String lower = message.toLowerCase();
        if (lower.contains("clear") || lower.contains("reset")) {
            analyticalMemory.clearContext(conversationId);
            return Flux.just("Session context cleared. I'm ready for a fresh analysis.");
        }
        return Flux.just("System command acknowledged.");
    }

    private Flux<String> handleGeneralConversation(String message, IntentType intent) {
        return llmClient.streamChat(List.of(
                Map.of("role", "system", "content", promptService.getChitchatSystemPrompt()),
                Map.of("role", "user", "content", message)
        ));
    }
}
