package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.ChatMessage;
import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.model.IntentType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Core AI Orchestrator — the brain of the application.
 * Routes requests through: Intent → Planning → Execution → Interpretation → Response
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {

    private final LlmClient llmClient;
    private final PromptService promptService;
    private final DuckDBService duckDBService;
    private final IntentClassifier intentClassifier;
    private final QuerySandbox querySandbox;
    private final ChatHistoryService historyService;
    private final FileIngestionService fileIngestionService;
    private final SemanticMapper semanticMapper;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Main entry point: processes a chat message and returns a streaming response.
     */
    public Flux<String> processMessage(String message, String conversationId, String activeDatasetId) {
        DatasetInfo dataset = activeDatasetId != null ? fileIngestionService.getDataset(activeDatasetId) : null;
        boolean hasDataset = dataset != null;
        boolean hasPrevContext = conversationId != null;

        // Step 1: Classify intent
        IntentType intent = intentClassifier.classify(message, hasDataset, hasPrevContext);
        log.info("Intent classified: {} for message: '{}'", intent, message);

        // Step 2: Save user message
        if (conversationId != null) {
            saveMessage(conversationId, "user", message, null, intent.name(), activeDatasetId);
        }

        // Step 3: Route to handler
        Flux<String> response = switch (intent) {
            case CHITCHAT -> handleChitchat(message);
            case DATA_ANALYSIS -> handleDataAnalysis(message, dataset, conversationId);
            case DATA_DISCOVERY -> handleDataDiscovery(dataset);
            case VISUALIZATION_REQUEST -> handleVisualization(message, dataset, conversationId);
            case FOLLOW_UP_CONTEXTUAL -> handleFollowUp(message, conversationId, dataset);
            case EXPLANATION_REQUEST -> handleExplanation(message, dataset, conversationId);
            case SEMANTIC_REASONING -> handleSemanticReasoning(message, dataset, conversationId);
            case SYSTEM_COMMAND -> handleSystemCommand(message);
        };

        // Step 4: Collect and save assistant response
        return collectAndSave(response, conversationId, intent.name(), activeDatasetId);
    }

    // === INTENT HANDLERS ===

    private Flux<String> handleChitchat(String message) {
        if (!llmClient.isModelLoaded()) {
            return getOfflineChitchatResponse(message);
        }
        return llmClient.streamChat(List.of(
                Map.of("role", "system", "content", promptService.getChitchatSystemPrompt()),
                Map.of("role", "user", "content", message)
        ));
    }

    private Flux<String> handleDataAnalysis(String message, DatasetInfo dataset, String conversationId) {
        if (dataset == null) {
            return Flux.just("Please upload a dataset first before asking data questions. I support **CSV** and **Excel** files.");
        }
        if (!llmClient.isModelLoaded()) {
            return Flux.just("The AI model is not loaded. Please ensure the model file is in place.");
        }

        try {
            // a. Generate SQL
            String sqlPrompt = promptService.getSqlGenerationPrompt(message, dataset);
            String rawSql = llmClient.complete(List.of(
                    Map.of("role", "system", "content", "You are a SQL generator. Return ONLY SQL, nothing else."),
                    Map.of("role", "user", "content", sqlPrompt)
            )).trim();

            String sql = querySandbox.sanitize(rawSql);
            log.info("Generated SQL: {}", sql);

            // b. Validate
            String validationError = querySandbox.validate(sql);
            if (validationError != null) {
                log.warn("SQL validation failed: {}", validationError);
                return Flux.just("⚠️ I generated an unsafe query and blocked it for your protection.\n\n**Reason:** " + validationError);
            }

            // c. Execute via DuckDB
            List<Map<String, Object>> results = duckDBService.query(sql);
            String resultsJson = objectMapper.writeValueAsString(
                    results.size() > 50 ? results.subList(0, 50) : results);

            // d. Stream interpretation with SQL preview
            Flux<String> sqlPreview = Flux.just("\n\n<sql>" + sql + "</sql>\n\n");

            Flux<String> interpretation = llmClient.streamChat(List.of(
                    Map.of("role", "system", "content", promptService.getSystemPrompt()),
                    Map.of("role", "user", "content",
                            promptService.getInterpretationPrompt(message, sql, resultsJson, results.size()))
            ));

            return Flux.concat(interpretation, sqlPreview);

        } catch (SQLException e) {
            log.error("DuckDB query failed", e);
            return Flux.just("❌ Query execution error: " + e.getMessage() + "\n\nI'll try a different approach next time.");
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed", e);
            return Flux.just("Error processing results.");
        }
    }

    private Flux<String> handleDataDiscovery(DatasetInfo dataset) {
        if (dataset == null) {
            // List all available datasets
            Collection<DatasetInfo> all = fileIngestionService.getAllDatasets();
            if (all.isEmpty()) {
                return Flux.just("No datasets uploaded yet. Please upload a **CSV** or **Excel** file to get started!");
            }
            StringBuilder sb = new StringBuilder("## Available Datasets\n\n");
            for (DatasetInfo d : all) {
                sb.append(String.format("- **%s** — %d rows, %d columns (table: `%s`)\n",
                        d.getName(), d.getRowCount(), d.getColumnCount(), d.getTableName()));
            }
            return Flux.just(sb.toString());
        }

        if (!llmClient.isModelLoaded()) {
            // Provide schema without LLM
            StringBuilder sb = new StringBuilder("## Dataset: " + dataset.getName() + "\n\n");
            sb.append(String.format("**Rows:** %,d | **Columns:** %d\n\n", dataset.getRowCount(), dataset.getColumnCount()));
            sb.append("| Column | Type |\n|--------|------|\n");
            for (DatasetInfo.ColumnInfo col : dataset.getColumns()) {
                sb.append(String.format("| %s | %s |\n", col.getName(), col.getType()));
            }
            return Flux.just(sb.toString());
        }

        return llmClient.streamChat(List.of(
                Map.of("role", "system", "content", promptService.getSystemPrompt()),
                Map.of("role", "user", "content", promptService.getDataDiscoveryPrompt(dataset))
        ));
    }

    private Flux<String> handleVisualization(String message, DatasetInfo dataset, String conversationId) {
        if (dataset == null) {
            return Flux.just("Please upload and select a dataset before requesting visualizations.");
        }
        if (!llmClient.isModelLoaded()) {
            return Flux.just("The AI model is required for chart generation. Please load the model file.");
        }

        try {
            String sqlPrompt = promptService.getVisualizationPrompt(message, dataset);
            String rawSql = llmClient.complete(List.of(
                    Map.of("role", "system", "content", "You are a SQL generator for chart data. Return ONLY SQL."),
                    Map.of("role", "user", "content", sqlPrompt)
            )).trim();

            String sql = querySandbox.sanitize(rawSql);
            String validationError = querySandbox.validate(sql);
            if (validationError != null) {
                return Flux.just("⚠️ Chart query blocked: " + validationError);
            }

            List<Map<String, Object>> results = duckDBService.query(sql);
            String chartJson = objectMapper.writeValueAsString(results);

            // Send chart data as a special tagged block
            return Flux.just("\n<chart>" + chartJson + "</chart>\n\n" +
                    "📊 Here's the visualization based on your request.");

        } catch (Exception e) {
            log.error("Visualization failed", e);
            return Flux.just("❌ Failed to generate chart: " + e.getMessage());
        }
    }

    private Flux<String> handleFollowUp(String message, String conversationId, DatasetInfo dataset) {
        if (conversationId == null) {
            return handleChitchat(message);
        }

        try {
            List<ChatMessage> recent = historyService.getRecentMessages(conversationId, 6);
            List<Map<String, String>> context = recent.stream()
                    .map(m -> Map.of("role", m.getRole(), "content", m.getContent()))
                    .collect(Collectors.toList());

            if (dataset != null) {
                // Add schema context
                context.add(0, Map.of("role", "system", "content",
                        promptService.getSystemPrompt() + "\nActive table: " + dataset.getTableName() +
                        "\nColumns: " + dataset.getColumns().stream()
                                .map(c -> c.getName() + "(" + c.getType() + ")")
                                .collect(Collectors.joining(", "))));
            }

            context.add(Map.of("role", "user", "content", message));
            return llmClient.streamChat(context);
        } catch (SQLException e) {
            log.error("Failed to load conversation history", e);
            return handleChitchat(message);
        }
    }

    private Flux<String> handleExplanation(String message, DatasetInfo dataset, String conversationId) {
        // For explanations, combine context + the question
        if (dataset != null && llmClient.isModelLoaded()) {
            return handleDataAnalysis(message, dataset, conversationId);
        }
        return handleChitchat(message);
    }

    private Flux<String> handleSemanticReasoning(String message, DatasetInfo dataset, String conversationId) {
        if (dataset == null) {
            return Flux.just("Please select a dataset for semantic analysis.");
        }
        // Semantic reasoning uses the same SQL pipeline but with enriched prompts
        return handleDataAnalysis(message, dataset, conversationId);
    }

    private Flux<String> handleSystemCommand(String message) {
        String lower = message.toLowerCase();
        if (lower.contains("clear") || lower.contains("reset")) {
            return Flux.just("Chat cleared. Start a new conversation from the sidebar.");
        }
        return Flux.just("System command noted. Use the sidebar controls for chat management.");
    }

    // === HELPERS ===

    private Flux<String> getOfflineChitchatResponse(String message) {
        String lower = message.toLowerCase().trim();
        if (lower.matches("(hi|hello|hey|good morning|good evening|good afternoon).*")) {
            return Flux.just("Hello! 👋 I'm your offline AI Data Analyst. Upload a CSV or Excel file and I'll help you analyze it.\n\n" +
                    "**Note:** The LLM model is not loaded yet. Please ensure the `.gguf` file is in place for full AI capabilities.");
        }
        if (lower.contains("who are you") || lower.contains("what can you do")) {
            return Flux.just("I'm an **Offline AI Data Analyst**. I can:\n\n" +
                    "📊 Analyze your CSV and Excel datasets\n" +
                    "🔍 Answer questions about your data\n" +
                    "📈 Generate charts and visualizations\n" +
                    "🧠 Perform semantic reasoning\n" +
                    "🔒 Everything runs locally — your data never leaves your machine.\n\n" +
                    "*Upload a file to get started!*");
        }
        return Flux.just("I'm here to help with your data! The AI model isn't loaded yet, but you can still upload files and explore your data.");
    }

    private Flux<String> collectAndSave(Flux<String> source, String conversationId, String intentType, String datasetId) {
        if (conversationId == null) return source;

        StringBuilder collected = new StringBuilder();
        return source.doOnNext(collected::append)
                .doOnComplete(() -> {
                    if (!collected.isEmpty()) {
                        saveMessage(conversationId, "assistant", collected.toString(), null, intentType, datasetId);
                    }
                });
    }

    private void saveMessage(String conversationId, String role, String content, String sql, String intent, String datasetId) {
        try {
            historyService.addMessage(ChatMessage.builder()
                    .id(UUID.randomUUID().toString().substring(0, 8))
                    .conversationId(conversationId)
                    .role(role)
                    .content(content)
                    .sqlUsed(sql)
                    .intentType(intent)
                    .datasetId(datasetId)
                    .createdAt(LocalDateTime.now())
                    .build());
        } catch (SQLException e) {
            log.error("Failed to save message", e);
        }
    }
}
