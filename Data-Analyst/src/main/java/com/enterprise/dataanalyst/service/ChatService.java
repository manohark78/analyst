package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.ChatMessage;
import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.orchestration.QueryOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Core Chat Service — entry point for the frontend.
 * Now delegated to QueryOrchestrator for complex analytical workflows.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {

    private final QueryOrchestrator queryOrchestrator;
    private final ChatHistoryService historyService;
    private final FileIngestionService fileIngestionService;

    /**
     * Main entry point: processes a chat message and returns a streaming response.
     */
    public Flux<String> processMessage(String message, String conversationId, String activeDatasetId) {
        DatasetInfo dataset = activeDatasetId != null ? fileIngestionService.getDataset(activeDatasetId) : null;
        
        // Save user message first
        if (conversationId != null) {
            saveMessage(conversationId, "user", message, null, "UNKNOWN", activeDatasetId);
        }

        // Delegate to the Orchestrator for deterministic analytical flow
        Flux<String> response = queryOrchestrator.orchestrate(message, conversationId, dataset);

        // Collect and save assistant response
        return collectAndSave(response, conversationId, "ANALYSIS", activeDatasetId);
    }

    // === HELPERS ===

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
