package com.enterprise.dataanalyst.state;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages analytical state (SQL history, filters, active tables) per conversation.
 * Separate from ChatHistoryService which handles text messages.
 */
@Service
public class AnalyticalMemoryService {

    private final Map<String, AnalyticalContext> contexts = new ConcurrentHashMap<>();

    public AnalyticalContext getContext(String conversationId) {
        return contexts.computeIfAbsent(conversationId, id -> AnalyticalContext.builder()
                .conversationId(id)
                .build());
    }

    public void updateContext(AnalyticalContext context) {
        if (context.getConversationId() != null) {
            contexts.put(context.getConversationId(), context);
        }
    }

    public void clearContext(String conversationId) {
        contexts.remove(conversationId);
    }
}
