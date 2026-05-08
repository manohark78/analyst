package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.ChatMessage;
import com.enterprise.dataanalyst.model.Conversation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages persistent chat history using DuckDB metadata tables.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChatHistoryService {

    private final DuckDBService duckDBService;

    @PostConstruct
    public void initSchema() throws SQLException {
        duckDBService.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                active_dataset_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        duckDBService.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id VARCHAR PRIMARY KEY,
                conversation_id VARCHAR NOT NULL,
                role VARCHAR NOT NULL,
                content TEXT NOT NULL,
                sql_used VARCHAR,
                intent_type VARCHAR,
                dataset_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);
        log.info("Chat history schema initialized.");
    }

    public Conversation createConversation(String title, String datasetId) throws SQLException {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String safeTitle = title.replace("'", "''");
        duckDBService.execute(String.format(
                "INSERT INTO conversations (id, title, active_dataset_id) VALUES ('%s', '%s', '%s')",
                id, safeTitle, datasetId != null ? datasetId : ""));
        return Conversation.builder()
                .id(id)
                .title(title)
                .activeDatasetId(datasetId)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .messages(new ArrayList<>())
                .build();
    }

    public void addMessage(ChatMessage msg) throws SQLException {
        String safeContent = msg.getContent().replace("'", "''");
        String safeSql = msg.getSqlUsed() != null ? msg.getSqlUsed().replace("'", "''") : "";
        duckDBService.execute(String.format(
                "INSERT INTO messages (id, conversation_id, role, content, sql_used, intent_type, dataset_id) " +
                "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                msg.getId(), msg.getConversationId(), msg.getRole(), safeContent,
                safeSql, msg.getIntentType() != null ? msg.getIntentType() : "",
                msg.getDatasetId() != null ? msg.getDatasetId() : ""));
    }

    public List<Conversation> listConversations() throws SQLException {
        List<Map<String, Object>> rows = duckDBService.query(
                "SELECT * FROM conversations ORDER BY updated_at DESC");
        return rows.stream().map(row -> Conversation.builder()
                .id(str(row, "id"))
                .title(str(row, "title"))
                .activeDatasetId(str(row, "active_dataset_id"))
                .createdAt(LocalDateTime.now())
                .build()
        ).collect(Collectors.toList());
    }

    public Conversation getConversation(String conversationId) throws SQLException {
        List<Map<String, Object>> convRows = duckDBService.query(
                "SELECT * FROM conversations WHERE id = '" + conversationId + "'");
        if (convRows.isEmpty()) return null;

        Map<String, Object> conv = convRows.get(0);
        List<Map<String, Object>> msgRows = duckDBService.query(
                "SELECT * FROM messages WHERE conversation_id = '" + conversationId + "' ORDER BY created_at ASC");

        List<ChatMessage> messages = msgRows.stream().map(row -> ChatMessage.builder()
                .id(str(row, "id"))
                .conversationId(str(row, "conversation_id"))
                .role(str(row, "role"))
                .content(str(row, "content"))
                .sqlUsed(str(row, "sql_used"))
                .intentType(str(row, "intent_type"))
                .datasetId(str(row, "dataset_id"))
                .build()
        ).collect(Collectors.toList());

        return Conversation.builder()
                .id(str(conv, "id"))
                .title(str(conv, "title"))
                .activeDatasetId(str(conv, "active_dataset_id"))
                .messages(messages)
                .build();
    }

    public List<ChatMessage> getRecentMessages(String conversationId, int limit) throws SQLException {
        List<Map<String, Object>> rows = duckDBService.query(String.format(
                "SELECT * FROM messages WHERE conversation_id = '%s' ORDER BY created_at DESC LIMIT %d",
                conversationId, limit));
        Collections.reverse(rows);
        return rows.stream().map(row -> ChatMessage.builder()
                .id(str(row, "id"))
                .conversationId(str(row, "conversation_id"))
                .role(str(row, "role"))
                .content(str(row, "content"))
                .sqlUsed(str(row, "sql_used"))
                .build()
        ).collect(Collectors.toList());
    }

    public void renameConversation(String id, String newTitle) throws SQLException {
        String safeTitle = newTitle.replace("'", "''");
        duckDBService.execute(String.format(
                "UPDATE conversations SET title = '%s', updated_at = CURRENT_TIMESTAMP WHERE id = '%s'",
                safeTitle, id));
    }

    public void deleteConversation(String id) throws SQLException {
        duckDBService.execute("DELETE FROM messages WHERE conversation_id = '" + id + "'");
        duckDBService.execute("DELETE FROM conversations WHERE id = '" + id + "'");
    }

    private String str(Map<String, Object> row, String key) {
        Object val = row.get(key);
        return val != null ? val.toString() : "";
    }
}
