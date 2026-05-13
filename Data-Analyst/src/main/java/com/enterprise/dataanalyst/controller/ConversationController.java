package com.enterprise.dataanalyst.controller;

import com.enterprise.dataanalyst.model.Conversation;
import com.enterprise.dataanalyst.service.ChatHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/conversations")
@RequiredArgsConstructor
public class ConversationController {

    private final ChatHistoryService historyService;

    @PostMapping
    public ResponseEntity<Conversation> create(@RequestBody Map<String, String> body) {
        try {
            String title = body.getOrDefault("title", "New Chat");
            String datasetId = body.getOrDefault("datasetId", null);
            List<String> datasetIds = datasetId != null ? List.of(datasetId) : null;
            Conversation conv = historyService.createConversation(title, datasetIds);
            return ResponseEntity.ok(conv);
        } catch (Exception e) {
            log.error("Failed to create conversation", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping
    public ResponseEntity<List<Conversation>> list() {
        try {
            return ResponseEntity.ok(historyService.listConversations());
        } catch (Exception e) {
            log.error("Failed to list conversations", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<Conversation> get(@PathVariable String id) {
        try {
            Conversation conv = historyService.getConversation(id);
            if (conv == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(conv);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> rename(@PathVariable String id, @RequestBody Map<String, String> body) {
        try {
            historyService.renameConversation(id, body.get("title"));
            return ResponseEntity.ok(Map.of("status", "renamed"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable String id) {
        try {
            historyService.deleteConversation(id);
            return ResponseEntity.ok(Map.of("status", "deleted"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
