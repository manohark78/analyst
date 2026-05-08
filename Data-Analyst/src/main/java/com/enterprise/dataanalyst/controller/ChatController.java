package com.enterprise.dataanalyst.controller;

import com.enterprise.dataanalyst.service.ChatService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/chat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatService chatService;

    @PostMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamChat(@RequestBody Map<String, String> body) {
        String message = body.getOrDefault("message", "");
        String conversationId = body.getOrDefault("conversationId", null);
        String datasetId = body.getOrDefault("datasetId", null);

        log.info("Chat request: msg='{}', conv='{}', dataset='{}'", message, conversationId, datasetId);
        return chatService.processMessage(message, conversationId, datasetId);
    }
}
