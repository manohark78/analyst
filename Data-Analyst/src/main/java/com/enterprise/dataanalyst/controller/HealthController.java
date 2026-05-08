package com.enterprise.dataanalyst.controller;

import com.enterprise.dataanalyst.service.LlmClient;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class HealthController {

    private final LlmClient llmClient;

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "llmRunning", llmClient.isModelLoaded(),
                "version", "1.0.0"
        );
    }
}
