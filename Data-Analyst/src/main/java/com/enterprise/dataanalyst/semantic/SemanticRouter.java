package com.enterprise.dataanalyst.semantic;

import com.enterprise.dataanalyst.model.IntentType;
import com.enterprise.dataanalyst.service.LlmClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Semantic Router — uses LLM to classify user intent beyond simple keyword matching.
 * This is the gateway to the deterministic orchestration flow.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticRouter {

    private final LlmClient llmClient;

    /**
     * Classifies the user message into a specific IntentType using the LLM.
     * Uses a strict categorical prompt to ensure deterministic outputs.
     */
    public IntentType route(String message) {
        if (!llmClient.isModelLoaded()) {
            return IntentType.CHITCHAT;
        }

        String prompt = String.format("""
                Categorize the following analytical request into exactly ONE intent.
                
                INTENTS:
                - CHITCHAT: Greetings, help requests, or general conversation.
                - DATA_ANALYSIS: Quantitative questions, filtering, sorting, or calculations.
                - DATA_DISCOVERY: Questions about available data, table structure, or schemas.
                - VISUALIZATION_REQUEST: Requests for charts, plots, or visual data representation.
                - SEMANTIC_REASONING: Requests for qualitative analysis, sentiment, or patterns.
                - SYSTEM_COMMAND: Requests to clear history, reset session, or configuration.
                - FOLLOW_UP_CONTEXTUAL: Ambiguous follow-ups that require previous context.

                USER MESSAGE: "%s"
                
                Return ONLY the intent name in ALL_CAPS.
                INTENT:""", message);

        try {
            String classification = llmClient.complete(List.of(
                    Map.of("role", "system", "content", "You are an analytical intent classifier. Return ONLY the intent name."),
                    Map.of("role", "user", "content", prompt)
            )).trim().toUpperCase();

            for (IntentType type : IntentType.values()) {
                if (classification.contains(type.name())) {
                    log.info("Semantic routing match: {}", type);
                    return type;
                }
            }
        } catch (Exception e) {
            log.error("Semantic routing failed", e);
        }

        return IntentType.CHITCHAT;
    }
}
