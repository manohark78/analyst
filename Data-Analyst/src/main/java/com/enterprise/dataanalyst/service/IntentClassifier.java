package com.enterprise.dataanalyst.service;

import com.enterprise.dataanalyst.model.IntentType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Rule-based intent classifier. Fast, no LLM call needed for obvious cases.
 * Falls back to LLM for ambiguous inputs.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IntentClassifier {

    private static final Set<String> CHITCHAT_PATTERNS = Set.of(
            "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
            "how are you", "what's up", "thanks", "thank you", "bye", "goodbye",
            "ok", "okay", "sure", "yes", "no", "great", "cool", "nice",
            "who are you", "what can you do", "help"
    );

    private static final Set<String> DISCOVERY_KEYWORDS = Set.of(
            "columns", "schema", "structure", "what data", "what fields",
            "describe", "show table", "list tables", "what datasets",
            "how many columns", "data types", "what is available"
    );

    private static final Set<String> VIZ_KEYWORDS = Set.of(
            "chart", "graph", "plot", "visualize", "visualization",
            "bar chart", "pie chart", "line chart", "histogram", "scatter"
    );

    private static final Set<String> SYSTEM_KEYWORDS = Set.of(
            "clear chat", "delete", "reset", "new chat", "settings",
            "change theme", "switch model"
    );

    private static final Pattern FOLLOW_UP_PATTERN = Pattern.compile(
            "(?i)(also|and also|what about|how about|same for|compare with|" +
            "previous|last query|the same|those|these|it|them|that)"
    );

    private static final Set<String> EXPLANATION_KEYWORDS = Set.of(
            "why", "how come", "explain", "what does", "what is the reason",
            "can you explain", "tell me why", "elaborate"
    );

    private static final Set<String> SEMANTIC_KEYWORDS = Set.of(
            "sentiment", "unhappy", "happy", "positive", "negative",
            "risk", "anomaly", "outlier", "unusual", "suspicious",
            "segment", "classify", "categorize", "cluster"
    );

    public IntentType classify(String message, boolean hasActiveDataset, boolean hasPreviousContext) {
        String lower = message.toLowerCase().trim();

        // 1. System commands first
        for (String kw : SYSTEM_KEYWORDS) {
            if (lower.contains(kw)) return IntentType.SYSTEM_COMMAND;
        }

        // 2. Pure chitchat (exact or near-exact match)
        for (String pattern : CHITCHAT_PATTERNS) {
            if (lower.equals(pattern) || lower.startsWith(pattern + " ") || lower.endsWith(" " + pattern)) {
                return IntentType.CHITCHAT;
            }
        }

        // 3. Data discovery
        for (String kw : DISCOVERY_KEYWORDS) {
            if (lower.contains(kw)) return IntentType.DATA_DISCOVERY;
        }

        // 4. Visualization
        for (String kw : VIZ_KEYWORDS) {
            if (lower.contains(kw)) return IntentType.VISUALIZATION_REQUEST;
        }

        // 5. Semantic reasoning (sentiment, risk, anomaly)
        for (String kw : SEMANTIC_KEYWORDS) {
            if (lower.contains(kw)) return IntentType.SEMANTIC_REASONING;
        }

        // 6. Explanation
        for (String kw : EXPLANATION_KEYWORDS) {
            if (lower.startsWith(kw) || lower.contains(kw)) {
                return hasActiveDataset ? IntentType.EXPLANATION_REQUEST : IntentType.CHITCHAT;
            }
        }

        // 7. Follow-up detection
        if (hasPreviousContext && FOLLOW_UP_PATTERN.matcher(lower).find()) {
            return IntentType.FOLLOW_UP_CONTEXTUAL;
        }

        // 8. If there's an active dataset and it's a question, likely data analysis
        if (hasActiveDataset) {
            return IntentType.DATA_ANALYSIS;
        }

        // 9. Default fallback
        return IntentType.CHITCHAT;
    }
}
