package com.enterprise.dataanalyst.model;

/**
 * All supported user intent types.
 * The AI Orchestrator uses this to route requests to the correct handler.
 */
public enum IntentType {
    CHITCHAT,               // Greetings, general conversation
    DATA_ANALYSIS,          // Queries, aggregations, filters
    SEMANTIC_REASONING,     // Sentiment, risk, derived insights
    DATA_DISCOVERY,         // Schema, column, dataset questions
    VISUALIZATION_REQUEST,  // Chart / graph requests
    FOLLOW_UP_CONTEXTUAL,   // Follows from previous message
    EXPLANATION_REQUEST,    // "Why", "How", "Explain" questions
    SYSTEM_COMMAND          // Clear, reset, settings
}
