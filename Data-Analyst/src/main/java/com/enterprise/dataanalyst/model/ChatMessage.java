package com.enterprise.dataanalyst.model;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class ChatMessage {
    private String id;
    private String conversationId;
    private String role;       // "user", "assistant", "system"
    private String content;
    private String sqlUsed;    // SQL that was executed (if any)
    private String intentType; // Intent classification result
    private String datasetId;  // Which dataset was used
    private LocalDateTime createdAt;
}
