package com.enterprise.dataanalyst.model;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
public class Conversation {
    private String id;
    private String title;
    private List<String> datasetIds;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private List<ChatMessage> messages;
}
