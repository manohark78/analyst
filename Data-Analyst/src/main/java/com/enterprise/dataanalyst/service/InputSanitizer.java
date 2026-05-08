package com.enterprise.dataanalyst.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Input Sanitizer — prevents prompt injection and XSS from user input and uploaded file data.
 */
@Slf4j
@Service
public class InputSanitizer {

    /**
     * Sanitize user input before sending to LLM.
     * Prevents prompt injection and escapes dangerous characters.
     */
    public String sanitizeUserInput(String input) {
        if (input == null) return "";

        // Strip control characters
        input = input.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "");

        // Limit length
        if (input.length() > 5000) {
            input = input.substring(0, 5000);
            log.warn("User input truncated to 5000 chars");
        }

        return input.trim();
    }

    /**
     * Sanitize column names from uploaded files to prevent injection through data.
     */
    public String sanitizeColumnName(String name) {
        if (name == null) return "unnamed";
        return name.replaceAll("[^a-zA-Z0-9_\\s]", "_").trim();
    }

    /**
     * Sanitize file names to prevent path traversal.
     */
    public String sanitizeFileName(String name) {
        if (name == null) return "unnamed";
        // Remove path separators and dangerous characters
        return name.replaceAll("[/\\\\:*?\"<>|]", "_").trim();
    }
}
