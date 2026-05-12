package com.enterprise.dataanalyst.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Maps user terminology to actual dataset column names using
 * fuzzy matching, synonym dictionaries, and substring matching.
 * No LLM call needed — pure algorithmic approach.
 */
@Slf4j
@Service
public class SemanticMapper {

    /**
     * Given a user term, find the best matching column from the actual columns list.
     * Returns the actual column name or null if no match found.
     */
    public String findBestColumnMatch(String userTerm, List<String> actualColumns) {
        String normalized = userTerm.toLowerCase().trim().replaceAll("[\\s_-]+", "_");

        // 1. Exact match
        for (String col : actualColumns) {
            if (col.equalsIgnoreCase(normalized)) return col;
        }

        // 2. Substring match (user says "revenue", column is "total_revenue")
        for (String col : actualColumns) {
            String colLower = col.toLowerCase();
            if (colLower.contains(normalized) || normalized.contains(colLower)) {
                return col;
            }
        }

        // 3. Levenshtein distance (fuzzy match) — accept if distance <= 2
        String bestMatch = null;
        int bestDistance = Integer.MAX_VALUE;
        for (String col : actualColumns) {
            int dist = levenshtein(normalized, col.toLowerCase());
            if (dist < bestDistance && dist <= 2) {
                bestDistance = dist;
                bestMatch = col;
            }
        }

        return bestMatch;
    }

    /**
     * Map all user terms in a message to actual column names.
     * Returns a map of userTerm → actualColumnName.
     */
    public Map<String, String> mapTermsToColumns(String message, List<String> actualColumns) {
        Map<String, String> mappings = new LinkedHashMap<>();
        String[] words = message.toLowerCase().split("\\s+");

        for (String word : words) {
            String cleaned = word.replaceAll("[^a-zA-Z0-9_]", "");
            if (cleaned.length() < 2) continue;

            String match = findBestColumnMatch(cleaned, actualColumns);
            if (match != null) {
                mappings.put(cleaned, match);
            }
        }

        return mappings;
    }

    private int levenshtein(String a, String b) {
        int[][] dp = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= b.length(); j++) dp[0][j] = j;
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
            }
        }
        return dp[a.length()][b.length()];
    }
}
