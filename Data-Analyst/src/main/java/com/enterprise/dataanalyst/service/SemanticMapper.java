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

    // Business term → possible column name variants
    private static final Map<String, List<String>> SYNONYM_MAP = new LinkedHashMap<>();

    static {
        SYNONYM_MAP.put("salary", List.of("salary", "ctc", "compensation", "annual_income", "pay", "wage", "earnings", "income"));
        SYNONYM_MAP.put("name", List.of("name", "full_name", "first_name", "last_name", "customer_name", "employee_name", "emp_name"));
        SYNONYM_MAP.put("city", List.of("city", "location", "town", "place", "address_city"));
        SYNONYM_MAP.put("country", List.of("country", "nation", "region", "country_name"));
        SYNONYM_MAP.put("age", List.of("age", "years_old", "dob", "date_of_birth", "birth_date"));
        SYNONYM_MAP.put("email", List.of("email", "email_address", "mail", "e_mail", "email_id"));
        SYNONYM_MAP.put("phone", List.of("phone", "mobile", "telephone", "contact", "phone_number", "cell"));
        SYNONYM_MAP.put("revenue", List.of("revenue", "sales", "total_sales", "income", "turnover", "amount"));
        SYNONYM_MAP.put("profit", List.of("profit", "net_profit", "gross_profit", "margin", "earnings"));
        SYNONYM_MAP.put("date", List.of("date", "created_at", "updated_at", "order_date", "purchase_date", "transaction_date", "timestamp"));
        SYNONYM_MAP.put("status", List.of("status", "state", "order_status", "active", "is_active"));
        SYNONYM_MAP.put("category", List.of("category", "type", "group", "class", "segment", "department"));
        SYNONYM_MAP.put("quantity", List.of("quantity", "qty", "count", "units", "amount", "num"));
        SYNONYM_MAP.put("price", List.of("price", "cost", "unit_price", "rate", "amount", "mrp"));
        SYNONYM_MAP.put("rating", List.of("rating", "score", "review_score", "stars", "feedback_score"));
        SYNONYM_MAP.put("gender", List.of("gender", "sex", "male_female"));
        SYNONYM_MAP.put("product", List.of("product", "item", "product_name", "item_name", "sku"));
        SYNONYM_MAP.put("customer", List.of("customer", "client", "buyer", "user", "customer_name", "customer_id"));
        SYNONYM_MAP.put("order", List.of("order", "order_id", "order_number", "purchase_id", "transaction_id"));
    }

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

        // 3. Synonym lookup
        for (Map.Entry<String, List<String>> entry : SYNONYM_MAP.entrySet()) {
            if (entry.getValue().contains(normalized) || entry.getKey().equals(normalized)) {
                // Found a synonym group — now check if any actual column matches
                for (String synonym : entry.getValue()) {
                    for (String col : actualColumns) {
                        if (col.equalsIgnoreCase(synonym) || col.toLowerCase().contains(synonym)) {
                            return col;
                        }
                    }
                }
            }
        }

        // 4. Levenshtein distance (fuzzy match) — accept if distance <= 2
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
