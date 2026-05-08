package com.enterprise.dataanalyst.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * SQL Security Sandbox — validates all generated SQL before execution.
 * Prevents destructive operations, injection, and filesystem access.
 */
@Slf4j
@Service
public class QuerySandbox {

    private static final Set<String> BLOCKED_KEYWORDS = Set.of(
            "DROP", "ALTER", "DELETE", "INSERT", "UPDATE", "TRUNCATE",
            "CREATE INDEX", "GRANT", "REVOKE", "EXEC", "EXECUTE",
            "ATTACH", "DETACH", "COPY", "EXPORT", "IMPORT",
            "CALL", "SET", "PRAGMA"
    );

    private static final Pattern DANGEROUS_PATTERN = Pattern.compile(
            "(?i)(;\\s*(DROP|ALTER|DELETE|INSERT|UPDATE|CREATE))|" +
            "(--)|" +                       // SQL comments (possible injection)
            "(/\\*)|" +                     // Block comments
            "(\\bSLEEP\\b)|" +             // Time-based attacks
            "(\\bBENCHMARK\\b)|" +
            "(\\bLOAD\\b)|" +
            "(\\bINTO\\s+OUTFILE\\b)|" +
            "(\\bFROM\\s+read_csv\\b)|" +  // Prevent reading arbitrary files
            "(\\bFROM\\s+read_parquet\\b)"
    );

    /**
     * Validates SQL query for safety. Returns null if safe, error message if not.
     */
    public String validate(String sql) {
        if (sql == null || sql.isBlank()) {
            return "Empty SQL query";
        }

        String upperSql = sql.toUpperCase().trim();

        // Must start with SELECT or WITH (CTE)
        if (!upperSql.startsWith("SELECT") && !upperSql.startsWith("WITH")) {
            return "Only SELECT queries are allowed. Received: " + upperSql.substring(0, Math.min(20, upperSql.length()));
        }

        // Check for blocked keywords
        for (String blocked : BLOCKED_KEYWORDS) {
            if (upperSql.contains(blocked)) {
                return "Blocked operation detected: " + blocked;
            }
        }

        // Check for dangerous patterns
        if (DANGEROUS_PATTERN.matcher(sql).find()) {
            return "Potentially dangerous SQL pattern detected";
        }

        // Check for multiple statements (semicolon-separated)
        long semicolonCount = sql.chars().filter(c -> c == ';').count();
        if (semicolonCount > 1) {
            return "Multiple SQL statements not allowed";
        }

        log.debug("SQL validation passed: {}", sql);
        return null; // Safe
    }

    /**
     * Sanitize and clean SQL before execution.
     */
    public String sanitize(String sql) {
        if (sql == null) return null;
        
        // Extract from markdown block if present
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?s)```sql(.*?)```").matcher(sql);
        if (m.find()) {
            sql = m.group(1);
        } else {
            m = java.util.regex.Pattern.compile("(?s)```(.*?)```").matcher(sql);
            if (m.find()) {
                sql = m.group(1);
            }
        }
        
        // Remove trailing semicolons
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
