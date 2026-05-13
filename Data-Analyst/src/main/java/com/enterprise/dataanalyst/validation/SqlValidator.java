package com.enterprise.dataanalyst.validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * SQL Validator — ensures generated SQL is safe, stays within session boundaries,
 * and doesn't perform destructive or expensive operations.
 */
@Slf4j
@Service
public class SqlValidator {

    private static final Set<String> BLOCKED_KEYWORDS = Set.of(
            "DROP", "ALTER", "DELETE", "INSERT", "UPDATE", "TRUNCATE",
            "CREATE", "GRANT", "REVOKE", "ATTACH", "DETACH", "COPY",
            "EXPORT", "IMPORT", "PRAGMA", "CALL", "EXEC", "EXECUTE"
    );

    private static final Pattern DANGEROUS_PATTERN = Pattern.compile(
            "(?i)(read_csv|read_parquet|read_json|httpfs|mysql_scan|postgres_scan|sqlite_scan|install|load)"
    );
    public String validateAndFix(String sql, String allowedTableName) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("SQL query is empty");
        }

        String sanitizedSql = sanitize(sql);
        String upper = sanitizedSql.toUpperCase();

        // 1. Must be a read-only query
        if (!upper.contains("SELECT") && !upper.contains("WITH")) {
            throw new SecurityException("Invalid SQL: Only SELECT or WITH queries are permitted.");
        }

        // 2. Block destructive operations and extensions
        for (String blocked : BLOCKED_KEYWORDS) {
            if (upper.contains(blocked)) {
                throw new SecurityException("Security Violation: Blocked keyword detected: " + blocked);
            }
        }

        // 3. Block filesystem/network access patterns
        if (DANGEROUS_PATTERN.matcher(sanitizedSql).find()) {
            throw new SecurityException("Security Violation: External data access or extension loading is blocked.");
        }

        // 4. Boundary enforcement: Check if query references allowed table or context CTE
        if (allowedTableName != null) {
            String table = allowedTableName.toUpperCase();
            if (!upper.contains(table) && !upper.contains("BASE_CONTEXT") && !upper.contains("PREVIOUS_RESULTS")) {
                log.warn("SQL Grounding Warning: Query does not reference the active table '{}'", allowedTableName);
            }
        }

        // 5. Performance Safety: Enforce LIMIT if missing
        if (!upper.contains("LIMIT")) {
            sanitizedSql = sanitizedSql.trim();
            if (sanitizedSql.endsWith(";")) {
                sanitizedSql = sanitizedSql.substring(0, sanitizedSql.length() - 1).trim();
            }
            sanitizedSql += " LIMIT 100";
            log.info("Safe Execution: Automatically appended LIMIT 100 to query.");
        }

        return sanitizedSql;
    }

    private String sanitize(String sql) {
        // Extract from markdown if LLM wrapped it
        java.util.regex.Matcher m = Pattern.compile("(?s)```(sql)?(.*?)```").matcher(sql);
        if (m.find()) {
            sql = m.group(2);
        }
        
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
