package com.enterprise.dataanalyst.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DuckDBService {

    @Value("${app.storage.db-path}")
    private String dbPath;

    private Connection connection;

    @PostConstruct
    public void init() throws SQLException {
        try {
            java.io.File dbFile = new java.io.File(dbPath);
            if (dbFile.getParentFile() != null && !dbFile.getParentFile().exists()) {
                dbFile.getParentFile().mkdirs();
            }
        } catch (Exception e) {
            log.warn("Could not create directories for DB path: {}", e.getMessage());
        }
        log.info("Initializing DuckDB connection at: {}", dbPath);
        connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
    }

    @PreDestroy
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            log.info("DuckDB connection closed.");
        }
    }

    public List<Map<String, Object>> query(String sql) throws SQLException {
        log.debug("Executing query: {}", sql);
        List<Map<String, Object>> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        }
        return results;
    }

    public void execute(String sql) throws SQLException {
        log.debug("Executing statement: {}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
