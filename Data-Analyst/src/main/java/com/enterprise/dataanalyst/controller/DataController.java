package com.enterprise.dataanalyst.controller;

import com.enterprise.dataanalyst.service.DuckDBService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/data")
@RequiredArgsConstructor
public class DataController {

    private final DuckDBService duckDBService;

    @GetMapping("/preview")
    public List<Map<String, Object>> getPreview(@RequestParam String tableName, @RequestParam(defaultValue = "50") int limit) throws SQLException {
        // Basic security: only allow data_ prefixed tables
        if (!tableName.startsWith("data_")) {
            throw new IllegalArgumentException("Invalid table access");
        }
        String sql = String.format("SELECT * FROM %s LIMIT %d", tableName, limit);
        return duckDBService.query(sql);
    }
}
