package com.enterprise.dataanalyst.controller;

import com.enterprise.dataanalyst.model.DatasetInfo;
import com.enterprise.dataanalyst.service.FileIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/datasets")
@RequiredArgsConstructor
public class DatasetController {

    private final FileIngestionService fileIngestionService;

    @PostMapping("/upload")
    public ResponseEntity<?> uploadDataset(@RequestParam("file") MultipartFile file) {
        try {
            List<DatasetInfo> datasets = fileIngestionService.ingestFile(file);
            return ResponseEntity.ok(datasets);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Upload failed", e);
            return ResponseEntity.internalServerError().body(Map.of("error", "Upload failed: " + e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<Collection<DatasetInfo>> listDatasets() {
        return ResponseEntity.ok(fileIngestionService.getAllDatasets());
    }

    @GetMapping("/{id}")
    public ResponseEntity<DatasetInfo> getDataset(@PathVariable String id) {
        DatasetInfo info = fileIngestionService.getDataset(id);
        if (info == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(info);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteDataset(@PathVariable String id) {
        try {
            fileIngestionService.removeDataset(id);
            return ResponseEntity.ok(Map.of("status", "deleted"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
