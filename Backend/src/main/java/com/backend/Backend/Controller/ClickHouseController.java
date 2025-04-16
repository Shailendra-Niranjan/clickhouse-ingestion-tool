package com.backend.Backend.Controller;

import com.backend.Backend.Dto.ClickHouseConfig;
import com.backend.Backend.Service.ClickhouseService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/clickhouse")
public class ClickHouseController {

    @Autowired
    ClickhouseService clickhouseService;

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseController.class);



    @PostMapping("/connect")
    public ResponseEntity<?> connectClickHouse(@RequestBody ClickHouseConfig config) {
        return clickhouseService.testConnection(config);
    }



    @PostMapping("/preview-table")
    public ResponseEntity<?> previewTableData(
            @RequestParam("tableName") String tableName,
            @RequestParam("config") String configJson,
            @RequestParam String columnsJson
        ) {

        return clickhouseService.GetPreviewData(tableName, configJson , columnsJson);
    }



    @PostMapping("/get-all-tables")
    public ResponseEntity<?> getAllTables(@RequestParam("config") String configJson) {
        return clickhouseService.getAllTable(configJson);
    }



    @PostMapping("/get-table-columns")
    public ResponseEntity<?> getTableColumns(
            @RequestParam("tableName") String tableName,
            @RequestParam("config") String configJson) {

        return clickhouseService.fetchTableColumns(tableName, configJson);
    }



    @PostMapping("/fetch-table-data")
    public ResponseEntity<?> fetchTableData(
            @RequestParam("tableName") String tableName,
            @RequestParam("columns") String columnsJson,
            @RequestParam("config") String configJson) {

        return clickhouseService.convertTableIntoCSV(tableName, columnsJson, configJson);
    }



}
