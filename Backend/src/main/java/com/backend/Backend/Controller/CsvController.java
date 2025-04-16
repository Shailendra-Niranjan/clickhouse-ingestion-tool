package com.backend.Backend.Controller;


import com.backend.Backend.Dto.ClickHouseConfig;
import com.backend.Backend.Service.CsvReader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/csv")
public class CsvController {


    @Autowired
  public   CsvReader csvReader;




    @PostMapping("/columns")
    public ResponseEntity<?> getCsvColumns(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "delimiter", defaultValue = ",") String delimiter,
            @RequestParam(required = false , defaultValue = "false") Boolean newTable,
            @RequestParam(required = false) String tableName

    ) {

            return csvReader.getCsvColumn(file, delimiter);
    }

    @PostMapping("/check-table")
    public ResponseEntity<?> checkTableExists(
            @RequestParam("tableName") String tableName,
            @RequestParam("config") String configJson
    ) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);

            // Connection
            String url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                    config.getHost(), config.getPort(), config.getDatabase()
            );

            try (Connection connection = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
                DatabaseMetaData metaData = connection.getMetaData();

                // Force lowercase table name
                String normalizedTable = tableName.toLowerCase();

                ResultSet resultSet = metaData.getTables(config.getDatabase(), null, normalizedTable, new String[]{"TABLE"});
                boolean exists = resultSet.next();

                return ResponseEntity.ok(Map.of("exists", exists));
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }



    @PostMapping("/ingest")
    public ResponseEntity<?> ingestCsvToClickHouse(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter,
            @RequestParam("tableName") String tableName,
            @RequestParam("columns") String columnsJson,
            @RequestParam("config") String configJson) {

        try {
            // Parse config
            ObjectMapper mapper = new ObjectMapper();
            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);
            List<String> selectedColumns = mapper.readValue(columnsJson, new TypeReference<List<String>>() {});

            return csvReader.uploadCsvWithParallelBatch(file,selectedColumns,tableName,delimiter,config);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }





}
