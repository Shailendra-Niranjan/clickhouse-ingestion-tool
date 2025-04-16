package com.backend.Backend.ServiceImpl;

import com.backend.Backend.Dto.ClickHouseConfig;
import com.backend.Backend.Service.CsvReader;
import com.backend.Backend.Util.ClickHouseJdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@Service
@Slf4j
public class CsvReaderImpl implements CsvReader {




    private static final int BATCH_SIZE = 500;
    private static final int THREAD_POOL_SIZE = 4;


    @Override
    public ResponseEntity<?> getCsvColumn(MultipartFile file, String delimiter) {
        try {
            if (file.isEmpty()) {
                return ResponseEntity.badRequest().body("Empty file uploaded.");
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8)
            );

            String headerLine = reader.readLine();
            if (headerLine == null) {
                return ResponseEntity.badRequest().body("CSV file has no content.");
            }

            String[] columns = headerLine.split(delimiter);
            List<String> columnList = new ArrayList<>();
            for (String col : columns) {
                columnList.add(col.trim());
            }

            return ResponseEntity.ok(columnList);

        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to parse CSV: " + e.getMessage());
        }
    }

    @Override
    public ResponseEntity<?> getCsvColumn(MultipartFile file, String delimiter, boolean newTable, String tableName) {
        return null;
    }


    public boolean checkIfTableExists(Connection connection, String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                return tables.next(); // true if table exists
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }



    @Override
    public ResponseEntity<?> uploadCsvWithParallelBatch(MultipartFile file,
                                                                          List<String> selectedColumns,
                                                                          String tableName,
                                                                          String delimiter ,
                                                                          ClickHouseConfig config
    ) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Empty CSV"));
            }

            String[] headers = headerLine.split(delimiter);
            Map<String, Integer> headerIndexMap = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                headerIndexMap.put(headers[i].trim(), i);
            }

            List<String> allLines = reader.lines().collect(Collectors.toList());
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            List<Future<Integer>> futures = new ArrayList<>();

            for (int i = 0; i < allLines.size(); i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, allLines.size());
                List<String> batch = allLines.subList(i, end);

                Future<Integer> future = executor.submit(() -> {
                    return processBatch(batch, selectedColumns, headerIndexMap, tableName, delimiter , config);
                });

                futures.add(future);
            }

            int totalInserted = 0;
            for (Future<Integer> future : futures) {
                totalInserted += future.get(); // wait for each batch
            }

            executor.shutdown();

            return ResponseEntity.ok(Map.of("inserted", totalInserted, "skipped", allLines.size() - totalInserted));

        } catch (Exception e) {
            log.error("Error during batch processing: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    private int processBatch(List<String> lines,
                             List<String> selectedColumns,
                             Map<String, Integer> headerIndexMap,
                             String tableName,
                             String delimiter,
                             ClickHouseConfig config
    ) {

        List<String> valuesList = new ArrayList<>();

        for (String line : lines) {
            try {
                String[] data = line.split(delimiter, -1);
                StringBuilder sb = new StringBuilder("(");

                for (String col : selectedColumns) {
                    Integer idx = headerIndexMap.get(col.trim());
                    if (idx == null || idx >= data.length) {
                        sb.setLength(0); // skip malformed line
                        break;
                    }

                    String val = data[idx].replace("'", "''"); // escape single quotes
                    sb.append("'").append(val).append("',");
                }

                if (sb.length() > 0) {
                    sb.setCharAt(sb.length() - 1, ')'); // replace last comma
                    valuesList.add(sb.toString());
                }

            } catch (Exception ex) {
                log.warn("Skipping bad row: {}", ex.getMessage());
            }
        }

        if (!valuesList.isEmpty()) {
            return executeInsert(valuesList, tableName, selectedColumns ,config);
        }

        return 0;
    }

    private int executeInsert(List<String> values,
                              String tableName,
                              List<String> selectedColumns ,
                              ClickHouseConfig config
    ) {
        try {
            String colStr = selectedColumns.stream()
                    .map(col -> "`" + col.trim() + "`")
                    .collect(Collectors.joining(", "));

            String valStr = String.join(",", values);

            String createTableSql = selectedColumns.stream()
                    .map(col -> "`" + col + "` String")
                    .collect(Collectors.joining(", "));

            String createQuery = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
                    tableName, createTableSql);

            String query = String.format(
                    "INSERT INTO `%s` (%s) VALUES %s",
                    tableName, colStr, valStr);



            log.info("Executing batch insert: {} rows", values.size());
            try {
                ClickHouseJdbcTemplate.execute(createQuery ,config);
                ClickHouseJdbcTemplate.execute(query, config);
            } catch (SQLException e) {
                log.error("Ingestion failed with message: {}", e.getMessage(), e);

            }

            return values.size();

        } catch (Exception e) {
            log.error("Insert failed: ", e);
            return 0;
        }
    }


}
