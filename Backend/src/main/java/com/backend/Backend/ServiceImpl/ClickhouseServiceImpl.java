package com.backend.Backend.ServiceImpl;

import com.backend.Backend.Dto.ClickHouseConfig;
import com.backend.Backend.Service.ClickhouseService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.coyote.BadRequestException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ClickhouseServiceImpl implements ClickhouseService {


    @Override
    public  ResponseEntity<? extends Map<String, ? extends Object>> testConnection(ClickHouseConfig config) {
        String url = String.format(
                "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                config.getHost(), config.getPort(), config.getDatabase()
        );

        try (Connection connection = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
            List<String> tableNames = new ArrayList<>();

            String query = "SELECT name FROM system.tables WHERE database = ?";

            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setString(1, config.getDatabase());
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }

            log.info("User-defined tables in database '" + config.getDatabase() + "': " + tableNames);
            return ResponseEntity.ok(Map.of("status", "connected", "tables", tableNames));

        } catch (SQLException e) {
            String errorMessage = e.getMessage();

            if (errorMessage.contains("Unknown database")) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("status", "failed", "error", "Invalid database name. Please check your database name."));
            }

            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("status", "failed", "error", errorMessage));
        }
    }


    @Override
    public ResponseEntity<? extends Map<String, ? extends Object>> GetPreviewData(String tableName, String configJson ,   String columnsJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            // Parse the columns and configuration from the request

            List<String> selectedColumns = mapper.readValue(columnsJson, new TypeReference<List<String>>() {});
            if(selectedColumns.isEmpty() || tableName.trim().isEmpty())return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("status", "error", "message", "Please select a table and at least one column."));
            String col = selectedColumns.stream()
                    .map(c -> "`" + c + "`")  // sirf yahan backticks
                    .collect(Collectors.joining(", "));

            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);

            String url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                    config.getHost(), config.getPort(), config.getDatabase()
            );

            try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
                Statement stmt = conn.createStatement();
                String query = String.format("SELECT %s FROM `%s` LIMIT 100",col, tableName);
                log.info("query : {}" , query);
                ResultSet rs = stmt.executeQuery(query);

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                List<List<Object>> tableData = new ArrayList<>();

                // Add column headers as first row
                List<Object> headerRow = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    headerRow.add(metaData.getColumnName(i));
                }
                tableData.add(headerRow);

                // Add data rows
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    tableData.add(row);
                }

                return ResponseEntity.ok(Map.of(
                        "status", "success",
                        "data", tableData,
                        "rowCount", tableData.size() - 1 // Exclude header row
                ));
            }

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }
    @Override
     public ResponseEntity<? extends Map<String, ? extends Object>> getAllTable(String configJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);

            // Build the connection URL
            String url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                    config.getHost(), config.getPort(), config.getDatabase()
            );
            try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
                Statement stmt = conn.createStatement();

                // Query to show all tables in the database
                String query = "SHOW TABLES";
                ResultSet rs = stmt.executeQuery(query);

                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    String tableName = rs.getString(1); // Get table name from the result set
                    tables.add(tableName);
                }

                // Return the list of tables
                return ResponseEntity.ok(Map.of("tables", tables));

            } catch (Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of("error", e.getMessage()));
            }

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

@Override
    public ResponseEntity<? extends Map<String, ? extends Object>> fetchTableColumns(String tableName, String configJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);

            String url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                    config.getHost(), config.getPort(), config.getDatabase()
            );

            try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
                Statement stmt = conn.createStatement();

                String query = String.format("DESCRIBE TABLE %s", tableName);

                ResultSet rs = stmt.executeQuery(query);

                List<String> columns = new ArrayList<>();
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    columns.add(columnName);
                }

                // Return the list of columns
                return ResponseEntity.ok(Map.of("columns", columns));

            } catch (Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of("error", e.getMessage()));
            }

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @Override
    public ResponseEntity<?> convertTableIntoCSV(String tableName, String columnsJson, String configJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            // Parse the columns and configuration from the request
            List<String> selectedColumns = mapper.readValue(columnsJson, new TypeReference<List<String>>() {});
            ClickHouseConfig config = mapper.readValue(configJson, ClickHouseConfig.class);

            String url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                    config.getHost(), config.getPort(), config.getDatabase()
            );


            try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getToken())) {
                Statement stmt = conn.createStatement();


                String columnsList = String.join(", ", selectedColumns);
                String query = String.format("SELECT %s FROM %s", columnsList, tableName);


                ResultSet rs = stmt.executeQuery(query);


                StringBuilder csvContent = new StringBuilder();

                csvContent.append(String.join(",", selectedColumns)).append("\n");


                while (rs.next()) {
                    for (int i = 0; i < selectedColumns.size(); i++) {
                        String value = rs.getString(i + 1); // ResultSet is 1-indexed
                        csvContent.append(value == null ? "" : value).append(i == selectedColumns.size() - 1 ? "\n" : ",");
                    }
                }

                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=data.csv")
                        .body(csvContent.toString());


            } catch (Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of("error", e.getMessage()));
            }

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

}
