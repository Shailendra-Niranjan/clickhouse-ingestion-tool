package com.backend.Backend.Util;

import com.backend.Backend.Dto.ClickHouseConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

@Component
@Slf4j
public class ClickHouseJdbcTemplate {

    static {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC Driver not found", e);
        }
    }

    public  static  void execute(String query, ClickHouseConfig config) throws SQLException {
        String url = String.format(
                "jdbc:clickhouse://%s:%s/%s?ssl=true&compress=0",
                config.getHost(), config.getPort(), config.getDatabase()
        );

        try (Connection connection = DriverManager.getConnection(url, config.getUsername(), config.getToken());
             Statement statement = connection.createStatement()) {

            log.info("Executing query: {}", query);
            statement.execute(query);
        }
    }
}
