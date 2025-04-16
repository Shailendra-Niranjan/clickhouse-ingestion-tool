package com.backend.Backend.Service;

import com.backend.Backend.Dto.ClickHouseConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface ClickhouseService {

     ResponseEntity<? extends Map<String, ? extends Object>> testConnection(ClickHouseConfig config) ;
     ResponseEntity<? extends Map<String, ? extends Object>> GetPreviewData(String tableName, String configJson , String columnsJson);
     ResponseEntity<? extends Map<String, ? extends Object>> getAllTable(String configJson);
     ResponseEntity<? extends Map<String, ? extends Object>> fetchTableColumns(String tableName, String configJson);
    ResponseEntity<?> convertTableIntoCSV(String tableName, String columnsJson, String configJson);
}
