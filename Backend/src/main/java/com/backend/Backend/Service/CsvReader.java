package com.backend.Backend.Service;

import com.backend.Backend.Dto.ClickHouseConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Connection;
import java.util.List;
import java.util.Map;


public interface CsvReader {

     ResponseEntity<?> getCsvColumn(MultipartFile file, String delimiter);
     ResponseEntity<?> getCsvColumn(MultipartFile file, String delimiter , boolean newTable , String tableName);
     ResponseEntity<?> uploadCsvWithParallelBatch(MultipartFile file,
                                                                    List<String> selectedColumns,
                                                                    String tableName,
                                                                    String delimiter ,
                                                                    ClickHouseConfig config
     );
     boolean checkIfTableExists(Connection connection, String tableName);
}
