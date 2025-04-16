package com.backend.Backend.Dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ClickHouseConfig {
    private String host;
    private String port;
    private String database;
    private String username;
    private String token;


}
