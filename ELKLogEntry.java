package com.yourco.logging.model;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
public class ELKLogEntry {
    private String module;
    private String component;
    private String docId;
    private String eventType;
    private String logType;        // INFO/WARN/ERROR etc.
    private String server;
    private String timestamp = Instant.now().toString();

    private String soeid;
    private String traceId;
    private String spanId;

    private String responseCode;
    private double responseTime;

    private String accessToken;
    private String request;
    private String response;
    private String errorMsg;

    private String logMsg;
}
