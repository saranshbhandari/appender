package com.yourco.logging.appenders;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.Level;

import com.yourco.logging.model.ELKLogEntry;
import com.yourco.shared.http.HttpClientUtil;

import org.springframework.http.ResponseEntity;

import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class ELKLogAppender extends AppenderBase<ILoggingEvent> {

    // ----------- configurable via logback-spring.xml (springProperty) -----------
    private String logLevel;             // optional floor; e.g. INFO
    private String elkUrl;               // POST endpoint (accepts ELKLogEntry)
    private String component;
    private String module;

    private boolean EnableELKLogging = true;

    private int connectTimeoutMs = 2000;
    private int readTimeoutMs    = 2000;

    private int queueSize        = 1000; // internal queue capacity
    private int drainIntervalMs  = 500;  // worker idle sleep when queue empty
    private int maxRetries       = 2;    // small retry count
    private long baseBackoffMs   = 200;  // 200 → 400 → 800...

    private boolean sendAsync    = true; // false = synchronous in append()

    // ----------------- runtime fields -----------------
    private transient HttpClientUtil http;
    private transient BlockingQueue<ELKLogEntry> q;
    private transient ExecutorService singleWorker;
    private final AtomicBoolean workerRunning = new AtomicBoolean(false);

    private String serverName = "unknown";

    // metrics-ish
    private final LongAdder dropped = new LongAdder();
    private final LongAdder sent    = new LongAdder();
    private final LongAdder failed  = new LongAdder();

    // ----------------- setters for logback -----------------
    public void setLogLevel(String logLevel) { this.logLevel = logLevel; }
    public void setElkUrl(String elkUrl) { this.elkUrl = elkUrl; }
    public void setComponent(String component) { this.component = component; }
    public void setModule(String module) { this.module = module; }
    public void setEnableELKLogging(boolean enable) { this.EnableELKLogging = enable; }
    public void setConnectTimeoutMs(int v) { this.connectTimeoutMs = v; }
    public void setReadTimeoutMs(int v) { this.readTimeoutMs = v; }
    public void setQueueSize(int v) { this.queueSize = v; }
    public void setDrainIntervalMs(int v) { this.drainIntervalMs = v; }
    public void setMaxRetries(int v) { this.maxRetries = v; }
    public void setBaseBackoffMs(long v) { this.baseBackoffMs = v; }
    public void setSendAsync(boolean v) { this.sendAsync = v; }

    // ----------------- lifecycle -----------------
    @Override
    public void start() {
        if (!EnableELKLogging) {
            addInfo("ELK logging disabled by config; appender will not start.");
            super.start();
            return;
        }
        if (elkUrl == null || elkUrl.isBlank()) {
            addError("elkUrl is required to start ELKLogAppender");
            return;
        }

        try {
            serverName = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {}

        http = new HttpClientUtil(connectTimeoutMs, readTimeoutMs);
        q = new ArrayBlockingQueue<>(Math.max(64, queueSize));

        if (sendAsync) {
            singleWorker = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "elk-log-worker");
                t.setDaemon(true);
                return t;
            });

            workerRunning.set(true);
            singleWorker.submit(this::runWorker);
        }

        super.start();
        addInfo("ELKLogAppender started. async=" + sendAsync + " url=" + elkUrl);
    }

    @Override
    public void stop() {
        if (!isStarted()) return;
        workerRunning.set(false);

        if (singleWorker != null) {
            singleWorker.shutdown();
            try { singleWorker.awaitTermination(3, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }

        // best-effort flush remaining logs synchronously
        if (q != null) {
            ELKLogEntry e;
            while ((e = q.poll()) != null) {
                postOnceWithRetry(e);
            }
        }

        super.stop();
        addInfo("ELKLogAppender stopped. sent=" + sent.sum() + " failed=" + failed.sum() + " dropped=" + dropped.sum());
    }

    // ----------------- core -----------------
    @Override
    protected void append(ILoggingEvent event) {
        if (!EnableELKLogging) return;
        if (!passesLevel(event)) return;

        ELKLogEntry dto = toDto(event);

        if (sendAsync) {
            if (!q.offer(dto)) {
                // queue full -> drop oldest to keep system moving
                q.poll();
                if (!q.offer(dto)) dropped.increment();
            }
        } else {
            postOnceWithRetry(dto); // synchronous path
        }
    }

    private boolean passesLevel(ILoggingEvent e) {
        if (logLevel == null || logLevel.isBlank()) return true;
        try {
            Level floor = Level.valueOf(logLevel.trim());
            return e.getLevel().isGreaterOrEqual(floor);
        } catch (IllegalArgumentException ex) {
            // bad config → do not filter
            return true;
        }
    }

    private ELKLogEntry toDto(ILoggingEvent e) {
        Map<String, String> mdc = e.getMDCPropertyMap();

        ELKLogEntry log = new ELKLogEntry();
        log.setModule(module);
        log.setComponent(component);
        log.setServer(serverName);

        log.setLogType(Objects.toString(e.getLevel(), null));
        log.setEventType("LOG"); // or adapt if you need

        // Map MDC fields if available
        if (mdc != null) {
            log.setTraceId(mdc.getOrDefault("traceid", mdc.get("traceId")));
            log.setSpanId(mdc.getOrDefault("spanid", mdc.get("spanId")));
            log.setSoeid(mdc.getOrDefault("soeid", mdc.get("user")));
            // You can map request/response/errorMsg from MDC if you add them elsewhere
            log.setRequest(mdc.get("request"));
            log.setResponse(mdc.get("response"));
            log.setErrorMsg(mdc.get("error"));
            log.setResponseCode(mdc.get("responseCode"));
            try {
                if (mdc.get("responseTime") != null) {
                    log.setResponseTime(Double.parseDouble(mdc.get("responseTime")));
                }
            } catch (NumberFormatException ignored) {}
        }

        // Actual log line
        log.setLogMsg(e.getFormattedMessage());
        return log;
    }

    private void runWorker() {
        while (workerRunning.get() && !Thread.currentThread().isInterrupted()) {
            try {
                ELKLogEntry next = q.poll(drainIntervalMs, TimeUnit.MILLISECONDS);
                if (next != null) {
                    postOnceWithRetry(next);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                addError("ELK worker unexpected error", ex);
            }
        }
    }

    private void postOnceWithRetry(ELKLogEntry dto) {
        long backoff = baseBackoffMs;
        int attempt = 0;
        while (true) {
            try {
                ResponseEntity<Void> res = http.post(elkUrl, dto, Void.class);
                if (res.getStatusCode().is2xxSuccessful()) {
                    sent.increment();
                    return;
                }
                throw new RuntimeException("Non-2xx: " + res.getStatusCode());
            } catch (Exception ex) {
                attempt++;
                if (attempt > maxRetries) {
                    failed.increment();
                    addWarn("Failed to POST log after " + attempt + " attempts: " + ex.getMessage());
                    return;
                }
                try { Thread.sleep(backoff); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); return; }
                backoff = Math.min(backoff * 2, 2000);
            }
        }
    }
}
