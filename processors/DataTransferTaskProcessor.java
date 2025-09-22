package com.test.dataflowengine.processors;

import com.test.dataflowengine.factories.ConnectionPoolFactory;
import com.test.dataflowengine.models.ETLTask;
import com.test.dataflowengine.models.JobDetails;
import com.test.dataflowengine.models.TaskStatus;
import com.test.dataflowengine.models.enums.DataEndpointType;
import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.readers.*;
import com.test.dataflowengine.writers.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

@Slf4j
@Service
public class DataTransferTaskProcessor implements ITaskProcessor {

    @Autowired private ConnectionPoolFactory connectionPoolFactory;

    // ======================== PUBLIC ENTRY ========================
    @Override
    public TaskStatus ProcessTask(ETLTask task, JobDetails job) {
        final Instant started = Instant.now();
        final DataTaskSettings settings = task.getSettingsAsType(DataTaskSettings.class);
        final int batchSize = Math.max(1, settings.getBatchsize());
        final boolean rollbackOnError = settings.isRollbackonError();
        final boolean parallel = settings.isEnableparallelprocessing();

        log.info("[ETL] Start: id={}, name='{}', jobId={}, batchSize={}, parallel={}, rollbackOnError={}",
                task.getId(), task.getName(), job.getJobId(), batchSize, parallel, rollbackOnError);

        DataReader reader = null;
        DataWriter writer = null;

        try {
            reader = createReader(settings);
            writer = createWriter(settings, parallel);

            reader.open();
            writer.open();

            if (parallel) {
                runParallelStreaming(settings, reader, writer, batchSize, rollbackOnError);
            } else {
                runSingleThreaded(settings, reader, writer, batchSize, rollbackOnError);
            }

            log.info("[ETL] Success: id={} in {} ms", task.getId(),
                    Duration.between(started, Instant.now()).toMillis());
            return TaskStatus.SUCCESS;

        } catch (Exception ex) {
            log.error("[ETL] Failed: {}", ex.toString(), ex);
            if (writer != null && rollbackOnError) {
                safeRollback(writer);
            }
            return TaskStatus.FAILURE;

        } finally {
            safeClose(writer);
            safeClose(reader);
        }
    }

    // ======================== EXECUTION MODES ========================
    private void runSingleThreaded(DataTaskSettings settings,
                                   DataReader reader,
                                   DataWriter writer,
                                   int batchSize,
                                   boolean rollbackOnError) throws Exception {
        int total = 0, batches = 0;
        try {
            while (true) {
                List<Map<String, Object>> batch = reader.readBatch(batchSize);
                if (batch == null || batch.isEmpty()) break;
                writer.writeBatch(batch);
                total += batch.size();
                batches++;
            }
            writer.commit();
            log.info("[ETL] Single-threaded: wrote {} rows in {} batches.", total, batches);
        } catch (Exception e) {
            if (rollbackOnError) safeRollback(writer);
            throw e;
        }
    }

    /**
     * True streaming parallelism:
     * - Start producer and consumers together.
     * - Bounded queue -> natural backpressure (producer blocks when consumers lag).
     * - Do not wait for producer first; await consumers and producer together.
     * - DB: N writers with dedicated connections (no sharing).
     * - FILE: N part writers + merge at end.
     */
    private void runParallelStreaming(DataTaskSettings settings,
                                      DataReader reader,
                                      DataWriter baseWriter,
                                      int batchSize,
                                      boolean rollbackOnError) throws Exception {

        final int workers = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        final int queueCapacityBatches = Math.max(4, workers * 3); // small, to keep memory bounded

        final boolean isFileDest = isFile(settings.getDestination().getType());
        final SupportsPartitionFiles partCap =
                (baseWriter instanceof SupportsPartitionFiles) ? (SupportsPartitionFiles) baseWriter : null;

        if (isFileDest && partCap == null) {
            log.warn("[ETL] File destination without SupportsPartitionFiles; falling back to single-threaded.");
            runSingleThreaded(settings, reader, baseWriter, batchSize, rollbackOnError);
            return;
        }

        // Build per-thread writers
        final List<DataWriter> writers = new ArrayList<>(workers);
        try {
            if (isFileDest) {
                for (int i = 0; i < workers; i++) {
                    DataWriter w = partCap.createPartWriter(i);
                    w.open();
                    writers.add(w);
                }
            } else {
                // Database destination: fresh instance + dedicated connection per worker
                for (int i = 0; i < workers; i++) {
                    DataWriter w = newDatabaseWriter(settings);
                    if (w instanceof SupportsConnectionSupplier) {
                        ((SupportsConnectionSupplier) w).setConnectionSupplier(buildConnectionSupplier(settings.getDestination()));
                    }
                    w.open();
                    writers.add(w);
                }
            }

            // Streaming queue (batches). Keep batches reasonably sized.
            final BlockingQueue<List<Map<String, Object>>> queue =
                    new ArrayBlockingQueue<>(queueCapacityBatches);
            final List<Map<String, Object>> POISON = Collections.emptyList();

            final ExecutorService pool = Executors.newFixedThreadPool(workers + 1);

            // Producer starts now (does NOT preload everything)
            final Future<?> producerF = pool.submit(() -> {
                long produced = 0;
                int batchNo = 0;
                try {
                    while (true) {
                        List<Map<String, Object>> b = reader.readBatch(batchSize);
                        if (b == null || b.isEmpty()) break;
                        batchNo++;
                        queue.put(b); // blocks if queue is full -> backpressure
                        produced += b.size();
                        if ((batchNo & 15) == 0) {
                            log.debug("[ETL] Producer queued batch#{} (rows queued so far: {})", batchNo, produced);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Producer interrupted", ie);
                } catch (Exception e) {
                    throw new RuntimeException("Producer failed", e);
                } finally {
                    // Signal termination to all consumers
                    for (int i = 0; i < workers; i++) {
                        try { queue.put(POISON); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                    }
                    log.info("[ETL] Producer finished. Total rows queued: {}, batches: {}", produced, batchNo);
                }
            });

            // Consumers start immediately (overlap with producer)
            final List<Future<Long>> consumerFs = new ArrayList<>(workers);
            for (int idx = 0; idx < workers; idx++) {
                final DataWriter w = writers.get(idx);
                final int consumerIndex = idx;
                consumerFs.add(pool.submit(() -> {
                    long written = 0;
                    int bno = 0;
                    try {
                        while (true) {
                            List<Map<String, Object>> b = queue.take();
                            if (b == POISON) break;
                            if (!b.isEmpty()) {
                                w.writeBatch(b);
                                written += b.size();
                                bno++;
                                if ((bno & 31) == 0) {
                                    log.debug("[ETL] Consumer-{} wrote batch#{} (rows written so far: {})",
                                            consumerIndex, bno, written);
                                }
                            }
                        }
                        return written;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Consumer-" + consumerIndex + " interrupted", ie);
                    } catch (Exception e) {
                        throw new RuntimeException("Consumer-" + consumerIndex + " failed", e);
                    }
                }));
            }

            // Wait for BOTH sides concurrently; if any consumer fails, cancel producer.
            long total = 0;
            try {
                // As consumers finish, their get() returns; this allows true overlap during processing
                for (Future<Long> f : consumerFs) {
                    total += f.get(); // bubbles exceptions from consumers
                }
                // Only after consumers done, ensure producer has finished too
                producerF.get(); // bubbles producer exceptions
            } catch (Exception any) {
                // Fail-fast: stop producer and consumers
                producerF.cancel(true);
                for (Future<Long> f : consumerFs) f.cancel(true);
                throw any;
            }

            // Finish / merge / commit
            if (isFileDest) {
                partCap.mergeParts(writers.size());
            } else {
                for (DataWriter w : writers) w.commit();
            }

            log.info("[ETL] Parallel streaming completed. Rows written: {}", total);
            pool.shutdownNow();

        } catch (Exception e) {
            // On DB destination, best-effort rollback across writers if requested
            if (!isFileDest && rollbackOnError) {
                for (DataWriter w : writers) safeRollback(w);
            }
            throw e;
        } finally {
            for (DataWriter w : writers) safeClose(w);
            safeClose(baseWriter); // base writer not used for IO in parallel but may hold resources
        }
    }

    // ======================== BUILDERS ========================
    private DataReader createReader(DataTaskSettings settings) {
        DataEndpointSettings src = settings.getSource();
        DataEndpointType t = normalizeType(src.getType());
        switch (t) {
            case database: return new DatabaseReader(connectionPoolFactory, settings);
            case script:   return new ScriptReader(connectionPoolFactory, settings);
            case file: {
                String ft = safeLower(src.getFileSettings().getFileType());
                switch (ft) {
                    case "csv":   return new CsvFileReader(settings);
                    case "excel": return new ExcelFileReader(settings);
                    case "json":  return new JsonFileReader(settings);
                    default: throw new IllegalArgumentException("Unsupported file source type: " + ft);
                }
            }
            default: throw new IllegalArgumentException("Unsupported source type: " + src.getType());
        }
    }

    private DataWriter createWriter(DataTaskSettings settings, boolean forParallel) {
        DataEndpointSettings dest = settings.getDestination();
        DataEndpointType t = normalizeType(dest.getType());
        switch (t) {
            case database: {
                DataWriter w = new DatabaseWriter(connectionPoolFactory, settings);
                if (forParallel && w instanceof SupportsConnectionSupplier) {
                    ((SupportsConnectionSupplier) w).setConnectionSupplier(buildConnectionSupplier(dest));
                }
                return w;
            }
            case file: {
                String ft = safeLower(dest.getFileSettings().getFileType());
                switch (ft) {
                    case "csv":   return new CsvFileWriter(settings);
                    case "excel": return new ExcelFileWriter(settings);
                    case "json":  return new JsonFileWriter(settings);
                    default: throw new IllegalArgumentException("Unsupported file destination type: " + ft);
                }
            }
            default: throw new IllegalArgumentException("Unsupported destination type: " + dest.getType());
        }
    }

    /** Only used when we need extra DB writer instances for parallel mode */
    private DataWriter newDatabaseWriter(DataTaskSettings settings) {
        return new DatabaseWriter(connectionPoolFactory, settings);
    }

    private Supplier<Connection> buildConnectionSupplier(DataEndpointSettings dest) {
        return () -> {
            try {
                DatabaseSettings ds = dest.getDatabaseSettings();
                DataSource dataSource = connectionPoolFactory.getDataSource(ds.getDatabaseId());
                return dataSource.getConnection();
            } catch (Exception e) {
                throw new RuntimeException("Failed to obtain destination connection", e);
            }
        };
    }

    // ======================== HELPERS ========================
    private static void safeClose(Object x) {
        try {
            if (x instanceof DataWriter) ((DataWriter) x).close();
            else if (x instanceof DataReader) ((DataReader) x).close();
        } catch (Exception e) {
            log.warn("close() failed: {}", e.toString());
        }
    }
    private static void safeRollback(DataWriter w) {
        try { if (w != null) w.rollback(); } catch (Exception e) { log.warn("rollback() failed: {}", e.toString()); }
    }
    private static String safeLower(String s){ return s==null? "" : s.trim().toLowerCase(Locale.ROOT); }
    private static boolean isFile(DataEndpointType t){ return t == DataEndpointType.file; }
    /** Treat Hive as database for processing */
    private static DataEndpointType normalizeType(DataEndpointType t){
        if (t == DataEndpointType.Hive) return DataEndpointType.database;
        return t;
    }
}
