
package com.test.dataflowengine.processors;

import com.test.dataflowengine.factories.ConnectionPoolFactory;
import com.test.dataflowengine.models.ETLTask;
import com.test.dataflowengine.models.JobDetails;
import com.test.dataflowengine.models.TaskStatus;
import com.test.dataflowengine.models.enums.DataEndpointType;
import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.readers.*;
import com.test.dataflowengine.writers.*;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import lombok.extern.slf4j.Slf4j;

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

            if (parallel) runParallel(settings, reader, writer, batchSize, rollbackOnError);
            else          runSingle(settings, reader, writer, batchSize, rollbackOnError);

            log.info("[ETL] Success: id={} in {} ms", task.getId(),
                    Duration.between(started, Instant.now()).toMillis());
            return TaskStatus.SUCCESS;

        } catch (Exception ex) {
            log.error("[ETL] Failed: {}", ex.toString(), ex);
            if (writer != null && rollbackOnError) {
                try { writer.rollback(); log.warn("[ETL] Rolled back destination changes."); }
                catch (Exception rb) { log.error("[ETL] Rollback failed: {}", rb.toString(), rb); }
            }
            return TaskStatus.FAILURE;

        } finally {
            safeClose(writer);
            safeClose(reader);
        }
    }

    private void runSingle(DataTaskSettings settings, DataReader reader, DataWriter writer, int batchSize, boolean rollbackOnError) throws Exception {
        int total = 0, batches = 0;
        try {
            while (true) {
                List<java.util.Map<String, Object>> batch = reader.readBatch(batchSize);
                if (batch == null || batch.isEmpty()) break;
                writer.writeBatch(batch);
                total += batch.size(); batches++;
            }
            writer.commit();
            log.info("[ETL] Single-threaded wrote {} rows in {} batches.", total, batches);
        } catch (Exception e) {
            if (rollbackOnError) safeRollback(writer);
            throw e;
        }
    }

    private void runParallel(DataTaskSettings settings, DataReader reader, DataWriter baseWriter, int batchSize, boolean rollbackOnError) throws Exception {
        final int workers = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        final int queueCap = Math.max(4, workers * 2);

        final boolean isFileDest = settings.getDestination().getType() == DataEndpointType.file;
        SupportsPartitionFiles partCap = (baseWriter instanceof SupportsPartitionFiles)
                ? (SupportsPartitionFiles) baseWriter : null;

        if (isFileDest && partCap == null) {
            log.warn("[ETL] File destination without SupportsPartitionFiles; falling back to single-threaded.");
            runSingle(settings, reader, baseWriter, batchSize, rollbackOnError);
            return;
        }

        List<DataWriter> writers = new ArrayList<>(workers);
        try {
            if (isFileDest) {
                for (int i=0; i<workers; i++) { DataWriter w = partCap.createPartWriter(i); w.open(); writers.add(w); }
            } else {
                for (int i=0; i<workers; i++) {
                    DataWriter w = newDatabaseWriter(settings);
                    if (w instanceof SupportsConnectionSupplier) {
                        ((SupportsConnectionSupplier) w).setConnectionSupplier(buildConnectionSupplier(settings.getDestination()));
                    }
                    w.open(); writers.add(w);
                }
            }

            BlockingQueue<List<java.util.Map<String,Object>>> q = new ArrayBlockingQueue<>(queueCap);
            final List<java.util.Map<String,Object>> POISON = java.util.Collections.emptyList();
            ExecutorService pool = Executors.newFixedThreadPool(workers + 1);

            Future<?> prodF = pool.submit(() -> {
                int produced = 0;
                try {
                    while (true) {
                        List<java.util.Map<String,Object>> b = reader.readBatch(batchSize);
                        if (b == null || b.isEmpty()) break;
                        q.put(b); produced += b.size();
                    }
                } catch (Exception e) { throw new RuntimeException(e); }
                finally {
                    for (int i=0;i<writers.size();i++) { try { q.put(POISON); } catch (InterruptedException ie){ Thread.currentThread().interrupt(); } }
                    log.info("[ETL] Producer finished. Rows queued: {}", produced);
                }
            });

            List<Future<Long>> consFs = new ArrayList<>();
            for (int idx=0; idx<writers.size(); idx++) {
                final DataWriter w = writers.get(idx);
                consFs.add(pool.submit(() -> {
                    long written = 0;
                    try {
                        while (true) {
                            List<java.util.Map<String,Object>> b = q.take();
                            if (b == POISON) break;
                            if (!b.isEmpty()) { w.writeBatch(b); written += b.size(); }
                        }
                        return written;
                    } catch (Exception ex) { throw ex; }
                }));
            }

            prodF.get();
            long total = 0;
            for (Future<Long> f : consFs) total += f.get();

            if (isFileDest) partCap.mergeParts(writers.size());
            else for (DataWriter w : writers) w.commit();

            log.info("[ETL] Parallel write completed. Rows written: {}", total);
            pool.shutdownNow();

        } catch (Exception e) {
            if (!isFileDest && rollbackOnError) for (DataWriter w : writers) safeRollback(w);
            throw e;
        } finally {
            for (DataWriter w : writers) safeClose(w);
            safeClose(baseWriter);
        }
    }

    private DataReader createReader(DataTaskSettings settings) {
        DataEndpointSettings src = settings.getSource();
        DataEndpointType t = src.getType() == DataEndpointType.Hive ? DataEndpointType.database : src.getType();
        switch (t) {
            case database: return new DatabaseReader(connectionPoolFactory, settings);
            case script:   return new ScriptReader(connectionPoolFactory, settings);
            case file: {
                String type = safeLower(settings.getSource().getFileSettings().getFileType());
                switch (type) {
                    case "csv": return new CsvFileReader(settings);
                    case "excel": return new ExcelFileReader(settings);
                    case "json": return new JsonFileReader(settings);
                }
            }
        }
        throw new IllegalArgumentException("Unsupported source type: " + src.getType());
    }

    private DataWriter createWriter(DataTaskSettings settings, boolean forParallel) {
        DataEndpointSettings dest = settings.getDestination();
        DataEndpointType t = dest.getType() == DataEndpointType.Hive ? DataEndpointType.database : dest.getType();
        switch (t) {
            case database: {
                DataWriter w = new DatabaseWriter(connectionPoolFactory, settings);
                if (forParallel && w instanceof SupportsConnectionSupplier) {
                    ((SupportsConnectionSupplier) w).setConnectionSupplier(buildConnectionSupplier(dest));
                }
                return w;
            }
            case file: {
                String type = safeLower(dest.getFileSettings().getFileType());
                switch (type) {
                    case "csv": return new CsvFileWriter(settings);
                    case "excel": return new ExcelFileWriter(settings);
                    case "json": return new JsonFileWriter(settings);
                }
            }
        }
        throw new IllegalArgumentException("Unsupported destination type: " + dest.getType());
    }

    private DataWriter newDatabaseWriter(DataTaskSettings settings) { return new DatabaseWriter(connectionPoolFactory, settings); }

    private java.util.function.Supplier<Connection> buildConnectionSupplier(DataEndpointSettings dest) {
        return () -> {
            try {
                javax.sql.DataSource dataSource = connectionPoolFactory.getDataSource(dest.getDatabaseSettings().getDatabaseId());
                return dataSource.getConnection();
            } catch (Exception e) { throw new RuntimeException("Failed to obtain destination connection", e); }
        };
    }

    private static void safeClose(Object x) {
        try {
            if (x instanceof DataWriter) ((DataWriter) x).close();
            else if (x instanceof DataReader) ((DataReader) x).close();
        } catch (Exception ignore) {}
    }
    private static void safeRollback(DataWriter w) { try { if (w != null) w.rollback(); } catch (Exception ignore) {} }
    private static String safeLower(String s){ return s==null? "" : s.trim().toLowerCase(java.util.Locale.ROOT); }
}
