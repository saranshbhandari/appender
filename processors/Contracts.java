
package com.test.dataflowengine.processors;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface DataReader {
    void open() throws Exception;
    List<Map<String, Object>> readBatch(int batchSize) throws Exception;
    void close();
}
public interface DataWriter {
    void open() throws Exception;
    void writeBatch(List<Map<String, Object>> rows) throws Exception;
    void commit() throws Exception;
    void rollback() throws Exception;
    void close();
}
public interface SupportsConnectionSupplier { void setConnectionSupplier(Supplier<Connection> s); }
public interface SupportsPartitionFiles {
    DataWriter createPartWriter(int partIndex) throws Exception;
    void mergeParts(int parts) throws Exception;
}
