
package com.test.dataflowengine.writers;

import com.test.dataflowengine.factories.ConnectionPoolFactory;
import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataWriter;
import com.test.dataflowengine.processors.SupportsConnectionSupplier;
import com.test.dataflowengine.util.SqlUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
// com/citi/dataflowengine/writers/DatabaseWriter.java
@Slf4j
public class DatabaseWriter implements DataWriter, SupportsConnectionSupplier {

    private final ConnectionPoolFactory poolFactory;
    private final DataTaskSettings settings;
    private final DataEndpointSettings dest;
    private Supplier<Connection> connectionSupplier;

    private Connection conn;
    private PreparedStatement ps;
    private List<String> destColumnsOrder;
    private List<String> sourceColumnsForParams;  // NEW: aligns with dest order
    private String dbType;

    public DatabaseWriter(ConnectionPoolFactory poolFactory, DataTaskSettings settings) {
        this.poolFactory = poolFactory;
        this.settings = settings;
        this.dest = settings.getDestination();
    }

    @Override
    public void setConnectionSupplier(Supplier<Connection> s) { this.connectionSupplier = s; }

    @Override
    public void open() throws Exception {
        DatabaseSettings db = dest.getDatabaseSettings();
        javax.sql.DataSource ds = poolFactory.getDataSource(db.getDatabaseId());
        this.dbType = db.getDatabaseType();

        conn = (connectionSupplier != null) ? connectionSupplier.get() : ds.getConnection();
        conn.setAutoCommit(false);

        // 1) Build destination column order
        if (settings.getMappings() != null && !settings.getMappings().isEmpty()) {
            destColumnsOrder = new java.util.ArrayList<>();
            sourceColumnsForParams = new java.util.ArrayList<>();
            for (SourceDestinationMapping m : settings.getMappings()) {
                destColumnsOrder.add(m.getDestinationColumn());
                sourceColumnsForParams.add(m.getSourceColumn()); // align param source
            }
        } else {
            // Fallback: discover columns from table metadata and assume 1:1 names
            destColumnsOrder = fetchTableColumns(conn, db);
            sourceColumnsForParams = new java.util.ArrayList<>(destColumnsOrder);
        }

        // 2) Prepare INSERT
        String sql = com.citi.dataflowengine.util.SqlUtil.buildInsert(
                dbType, db.getSchemaName(), db.getTableName(), destColumnsOrder);
        log.info("[DBWriter] Using connection={} catalog={} schemaHint={} -> SQL: {}",
                safeConnId(conn), safe(conn.getCatalog()), db.getSchemaName(), sql);
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void writeBatch(List<Map<String, Object>> rows) throws Exception {
        int binds = 0;
        for (Map<String,Object> row : rows) {
            for (int i=0; i<destColumnsOrder.size(); i++) {
                String srcKey = sourceColumnsForParams.get(i);
                Object v = row.get(srcKey);
                ps.setObject(i+1, v);
            }
            ps.addBatch();
            binds++;
        }
        int[] counts = ps.executeBatch();
        if (counts != null) {
            long affected = java.util.Arrays.stream(counts).filter(x -> x != java.sql.Statement.SUCCESS_NO_INFO).count();
            log.debug("[DBWriter] Batch executed. statements={}, affectedCountEntries={}", binds, affected);
        } else {
            log.debug("[DBWriter] Batch executed. statements={} (no counts returned)", binds);
        }
    }

    @Override public void commit() throws Exception { if (conn!=null) { conn.commit(); log.info("[DBWriter] Committed."); } }
    @Override public void rollback() throws Exception { if (conn!=null) { conn.rollback(); log.warn("[DBWriter] Rolled back."); } }

    @Override
    public void close() {
        try { if (ps!=null) ps.close(); } catch(Exception e){ log.warn("close ps: {}", e.toString()); }
        try { if (conn!=null) conn.close(); } catch(Exception e){ log.warn("close conn: {}", e.toString()); }
    }

    private static java.util.List<String> fetchTableColumns(Connection c, DatabaseSettings db) throws Exception {
        String schema = db.getSchemaName();
        String table  = db.getTableName();
        java.util.List<String> cols = new java.util.ArrayList<>();
        try (ResultSet rs = c.getMetaData().getColumns(null, schema, table, null)) {
            while (rs.next()) cols.add(rs.getString("COLUMN_NAME"));
        }
        if (cols.isEmpty()) throw new IllegalStateException("No columns resolved for "+schema+"."+table);
        return cols;
    }

    private static String safeConnId(Connection c) {
        try { return Integer.toHexString(System.identityHashCode(c)); } catch (Throwable t) { return "?"; }
    }
    private static String safe(String s) { return s==null? "" : s; }
}
