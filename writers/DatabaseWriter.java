
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

@Slf4j
public class DatabaseWriter implements DataWriter, SupportsConnectionSupplier {

    private final ConnectionPoolFactory poolFactory;
    private final DataTaskSettings settings;
    private final DataEndpointSettings dest;
    private Supplier<Connection> connectionSupplier;

    private Connection conn;
    private PreparedStatement ps;
    private List<String> destColumnsOrder;
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
        DataSource ds = poolFactory.getDataSource(db.getDatabaseId());
        this.dbType = db.getDatabaseType();

        conn = (connectionSupplier != null) ? connectionSupplier.get() : ds.getConnection();
        conn.setAutoCommit(false);

        destColumnsOrder = (settings.getMappings()==null || settings.getMappings().isEmpty())
                ? fetchTableColumns(conn, db) :
                settings.getMappings().stream().map(SourceDestinationMapping::getDestinationColumn)
                        .collect(Collectors.toList());

        String sql = SqlUtil.buildInsert(dbType, db.getSchemaName(), db.getTableName(), destColumnsOrder);
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void writeBatch(List<Map<String, Object>> rows) throws Exception {
        for (Map<String,Object> row : rows) {
            for (int i=0;i<destColumnsOrder.size();i++) {
                ps.setObject(i+1, row.getOrDefault(destColumnsOrder.get(i), null));
            }
            ps.addBatch();
        }
        ps.executeBatch();
    }

    @Override public void commit() throws Exception { if (conn!=null) conn.commit(); }
    @Override public void rollback() throws Exception { if (conn!=null) conn.rollback(); }

    @Override
    public void close() {
        try { if (ps!=null) ps.close(); } catch(Exception e){}
        try { if (conn!=null) conn.close(); } catch(Exception e){}
    }

    private static List<String> fetchTableColumns(Connection c, DatabaseSettings db) throws Exception {
        String schema = db.getSchemaName();
        String table  = db.getTableName();
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = c.getMetaData().getColumns(null, schema, table, null)) {
            while (rs.next()) cols.add(rs.getString("COLUMN_NAME"));
        }
        if (cols.isEmpty()) throw new IllegalStateException("No columns for "+schema+"."+table);
        return cols;
    }
}
