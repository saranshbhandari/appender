
package com.test.dataflowengine.readers;

import com.test.dataflowengine.factories.ConnectionPoolFactory;
import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.util.SqlUtil;
import com.test.dataflowengine.processors.DataReader;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DatabaseReader implements DataReader {
    private final ConnectionPoolFactory poolFactory;
    private final DataTaskSettings settings;
    private final DataEndpointSettings source;

    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;

    public DatabaseReader(ConnectionPoolFactory poolFactory, DataTaskSettings settings) {
        this.poolFactory = poolFactory;
        this.settings = settings;
        this.source = settings.getSource();
    }

    @Override
    public void open() throws Exception {
        DatabaseSettings db = source.getDatabaseSettings();
        DataSource ds = poolFactory.getDataSource(db.getDatabaseId());
        conn = ds.getConnection();
        conn.setReadOnly(true);

        String dbType = db.getDatabaseType();
        String schema = db.getSchemaName();
        String table  = db.getTableName();

        List<String> srcCols = (settings.getMappings()==null) ? null :
                settings.getMappings().stream().map(SourceDestinationMapping::getSourceColumn)
                        .filter(Objects::nonNull).collect(Collectors.toList());

        String sql = SqlUtil.buildSelectAll(dbType, schema, table, srcCols);
        log.info("[DBReader] Query: {}", sql);

        ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        try { ps.setFetchSize(2_000); } catch (Throwable ignored) {}
        rs = ps.executeQuery();
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        if (rs == null) return null;
        List<Map<String,Object>> out = new ArrayList<>(batchSize);
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        int n = 0;
        while (n < batchSize && rs.next()) {
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i=1;i<=cols;i++) row.put(md.getColumnLabel(i), rs.getObject(i));
            out.add(row); n++;
        }
        return out.isEmpty() ? null : out;
    }

    @Override
    public void close() {
        try { if (rs!=null) rs.close(); } catch(Exception e){}
        try { if (ps!=null) ps.close(); } catch(Exception e){}
        try { if (conn!=null) conn.close(); } catch(Exception e){}
    }
}
