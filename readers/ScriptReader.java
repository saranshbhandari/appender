
package com.test.dataflowengine.readers;

import com.test.dataflowengine.factories.ConnectionPoolFactory;
import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataReader;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
public class ScriptReader implements DataReader {

    private final ConnectionPoolFactory poolFactory;
    private final DataTaskSettings settings;
    private final ScriptSettings scriptSettings;

    private Connection conn;
    private Statement stmt;
    private ResultSet rs;

    public ScriptReader(ConnectionPoolFactory poolFactory, DataTaskSettings settings) {
        this.poolFactory = poolFactory;
        this.settings = settings;
        this.scriptSettings = (ScriptSettings) settings.getSource().getSettings();
    }

    @Override
    public void open() throws Exception {
        Long dbId = scriptSettings.getDatabaseId();
        DataSource ds = poolFactory.getDataSource(dbId);
        conn = ds.getConnection();
        conn.setReadOnly(true);

        String sql = scriptSettings.getScript();
        stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rs   = stmt.executeQuery(sql);
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
        try { if (stmt!=null) stmt.close(); } catch(Exception e){}
        try { if (conn!=null) conn.close(); } catch(Exception e){}
    }
}
