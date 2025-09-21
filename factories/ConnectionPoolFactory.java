
package com.test.dataflowengine.factories;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Stub pool factory. Replace the lookup with your DB metadata store.
 * Add Hive driver if needed: org.apache.hive.jdbc.HiveDriver
 */
public class ConnectionPoolFactory {
    private final Map<Long, DataSource> pools = new ConcurrentHashMap<>();

    public DataSource getDataSource(Long dbId) {
        return pools.computeIfAbsent(dbId, id -> {
            BasicDataSource ds = new BasicDataSource();
            // TODO: swap with real URL/driver per dbId
            ds.setUrl("jdbc:h2:mem:testdb");
            ds.setUsername("sa");
            ds.setPassword("");
            ds.setDriverClassName("org.h2.Driver");
            ds.setMaxTotal(20);
            return ds;
        });
    }
}
