
package com.test.dataflowengine.models.settings;

public class DatabaseSettings {
    private Long databaseId;
    private String databaseType; // oracle, mysql, mssql, hive
    private String databaseName;
    private String schemaName;
    private String tableName;

    public Long getDatabaseId() { return databaseId; }
    public String getDatabaseType() { return databaseType; }
    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
}
