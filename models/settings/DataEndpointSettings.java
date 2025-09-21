
package com.test.dataflowengine.models.settings;

import com.test.dataflowengine.models.enums.DataEndpointType;

public class DataEndpointSettings {
    private DataEndpointType type;
    private DatabaseSettings databaseSettings;
    private FileSettings fileSettings;
    private Object settings;

    public DataEndpointType getType() { return type; }
    public DatabaseSettings getDatabaseSettings() { return databaseSettings; }
    public FileSettings getFileSettings() { return fileSettings; }
    public Object getSettings() { return settings; }
}
