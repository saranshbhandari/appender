
package com.test.dataflowengine.models;

import com.test.dataflowengine.models.settings.DataTaskSettings;

public class ETLTask {
    private Long id;
    private String name;
    private String type;
    private DataTaskSettings settings;

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getType() { return type; }
    public DataTaskSettings getSettingsAsType(Class<DataTaskSettings> cls) { return settings; }
}
