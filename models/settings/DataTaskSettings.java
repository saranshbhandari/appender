
package com.test.dataflowengine.models.settings;

import java.util.List;

public class DataTaskSettings {
    private int batchsize;
    private boolean rollbackonError;
    private boolean enableparallelprocessing;
    private DataEndpointSettings source;
    private DataEndpointSettings destination;
    private List<SourceDestinationMapping> mappings;

    public int getBatchsize() { return batchsize; }
    public boolean isRollbackonError() { return rollbackonError; }
    public boolean isEnableparallelprocessing() { return enableparallelprocessing; }
    public DataEndpointSettings getSource() { return source; }
    public DataEndpointSettings getDestination() { return destination; }
    public List<SourceDestinationMapping> getMappings() { return mappings; }
}
