
package com.test.dataflowengine.readers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataReader;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

@Slf4j
public class CsvFileReader implements DataReader {

    private final DataTaskSettings settings;
    private final FileSettings fs;
    private BufferedReader br;
    private String[] headers;
    private Map<String,Object> bufferedFirstRow = null;

    public CsvFileReader(DataTaskSettings settings) {
        this.settings = settings;
        this.fs = settings.getSource().getFileSettings();
    }

    @Override
    public void open() throws Exception {
        br = new BufferedReader(new FileReader(fs.getFilePath()));
        String first = br.readLine();
        if (first == null) return;
        if (fs.isFirstRowColumn()) {
            headers = first.split(fs.getFileDelimiter(), -1);
        } else {
            String[] parts = first.split(fs.getFileDelimiter(), -1);
            headers = new String[parts.length];
            for (int i=0;i<parts.length;i++) headers[i] = "C"+(i+1);
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i=0;i<parts.length;i++) row.put(headers[i], parts[i]);
            bufferedFirstRow = row;
        }
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        List<Map<String,Object>> batch = new ArrayList<>(batchSize);
        if (bufferedFirstRow != null) { batch.add(bufferedFirstRow); bufferedFirstRow=null; }
        if (br == null) return null;
        String line;
        while (batch.size() < batchSize && (line = br.readLine()) != null) {
            String[] vals = line.split(fs.getFileDelimiter(), -1);
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i=0;i<headers.length;i++) row.put(headers[i], i<vals.length ? vals[i] : "");
            batch.add(row);
        }
        return batch.isEmpty() ? null : batch;
    }

    @Override
    public void close() {
        try { if (br!=null) br.close(); } catch(Exception e){}
    }
}
