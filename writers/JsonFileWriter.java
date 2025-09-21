
package com.test.dataflowengine.writers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataWriter;
import com.test.dataflowengine.processors.SupportsPartitionFiles;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonGenerator;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

@Slf4j
public class JsonFileWriter implements DataWriter, SupportsPartitionFiles {

    private final DataTaskSettings settings;
    private final FileSettings fs;
    private final boolean isPart;
    private final String partPath;

    private ObjectMapper mapper = new ObjectMapper();
    private JsonGenerator gen;

    public JsonFileWriter(DataTaskSettings settings) { this(settings, false, null); }
    private JsonFileWriter(DataTaskSettings settings, boolean isPart, String partPath) {
        this.settings = settings; this.fs = settings.getDestination().getFileSettings();
        this.isPart = isPart; this.partPath = partPath;
    }

    @Override
    public void open() throws Exception {
        String path = isPart ? partPath : fs.getFilePath().replaceAll("\.json$", ".ndjson");
        gen = mapper.getFactory().createGenerator(new FileOutputStream(path), com.fasterxml.jackson.core.JsonEncoding.UTF8);
    }

    @Override
    public void writeBatch(List<Map<String, Object>> rows) throws Exception {
        for (Map<String,Object> r : rows) {
            mapper.writeValue(gen, r);
            gen.writeRaw('\n');
        }
        gen.flush();
    }

    @Override public void commit() throws Exception { if (gen!=null) gen.flush(); }
    @Override public void rollback() { }
    @Override public void close() { try { if (gen!=null) gen.close(); } catch(Exception e){} }

    @Override
    public DataWriter createPartWriter(int partIndex) {
        String base = fs.getFilePath().replaceAll("\.json$", ".ndjson");
        String part = base.replaceFirst("\.ndjson$", "_part" + partIndex + ".ndjson");
        return new JsonFileWriter(settings, true, part);
    }

    @Override
    public void mergeParts(int parts) throws Exception {
        String finalJson = fs.getFilePath();
        try (BufferedWriter out = new BufferedWriter(new FileWriter(finalJson))) {
            out.write('[');
            boolean first = true;
            for (int p=0;p<parts;p++) {
                String part = finalJson.replaceAll("\.json$", ".ndjson").replaceFirst("\.ndjson$", "_part"+p+".ndjson");
                try (BufferedReader br = new BufferedReader(new FileReader(part))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!first) out.write(',');
                        out.write(line);
                        first = false;
                    }
                }
                new java.io.File(part).delete();
            }
            out.write(']');
        }
    }
}
