
package com.test.dataflowengine.writers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataWriter;
import com.test.dataflowengine.processors.SupportsPartitionFiles;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class CsvFileWriter implements DataWriter, SupportsPartitionFiles {

    private final DataTaskSettings settings;
    private final FileSettings fs;
    private final boolean isPart;
    private final String partPath;

    private PrintWriter out;
    private List<String> headerOrder;

    public CsvFileWriter(DataTaskSettings settings) { this(settings, false, null); }
    private CsvFileWriter(DataTaskSettings settings, boolean isPart, String partPath) {
        this.settings = settings; this.fs = settings.getDestination().getFileSettings();
        this.isPart = isPart; this.partPath = partPath;
    }

    @Override
    public void open() throws Exception {
        String path = isPart ? partPath : fs.getFilePath();
        headerOrder = settings.getMappings().stream().map(SourceDestinationMapping::getDestinationColumn).collect(Collectors.toList());
        out = new PrintWriter(new FileWriter(path, false));
        out.println(String.join(fs.getFileDelimiter(), headerOrder));
    }

    @Override
    public void writeBatch(List<Map<String, Object>> rows) {
        for (Map<String,Object> r : rows) {
            List<String> vals = new ArrayList<>();
            for (String h : headerOrder) vals.add(String.valueOf(r.getOrDefault(h, "")));
            out.println(String.join(fs.getFileDelimiter(), vals));
        }
    }

    @Override public void commit() { if (out!=null) out.flush(); }
    @Override public void rollback() { /* no-op */ }
    @Override public void close() { if (out!=null) out.close(); }

    @Override
    public DataWriter createPartWriter(int partIndex) {
        String base = fs.getFilePath();
        String part = base.replaceFirst("\.(csv)$", "_part" + partIndex + ".$1");
        return new CsvFileWriter(settings, true, part);
    }

    @Override
    public void mergeParts(int producedParts) throws Exception {
        String base = fs.getFilePath();
        try (PrintWriter merged = new PrintWriter(new FileWriter(base, false))) {
            merged.println(String.join(fs.getFileDelimiter(), headerOrder));
            for (int i=0;i<producedParts;i++) {
                String partPath = base.replaceFirst("\.(csv)$", "_part" + i + ".$1");
                try (BufferedReader br = new BufferedReader(new FileReader(partPath))) {
                    br.readLine(); // skip header
                    String line; while ((line = br.readLine()) != null) merged.println(line);
                }
                new java.io.File(partPath).delete();
            }
        }
    }
}
