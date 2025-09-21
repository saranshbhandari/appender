
package com.test.dataflowengine.readers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataReader;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.util.*;

@Slf4j
public class JsonFileReader implements DataReader {

    private final DataTaskSettings settings;
    private final FileSettings fs;

    private JsonParser parser;
    private ObjectMapper mapper = new ObjectMapper();
    private boolean arrayMode;

    public JsonFileReader(DataTaskSettings settings) {
        this.settings = settings;
        this.fs = settings.getSource().getFileSettings();
    }

    @Override
    public void open() throws Exception {
        parser = new JsonFactory().createParser(new FileInputStream(fs.getFilePath()));
        JsonToken t = parser.nextToken();
        arrayMode = (t == JsonToken.START_ARRAY);
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        List<Map<String,Object>> out = new ArrayList<>(batchSize);
        if (arrayMode) {
            while (out.size() < batchSize) {
                JsonToken t = parser.nextToken();
                if (t == JsonToken.END_ARRAY || t == null) break;
                if (t == JsonToken.START_OBJECT) {
                    Map map = mapper.readValue(parser, Map.class);
                    out.add(map);
                }
            }
        } else {
            while (out.size() < batchSize) {
                Map map = mapper.readValue(parser, Map.class);
                if (map == null) break;
                out.add(map);
                if (parser.nextToken() == null) break;
            }
        }
        return out.isEmpty() ? null : out;
    }

    @Override
    public void close() {
        try { if (parser!=null) parser.close(); } catch(Exception e){}
    }
}
