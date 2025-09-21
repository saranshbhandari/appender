
package com.test.dataflowengine.readers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.util.*;

@Slf4j
public class ExcelFileReader implements DataReader {

    private final DataTaskSettings settings;
    private final FileSettings fs;

    private Workbook wb;
    private Sheet sheet;
    private Iterator<Row> iter;
    private List<String> headers;
    private Map<String,Object> bufferedFirst = null;

    public ExcelFileReader(DataTaskSettings settings) {
        this.settings = settings;
        this.fs = settings.getSource().getFileSettings();
    }

    @Override
    public void open() throws Exception {
        wb = new XSSFWorkbook(new FileInputStream(fs.getFilePath()));
        String sheetName = (fs.getExcelSheetName() == null || fs.getExcelSheetName().isEmpty())
                ? wb.getSheetAt(0).getSheetName()
                : fs.getExcelSheetName();
        sheet = wb.getSheet(sheetName);
        iter = sheet.iterator();

        headers = new ArrayList<>();
        if (iter.hasNext()) {
            Row r = iter.next();
            for (Cell c : r) headers.add(getCellString(c));
            if (!fs.isFirstRowColumn()) { bufferedFirst = rowToMap(r, headers); }
        }
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) {
        List<Map<String,Object>> out = new ArrayList<>(batchSize);
        if (bufferedFirst != null) { out.add(bufferedFirst); bufferedFirst = null; }
        while (iter.hasNext() && out.size() < batchSize) {
            Row r = iter.next();
            out.add(rowToMap(r, headers));
        }
        return out.isEmpty() ? null : out;
    }

    @Override
    public void close() {
        try { if (wb!=null) wb.close(); } catch(Exception e){}
    }

    private static Map<String,Object> rowToMap(Row r, List<String> headers) {
        Map<String,Object> m = new LinkedHashMap<>();
        for (int i=0;i<headers.size();i++) {
            Cell c = r.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            m.put(headers.get(i), c==null ? "" : getCellString(c));
        }
        return m;
    }
    private static String getCellString(Cell c) {
        switch (c.getCellType()) {
            case STRING: return c.getStringCellValue();
            case NUMERIC: return org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(c)
                    ? c.getDateCellValue().toString() : String.valueOf(c.getNumericCellValue());
            case BOOLEAN: return Boolean.toString(c.getBooleanCellValue());
            case FORMULA: return c.getCellFormula();
            case BLANK: default: return "";
        }
    }
}
