
package com.test.dataflowengine.writers;

import com.test.dataflowengine.models.settings.*;
import com.test.dataflowengine.processors.DataWriter;
import com.test.dataflowengine.processors.SupportsPartitionFiles;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ExcelFileWriter implements DataWriter, SupportsPartitionFiles {

    private final DataTaskSettings settings;
    private final FileSettings fs;
    private final boolean isPart;
    private final String partPath;

    private Workbook wb;
    private Sheet sheet;
    private List<String> headerOrder;
    private int currentRow = 0;

    public ExcelFileWriter(DataTaskSettings settings) { this(settings, false, null); }
    private ExcelFileWriter(DataTaskSettings settings, boolean isPart, String partPath) {
        this.settings = settings; this.fs = settings.getDestination().getFileSettings();
        this.isPart = isPart; this.partPath = partPath;
    }

    @Override
    public void open() throws Exception {
        wb = new XSSFWorkbook();
        String sheetName = (fs.getExcelSheetName()==null||fs.getExcelSheetName().isEmpty()) ? "Sheet1" : fs.getExcelSheetName();
        sheet = wb.createSheet(sheetName);
        headerOrder = settings.getMappings().stream().map(SourceDestinationMapping::getDestinationColumn).collect(Collectors.toList());
        Row h = sheet.createRow(currentRow++);
        for (int i=0;i<headerOrder.size();i++) h.createCell(i).setCellValue(headerOrder.get(i));
    }

    @Override
    public void writeBatch(List<Map<String, Object>> rows) {
        for (Map<String,Object> r : rows) {
            Row row = sheet.createRow(currentRow++);
            for (int i=0;i<headerOrder.size();i++) {
                Object v = r.getOrDefault(headerOrder.get(i), "");
                row.createCell(i).setCellValue(v==null ? "" : String.valueOf(v));
            }
        }
    }

    @Override public void commit() throws Exception {
        try (FileOutputStream fos = new FileOutputStream(isPart ? partPath : fs.getFilePath())) { wb.write(fos); }
    }
    @Override public void rollback() { }
    @Override public void close() { try { if (wb!=null) wb.close(); } catch(Exception e){} }

    @Override
    public DataWriter createPartWriter(int partIndex) {
        String base = fs.getFilePath();
        String part = base.replaceFirst("\.(xlsx?)$", "_part" + partIndex + ".$1");
        return new ExcelFileWriter(settings, true, part);
    }

    @Override
    public void mergeParts(int parts) throws Exception {
        String base = fs.getFilePath();
        org.apache.poi.xssf.usermodel.XSSFWorkbook merged = new org.apache.poi.xssf.usermodel.XSSFWorkbook();
        Sheet out = merged.createSheet((fs.getExcelSheetName()==null||fs.getExcelSheetName().isEmpty()) ? "Sheet1" : fs.getExcelSheetName());
        int rIdx = 0;
        Row h = out.createRow(rIdx++);
        for (int i=0;i<headerOrder.size();i++) h.createCell(i).setCellValue(headerOrder.get(i));

        for (int p=0;p<parts;p++) {
            String path = base.replaceFirst("\.(xlsx?)$", "_part" + p + ".$1");
            try (org.apache.poi.xssf.usermodel.XSSFWorkbook partWb = new org.apache.poi.xssf.usermodel.XSSFWorkbook(new FileInputStream(path))) {
                Sheet s = partWb.getSheetAt(0);
                java.util.Iterator<Row> it = s.iterator();
                if (it.hasNext()) it.next(); // skip header
                while (it.hasNext()) {
                    Row pr = it.next();
                    Row nr = out.createRow(rIdx++);
                    for (int i=0;i<headerOrder.size();i++) {
                        Cell pc = pr.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        nr.createCell(i).setCellValue(pc==null ? "" : pc.toString());
                    }
                }
            }
            new java.io.File(path).delete();
        }

        try (FileOutputStream fos = new FileOutputStream(base)) { merged.write(fos); }
        merged.close();
    }
}
