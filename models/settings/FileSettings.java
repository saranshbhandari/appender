
package com.test.dataflowengine.models.settings;

public class FileSettings {
    private String filePath;
    private String fileType; // csv, excel, json
    private String fileDelimiter = ",";
    private String excelSheetName;
    private boolean firstRowColumn = true;

    public String getFilePath() { return filePath; }
    public String getFileType() { return fileType; }
    public String getFileDelimiter() { return fileDelimiter; }
    public String getExcelSheetName() { return excelSheetName; }
    public boolean isFirstRowColumn() { return firstRowColumn; }
}
