
package com.test.dataflowengine.util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;

public final class SqlUtil {
    private SqlUtil(){}

    public static String quoteTable(String dbType, String schema, String table) {
        switch (safe(dbType)) {
            case "mysql": return bt(schema)+"."+bt(table);
            case "hive":  return bt(schema)+"."+bt(table);
            case "mssql":
            case "sqlserver": return br(schema)+"."+br(table);
            case "oracle":
            default: return dq(schema)+"."+dq(table);
        }
    }
    public static String quoteColumn(String dbType, String col) {
        switch (safe(dbType)) {
            case "mysql":
            case "hive":  return bt(col);
            case "mssql":
            case "sqlserver": return br(col);
            case "oracle":
            default: return dq(col);
        }
    }
    public static String buildSelectAll(String dbType, String schema, String table, List<String> cols) {
        String tgt = quoteTable(dbType, schema, table);
        if (cols == null || cols.isEmpty()) return "SELECT * FROM " + tgt;
        String c = cols.stream().map(x -> quoteColumn(dbType, x)).collect(Collectors.joining(", "));
        return "SELECT " + c + " FROM " + tgt;
    }
    public static String buildInsert(String dbType, String schema, String table, List<String> cols) {
        String tgt = quoteTable(dbType, schema, table);
        String colList = cols.stream().map(x -> quoteColumn(dbType, x)).collect(Collectors.joining(", "));
        String qMarks = String.join(",", Collections.nCopies(cols.size(), "?"));
        return "INSERT INTO " + tgt + " (" + colList + ") VALUES (" + qMarks + ")";
    }
    private static String bt(String s){ return "`"+s+"`"; }
    private static String dq(String s){ return """+s+"""; }
    private static String br(String s){ return "["+s+"]"; }
    private static String safe(String s){ return s==null? "" : s.toLowerCase(); }
}
