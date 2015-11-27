package com.googlecode.scheme2ddl;

/**
 * @author A_Molchanov
 * @since Date: 17.11.2015
 * This class used for exporting data tables.
 */
public class TableExportProperty {
    public int maxRowsExport;
    public String where;
    public String orderBy;

    public static final int unlimitedExportData = 0;
    public static final int doesNotExportData = -1;
    public static final int emptyMaxRowsExport = -100;

    public TableExportProperty(int maxRowsExport, String where, String orderBy) {
        this.maxRowsExport = maxRowsExport;
        this.where = where;
        this.orderBy = orderBy;
    }
}

