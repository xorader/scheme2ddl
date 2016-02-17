package com.googlecode.scheme2ddl.dao;

import com.googlecode.scheme2ddl.DDLFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;

import java.sql.*;

import com.googlecode.scheme2ddl.TableExportProperty;
import com.googlecode.scheme2ddl.FileNameConstructor;
import static com.googlecode.scheme2ddl.FileNameConstructor.map2FileNameStatic;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.UnsupportedEncodingException;

// Needed since we will be using Oracle's BLOB, part of Oracle's JDBC extended
// classes. Keep in mind that we could have included Java's JDBC interfaces
// java.sql.Blob which Oracle does implement. The oracle.sql.BLOB class
// provided by Oracle does offer better performance and functionality.
import oracle.sql.*;
// Needed for Oracle JDBC Extended Classes
import oracle.jdbc.*;

// for this import need the 'xdb6.jar' and 'xmlparserv2.jar' libraries
import oracle.xdb.XMLType;

/**
 * @author A_Molchanov
 * @since Date: 17.11.2015
 */
public class InsertStatements {
    private static final Log log = LogFactory.getLog(InsertStatements.class);

    private int currentLineLength;
    private DDLFormatter ddlFormatter = null;
    private boolean isPresentUnknownType = false;

    InsertStatements() {
        ddlFormatter = new DDLFormatter();
        isPresentUnknownType = false;
    }

    InsertStatements(String charsetName) {
        ddlFormatter = new DDLFormatter(charsetName);
        isPresentUnknownType = false;
    }

    private static String getTablePrimaryKeyColumn(final DatabaseMetaData meta, final String schema_name, final String tableName)
        throws SQLException
    {
        ResultSet rs = meta.getPrimaryKeys(null, schema_name, tableName);
        if (rs.next()) {
            return rs.getString("COLUMN_NAME");
        }
        return null;
    }

    private static boolean isNotNullColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final String columnName)
        throws SQLException
    {
        ResultSet rs = meta.getColumns(null, schema_name, tableName, columnName);
        while(rs.next()) {
            if (rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls) {
                return true;
            }
        }
        rs.close();
        return false;
    }

    private static boolean isSortableTypeColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final String columnName)
        throws SQLException
    {
        ResultSet rs = meta.getColumns(null, schema_name, tableName, columnName);
        boolean result = false;
        while(rs.next()) {
            int column_type = rs.getInt("DATA_TYPE");
            /*
             * It's a little hack, place. We can not order by not internal types, and types like BLOB, CLOB, ARRAY and OTHERs. This types has a number higher then 1000 (magic number).
             * java.sql.Types can see here:
             *  https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types.ARRAY
             *  https://docs.oracle.com/javase/7/docs/api/java/sql/Types.html
             */
            if (column_type >= 1000) {
                return false;
            }
            result = true;
        }
        rs.close();
        return result;
    }

    private static String getTableUniqueIdxColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final boolean onlyNotNull, final String sortingByColumnsRegexpList)
        throws SQLException
    {
        ResultSet rs = meta.getIndexInfo(null, schema_name, tableName, true, true);
        String last_result = null;
        while(rs.next()) {
            final String indexName = rs.getString("INDEX_NAME");
            final String columnName = rs.getString("COLUMN_NAME");
            boolean isNonUnique = rs.getBoolean("NON_UNIQUE");
            if (indexName == null || columnName == null
              || (onlyNotNull && !isNotNullColumn(meta, schema_name, tableName, columnName))
              || !isSortableTypeColumn(meta, schema_name, tableName, columnName)) {
                continue;
            }

            if (sortingByColumnsRegexpList != null && !sortingByColumnsRegexpList.equals("") && columnName.toLowerCase().matches(sortingByColumnsRegexpList)) {
                return columnName;
            }
            last_result = columnName;
        }
        rs.close();
        return last_result;
    }

    private static String getTableAutoIdentifyColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final String sortingByColumnsRegexpList)
        throws SQLException
    {
        ResultSet rs = meta.getBestRowIdentifier(null, schema_name, tableName, 2, true);
        ResultSetMetaData rs_metadata = rs.getMetaData();
        String last_result = null;
        // Display the result set data.
        int cols = rs_metadata.getColumnCount();
        while(rs.next()) {
            final String columnName = rs.getString("COLUMN_NAME");
            if (!isSortableTypeColumn(meta, schema_name, tableName, columnName)) {
                continue;
            }
            if (sortingByColumnsRegexpList != null && !sortingByColumnsRegexpList.equals("") && columnName.toLowerCase().matches(sortingByColumnsRegexpList)) {
                return columnName;
            }
            last_result = columnName;
        }
        rs.close();
        return last_result;
    }

    private static String getTableNameIdentifyColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final String sortingByColumnsRegexpList)
        throws SQLException
    {
        if (sortingByColumnsRegexpList == null || sortingByColumnsRegexpList.equals(""))
            return null;

        ResultSet rs = meta.getColumns(null, schema_name, tableName, null);
        while(rs.next()) {
            final String columnName = rs.getString("COLUMN_NAME");
            if (!isSortableTypeColumn(meta, schema_name, tableName, columnName)) {
                continue;
            }
            if (columnName.toLowerCase().matches(sortingByColumnsRegexpList)) {
                return columnName;
            }
        }
        rs.close();
        return null;
    }

    private static boolean isTableContainLobOrXmlColumn(final DatabaseMetaData meta, final String schema_name, final String tableName)
        throws SQLException
    {
        ResultSet rs = meta.getColumns(null, schema_name, tableName, null);
        while(rs.next()) {
            int columnType = rs.getInt("DATA_TYPE");
            if (columnType == java.sql.Types.CLOB || columnType == java.sql.Types.BLOB || columnType == oracle.xdb.XMLType._SQL_TYPECODE || columnType == java.sql.Types.SQLXML)
                return true;
        }
        rs.close();
        return false;
    }

    private String formatColumnValue(final String value)
    {
        final int currentColumnLength = ddlFormatter.getStringBytes(value);
        if (this.currentLineLength + currentColumnLength + 1 > ddlFormatter.maxSqlplusCmdLineLength) {
            this.currentLineLength = currentColumnLength;
            return ddlFormatter.newline + value;
        }
        this.currentLineLength += currentColumnLength;
        return value;
    }

    /**
     * Formating INSERT text value strings:
     *  - Split lines (by "'" + newline + "||'") if them is greater than maxSqlplusCmdLineLength
     *  - Replace the ' to '' symbols
     *  - quote whole text and every lines around by '
     */
    private String formatTextColumnValue(final String value)
    {
        // replace in string: ' to '' and quote text around by '
        String result = "'" + value.replaceAll("'", "''") + "'";

        int position = 0;
        int startLinePosition = 0;
        String convertedLines = "";
        for (char character: result.toCharArray()) {
            position++;
            if (character == '\n') {
                final String currentLine;
                final String eolChrs;
                if (position > 1 && result.charAt(position-2) == '\r') {    // check Windows eol: \r\n
                    currentLine = result.substring(startLinePosition, position-2);
                    eolChrs = "'" + ddlFormatter.newline + "||CHR(13)||CHR(10)||'";
                    this.currentLineLength=21;
                } else {
                    currentLine = result.substring(startLinePosition, position-1);
                    eolChrs = "'" + ddlFormatter.newline + "||CHR(10)||'";
                    this.currentLineLength=12;
                }

                convertedLines += currentLine + eolChrs;
                startLinePosition = position;
                continue;
            }
            this.currentLineLength += ddlFormatter.getBytesOfCharacter(character);
            if (this.currentLineLength >= (ddlFormatter.maxSqlplusCmdLineLength-1)) {
                if (position == 1) {
                    convertedLines = ddlFormatter.newline + "'";
                    this.currentLineLength=1;
                } else {
                    if (character == '\'' && result.charAt(position-2) != '\'') {
                        convertedLines += result.substring(startLinePosition, position) + ddlFormatter.newline;
                        this.currentLineLength=0;
                    } else {
                        convertedLines += result.substring(startLinePosition, position) + "'" + ddlFormatter.newline + "||'";
                        this.currentLineLength=3;
                    }
                }
                startLinePosition = position;
            }
        }

        if (!convertedLines.equals("")) {
            result = convertedLines + result.substring(startLinePosition, position);
        }

        // fix data values with bug (sqlplus dont allow '\0' symbol)
        result = result.replaceAll("\0", "");

        return result;
    }

    private String getLastLine(String str) {
        return str.substring(str.lastIndexOf("\n")+1);
    }

    final static private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private String formatDatumToString(final Datum tableCell, final int type, final String typeName, final String columnName, final String primaryKeyValue, final String outputPath, final String preparedTemplateDataLob, final String schema_name, final String tableName, final String fullTableName, final OracleConnection oraConnection, final int depth)
        throws SQLException, IOException
    {
        if (depth > 5) {
            // small protection of infinity recursive calls
            log.info(String.format("   !!!Error: Detect max depth for formatDatumToString() for '%s' table and the '%s' column", tableName, columnName));
            return formatColumnValue("null");
        }
        String resultString = "";
        /*
         * For property get String values by 'tableCell.stringValue()' (and others) requires load the 'orai18n.jar' (into class paths)!
         */

        switch (type) {
            case java.sql.Types.BIGINT:
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                resultString += formatColumnValue(tableCell.stringValue());
                break;

            case java.sql.Types.DATE:
                //log.info("   qqq DATE type for the '"+fullTableName+"' table with column: " + columnName + "["+counterRow+"]. String value: " + tableCell.stringValue() + ". DATE value: " + (tableCell.dateValue() == null ? "NULL!!!" : tableCell.dateValue().toString()) + ". TIME value: " + tableCell.timeValue().toString());

                // Also, we can get dateStr value by the following code:
                //   import java.util.TimeZone;
                //   ...
                //   Date fullDate = new Date(tableCell.dateValue().getTime() + tableCell.timeValue().getTime() + TimeZone.getDefault().getRawOffset());
                //   resultString += formatColumnValue("TO_DATE('" + dateFormat.format(fullDate) + "', 'YYYY/MM/DD HH24:MI:SS')");
                //
                // But you must update timezone data for every Java installation (JDK/JRE) !!!
                // For example without update, the 'TimeZone.getDefault()' function for 'Europe/Moscow' TZ returns equivalent 'UTC+4'
                //      value - TimeZone.getTimeZone("GMT+4")  (currect returning must be - 'GMT+3').
                //
                // Update instruction here: http://www.oracle.com/technetwork/java/javase/tzupdater-readme-136440.html

                // the following code more short. And updating timezone data for Java not require:
                final String dateStr = tableCell.stringValue();
                if (dateStr == null) {
                    resultString += formatColumnValue("null");
                } else {
                    resultString += formatColumnValue("TO_DATE('" + dateStr + "', 'YYYY-MM-DD HH24:MI:SS')");
                }

                break;

            case java.sql.Types.TIME:
                log.info(String.format("   !!!WARNING: Unknown for Oracle TIME type column '%s' for '%s' table.", columnName, tableName));

                Time time = tableCell.timeValue();
                if (time == null) {
                    resultString += formatColumnValue("null");
                }
                else {
                    final String timeStr;
                    try {
                        timeStr = dateFormat.format(new Date(time.getTime()));  // require '+ TimeZone.getDefault().getRawOffset()' ?
                    } catch (ArrayIndexOutOfBoundsException e) {
                        resultString += formatColumnValue("null");
                        log.info("   !!!Error: ArrayIndexOutOfBoundsException (TODO why?): during execute dateFormat.format(new Date(time.getTime())) for " + fullTableName + " column: " + columnName);
                        log.info("   Error message: " + e.getMessage());
                        //throw new Exception("Can't execute dateFormat.format(d) for " + fullTableName + " column: " + columnName);
                        //throw new SQLException(e);
                        break;
                    }
                    resultString += formatColumnValue("TO_DATE('" + timeStr + "', 'YYYY/MM/DD HH24:MI:SS')");
                }
                break;

            case oracle.jdbc.OracleTypes.TIMESTAMP:
                resultString += formatColumnValue("TO_TIMESTAMP('" + tableCell.stringValue() + "', 'YYYY-MM-DD HH24:MI:SS.FF')");
                break;
            case oracle.jdbc.OracleTypes.TIMESTAMPTZ:
            case oracle.jdbc.OracleTypes.TIMESTAMPLTZ:
                resultString += formatColumnValue("TO_TIMESTAMP_TZ('" + tableCell.stringValue(oraConnection) + "', 'YYYY-MM-DD HH24:MI:SS.FF TZR')");
                break;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NUMERIC:
                final String textColumnValue = tableCell.stringValue();
                if (textColumnValue != null) {
                    resultString += formatTextColumnValue(textColumnValue);
                }
                else {
                    resultString += formatColumnValue("null");
                }
                break;
            case oracle.xdb.XMLType._SQL_TYPECODE:  // 2007
            case java.sql.Types.SQLXML: // 2009
            //case OracleTypes.OPAQUE: // 2007 (we know it is an XMLType). Duplicate case label.
                resultString += formatColumnValue("null");
                if (primaryKeyValue == null) {
                    // The Primary Key has not been found.
                    // Skip exporting any XMLTYPE data for this table.
                    break;
                }
                // the same result like the following line: XMLType xmlData = XMLType.createXML((OPAQUE)tableCell);
                XMLType xmlData = (XMLType) ((OPAQUE)tableCell).toJdbc();   // Directly Returning XMLType Data
                if (xmlData == null) {
                    break;
                }
                String xmlString = xmlData.getStringVal();
                if (xmlString == null) {
                    break;
                }

                final String outputXmlFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                        + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, columnName + "." + primaryKeyValue, "xmltype"));
                File  outputXmlFile = new File(outputXmlFileName);
                log.debug(String.format("Export data table XMLTYPE '%s' column '%s' with id '%s' to file: %s",
                            fullTableName.toLowerCase(), columnName, primaryKeyValue, outputXmlFile.getAbsolutePath()));
                BufferedWriter xmlOut = new BufferedWriter(new FileWriter(outputXmlFile));
                xmlOut.write(xmlString);
                xmlOut.close();
                break;

            case java.sql.Types.CLOB:
            case java.sql.Types.BLOB:
                // LOB data will exported below to separate file
                resultString += formatColumnValue("null");

                if (primaryKeyValue == null) {
                     // The Primary Key has not been found.
                     // Skip exporting any LOB data for this table.
                    break;
                }

                // Import CLOB/BLOB data to "<column_name>.<current Primary Key value>.lob_data" file
                final String outputBinaryFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                        + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, columnName + "." + primaryKeyValue, type == java.sql.Types.CLOB ? "clob_data" : "blob_data"));
                File outputBinaryFile = new File(outputBinaryFileName);
                log.debug(String.format("Export data table LOB '%s' column '%s' with id '%s' to file: %s",
                            fullTableName.toLowerCase(), columnName, primaryKeyValue, outputBinaryFile.getAbsolutePath()));

                if (type == java.sql.Types.CLOB) {
                    CLOB clob = (CLOB) tableCell;
                    if (clob != null) {
                        FileUtils.touch(outputBinaryFile);  // create new file with directories hierarchy
                        Writer fileWriter = new BufferedWriter(new FileWriter(outputBinaryFile));

                        // can't use clob.getAsciiStream(), because it will broke UTF-8 characters in CLOB
                        Reader clobReader = clob.getCharacterStream();

                        try {
                            int chunkSize = clob.getChunkSize();
                            int bytesRead;
                            char[] buf = new char[chunkSize];
                            while ((bytesRead = clobReader.read(buf, 0, chunkSize)) != -1) {
                                fileWriter.write(buf, 0, bytesRead);
                            }
                        } finally {
                            clobReader.close();
                            fileWriter.close();
                        }
                    }
                } else {
                    BLOB blob = (BLOB) tableCell;
                    if (blob != null) {
                        FileUtils.touch(outputBinaryFile);  // create new file with directories hierarchy
                        FileOutputStream outputFileOutputStream = new FileOutputStream(outputBinaryFile);
                        InputStream blobInputStream = blob.getBinaryStream();

                        try {
                            int chunkSize = blob.getChunkSize();
                            int bytesRead;
                            byte[] buf = new byte[chunkSize];
                            while ((bytesRead = blobInputStream.read(buf)) != -1) {
                                outputFileOutputStream.write(buf, 0, bytesRead);
                            }
                        } finally {
                            blobInputStream.close();
                            outputFileOutputStream.close();
                        }
                    }
                }
                break;

                // type for nested tables: http://www.orafaq.com/wiki/NESTED_TABLE
            case java.sql.Types.ARRAY:
                ARRAY cellDataArray = (ARRAY) tableCell;
                if (cellDataArray == null) {
                    resultString += formatColumnValue("null");
                    break;
                }

                Datum[] arrayValues = cellDataArray.getOracleArray();
                resultString += formatColumnValue(cellDataArray.getSQLTypeName() + "(");
                for(int row = 0; row < arrayValues.length; row++ ) {
                    resultString += formatDatumToString(arrayValues[row], cellDataArray.getBaseType(), cellDataArray.getBaseTypeName(), columnName + ".LIST_"+primaryKeyValue, Integer.toString(row), outputPath, preparedTemplateDataLob, schema_name, tableName, fullTableName, oraConnection, depth+1);

                    if (row < arrayValues.length-1) {
                        if (this.currentLineLength + 1 > ddlFormatter.maxSqlplusCmdLineLength) {
                            resultString += ddlFormatter.newline;
                            this.currentLineLength = 0;
                        }
                        resultString += ",";
                        this.currentLineLength++;
                    }
                }
                resultString += formatColumnValue(")");
                break;

                // type for nested tables: http://www.orafaq.com/wiki/NESTED_TABLE
            case java.sql.Types.STRUCT:
                STRUCT cellStruct = (STRUCT) tableCell;
                StructDescriptor structDesc = cellStruct.getDescriptor();
                ResultSetMetaData structDescMeta = structDesc.getMetaData();
                Datum[] structValues = cellStruct.getOracleAttributes();
                resultString += formatColumnValue(typeName + "(");
                for(int structCol=0; structCol < structValues.length; structCol++) {
                    resultString += formatDatumToString(structValues[structCol], structDescMeta.getColumnType(structCol+1), structDescMeta.getColumnTypeName(structCol+1), columnName + ".STRUCT_"+primaryKeyValue, structDescMeta.getColumnName(structCol+1), outputPath, preparedTemplateDataLob, schema_name, tableName, fullTableName, oraConnection, depth+1);

                    if (structCol < structValues.length-1) {
                        if (this.currentLineLength + 1 > ddlFormatter.maxSqlplusCmdLineLength) {
                            resultString += ddlFormatter.newline;
                            this.currentLineLength = 0;
                        }
                        resultString += ",";
                        this.currentLineLength++;
                    }
                }
                resultString += formatColumnValue(")");
                break;

            case java.sql.Types.VARBINARY:      // Oracle RAW type
                final String hexTextColumnValue = tableCell.stringValue();
                if (hexTextColumnValue != null) {
                    resultString += formatTextColumnValue(hexTextColumnValue);
                }
                else {
                    resultString += formatColumnValue("null");
                }
                break;

            default:
                String defaultColumnValue;
                if (!isPresentUnknownType) {
                    log.info(String.format("   !!!Warning: Try to take data from the '%s' table from the '%s' column with unknown column type: '%s' [%d]", fullTableName, columnName, typeName, type));
                    isPresentUnknownType = true;
                }

                try {
                    defaultColumnValue = tableCell.stringValue();
                    if (defaultColumnValue == null) {
                        throw new Exception("Take string from unknown column type");
                    }
                } catch (Exception e) {
                    defaultColumnValue = null;
                }

                if (defaultColumnValue != null) {
                    resultString += formatTextColumnValue(defaultColumnValue);
                }
                else {
                    resultString += formatColumnValue("null");
                }
                break;
        }

        if (resultString.equals("")) {
            return formatColumnValue("null");
        }
        return resultString;
    }

    /*
     *  generate DATA_TABLE/<tableName>.sql file (with CLOB/BLOB/XMLTYPE files additional)
     *
     *  The usefull links:
     *        - http://www.idevelopment.info/data/Programming/java/jdbc/LOBS/BLOBFileExample.java
     *        - http://www.sql.ru/faq/faq_topic.aspx?fid=469
     *        - http://asktom.oracle.com/pls/asktom/f?p=100:11:::::P11_QUESTION_ID:6379798216275
     *        - http://stackoverflow.com/questions/8348427/how-to-write-update-oracle-blob-in-a-reliable-way
     *        - http://stackoverflow.com/questions/862355/overcomplicated-oracle-jdbc-blob-handling
     *        - http://docs.oracle.com/cd/B28359_01/appdev.111/b28369/xdb11jav.htm (Java DOM API for XMLType)
     */
    public void generateInsertStatements(Connection conn, String schema_name, String tableName, final TableExportProperty tableProperty,
            final String preparedTemplate, final String preparedTemplateDataLob, final String outputPath,
            final boolean isSortExportedDataTable, final String sortingByColumnsRegexpList)
        throws SQLException, DataAccessException, IOException
    {
        final String fullTableName;
        final String absoluteFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplate, null, "sql"));

        if (schema_name == null) {
            fullTableName = "\"" + tableName + "\"";
        } else {
            fullTableName = "\"" + schema_name + "\".\"" + tableName + "\"";
        }

        String primaryKeyColumn = null;     // column name, using for creating LOB-files or XMLTYPE-files
        boolean isPrimaryKeyColumnSearched = false;
        boolean tableContainLobXmlColumn = false;

        DatabaseMetaData conn_meta = conn.getMetaData();
        OracleConnection oraConnection = (OracleConnection) conn_meta.getConnection();

        // finding the Primary Key column in this table for CLOB/BLOB/XMLTYPE data exporting
        if (isTableContainLobOrXmlColumn(conn_meta, schema_name, tableName)) {
            tableContainLobXmlColumn = true;
            primaryKeyColumn = getTablePrimaryKeyColumn(conn_meta, schema_name, tableName);
            if (primaryKeyColumn == null) {
                primaryKeyColumn = getTableUniqueIdxColumn(conn_meta, schema_name, tableName, true, sortingByColumnsRegexpList);
            }
            if (primaryKeyColumn == null) {
                // primary key was not found. CLOB/BLOB/XMLTYPE columns can not be exported
                log.info(String.format("   !!!Warning: Can not save blob/clob/xml column(s) of the '%s' table, because can't find Primary Key for this table.", fullTableName));
            } else {
                // Create the 'primary_key' file with primary key column name for this table (for BLOB/CLOB/XMLTYPE columns identity).
                String primaryKeyFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                        + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, "_", "primary_key"));
                File filePK = new File(primaryKeyFileName);
                FileUtils.writeStringToFile(filePK, primaryKeyColumn);
                log.info(String.format("Save column name with primary key of the '%s' table for LOB/XMLTYPE column(s) to file: %s",
                            fullTableName.toLowerCase(), filePK.getAbsolutePath()));
            }
            isPrimaryKeyColumnSearched = true;
        }

        int numRows = 0;
        String query_string = "SELECT * FROM " + fullTableName;
        if (tableProperty.where != null) {
            query_string += " WHERE " + tableProperty.where;
        }
        if (isSortExportedDataTable) {
            String bestRowIdentifier = tableProperty.orderBy;    // column name, using for sorting

            if (bestRowIdentifier == null) {
                if (!isPrimaryKeyColumnSearched) {
                    bestRowIdentifier = getTablePrimaryKeyColumn(conn_meta, schema_name, tableName);
                } else {
                    bestRowIdentifier = primaryKeyColumn;
                }
            }
            if (bestRowIdentifier == null)
                bestRowIdentifier = getTableUniqueIdxColumn(conn_meta, schema_name, tableName, false, sortingByColumnsRegexpList);
            if (bestRowIdentifier == null)
                bestRowIdentifier = getTableAutoIdentifyColumn(conn_meta, schema_name, tableName, sortingByColumnsRegexpList);
            if (bestRowIdentifier == null)
                bestRowIdentifier = getTableNameIdentifyColumn(conn_meta, schema_name, tableName, sortingByColumnsRegexpList);

            if (bestRowIdentifier != null)
                query_string += " ORDER BY " + bestRowIdentifier;
        }

        File file = new File(absoluteFileName);
        FileUtils.touch(file);  // create new file with directories hierarchy
        log.info(String.format("Export data table %s to file %s", fullTableName.toLowerCase(), file.getAbsolutePath()));

        Statement stmt = conn.createStatement();
        OracleResultSet rs;
        try {
            rs = (OracleResultSet) stmt.executeQuery(query_string);
        } catch (SQLException e) {
            log.info("   !!!Error during SQL execute. Check xml-config for errors in the 'includesDataTables -> orderBy' properties for the '"+fullTableName+"' table. Can not execute:  " + query_string);
            log.info("   Error message: " + e.getMessage());
            throw new SQLException(e);
        }
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        int[] columnTypes = new int[numColumns];
        String[] columnNamesArray = new String[numColumns];
        String[] columnTypeNamesArray = new String[numColumns];

        String columnNames = "";
        for (int i = 0; i < numColumns; i++) {
            final String columnName = rsmd.getColumnName(i + 1);
            final String columnTypeName = rsmd.getColumnTypeName(i + 1);
            if (columnTypeName.equalsIgnoreCase("DATE")) {
                // For the 'DATA' type (in this place) Oracle jdbc return the 'java.sql.Types.TIMESTAMP' type. We fix it Oracle bug. :(
                columnTypes[i] = java.sql.Types.DATE;
            } else {
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }
            if (i != 0) {
                columnNames += ",";
            }
            columnNames += columnName;
            columnNamesArray[i] = columnName;
            columnTypeNamesArray[i] = columnTypeName;
        }

        String limitRowsComment = tableProperty.maxRowsExport != TableExportProperty.unlimitedExportData ? String.format("  [limited by %d rows]", tableProperty.maxRowsExport) : "";

        PrintWriter p = new PrintWriter(new FileWriter(file));
        p.println(ddlFormatter.splitBigLinesByNewline("-- INSERTING into " + fullTableName + " (" + columnNames + ")"));
        p.println("-- taked by: " + query_string + limitRowsComment);
        p.println("set sqlt on");  // Sets the character used to end and execute SQL commands to ";"
        p.println("set sqlblanklines on"); // Controls whether SQL*Plus puts blank lines within a SQL command or script. ON interprets blank lines and new lines as part of a SQL command or script. OFF, the default value, does not allow blank lines or new lines in a SQL command or script or script.
        p.println("set define off");   // Sets off the character used to prefix variables to "&"

        /*
         * Main loop for INSERT's generating
         */
        while (rs.next()) {
            if (tableProperty.maxRowsExport != TableExportProperty.unlimitedExportData && ++numRows > tableProperty.maxRowsExport) {
                break;
            }

            final String primaryKeyValue;
            if (tableContainLobXmlColumn && primaryKeyColumn != null) {
                primaryKeyValue = rs.getString(primaryKeyColumn);
            } else {
                primaryKeyValue = null;
            }

            String resultString = ddlFormatter.splitBigLinesByNewline(String.format("INSERT INTO %s (%s) values (", fullTableName, columnNames));
            this.currentLineLength = ddlFormatter.getStringBytes(getLastLine(resultString));

            for (int i = 0; i < numColumns; i++) {
                oracle.sql.Datum tableCell = rs.getOracleObject(i + 1);

                if (i != 0) {
                    resultString += ",";
                    this.currentLineLength++;
                }

                if (tableCell == null) {
                    resultString += formatColumnValue("null");
                    continue;
                }

                resultString += formatDatumToString(tableCell, columnTypes[i], columnTypeNamesArray[i], columnNamesArray[i], primaryKeyValue, outputPath, preparedTemplateDataLob, schema_name, tableName, fullTableName, oraConnection, 1);
            }

            if (this.currentLineLength + 2 > ddlFormatter.maxSqlplusCmdLineLength) {
                resultString += ddlFormatter.newline;
            }
            p.println(resultString + ");");
        }
        p.close();
    }

}
