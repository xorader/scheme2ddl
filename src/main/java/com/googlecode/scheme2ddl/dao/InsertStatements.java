package com.googlecode.scheme2ddl.dao;

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

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;

// Needed since we will be using Oracle's BLOB, part of Oracle's JDBC extended
// classes. Keep in mind that we could have included Java's JDBC interfaces
// java.sql.Blob which Oracle does implement. The oracle.sql.BLOB class
// provided by Oracle does offer better performance and functionality.
import oracle.sql.*;
// Needed for Oracle JDBC Extended Classes
import oracle.jdbc.*;


/**
 * @author A_Molchanov
 * @since Date: 17.11.2015
 */
public class InsertStatements {
    private static final Log log = LogFactory.getLog(InsertStatements.class);

    private static final int maxSqlplusCmdLineLength = 2497;    // 2500 is the maximum of sqlplus CMD line length (and 3 bytes sqlplus uses for eols and something)
    private int currentLineLength;
    private CharsetEncoder encoder;
    private static String charsetNameDefault = "UTF-8";
    static String newline = System.getProperty("line.separator");

    InsertStatements() {
        encoder = Charset.forName(charsetNameDefault).newEncoder();
    }

    InsertStatements(String charsetName) {
        if (charsetName != null && !charsetName.equals("")) {
            encoder = Charset.forName(charsetName).newEncoder();
        } else {
            encoder = Charset.forName(charsetNameDefault).newEncoder();
        }
    }

    private static String getTablePrimaryKeyColumn(final DatabaseMetaData meta, final String schema_name, final String tableName)
        throws SQLException
    {
        ResultSet rs = meta.getPrimaryKeys(null, schema_name, tableName);
        if (rs.next()) {
            //log.info(String.format("== getTablePrimaryKeyColumn %s.%s: %s", schema_name, tableName, rs.getString("COLUMN_NAME")));
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
        return false;
    }

    private static boolean isSortableTypeColumn(final DatabaseMetaData meta, final String schema_name, final String tableName, final String columnName)
        throws SQLException
    {
        ResultSet rs = meta.getColumns(null, schema_name, tableName, columnName);
        while(rs.next()) {
            int column_type = rs.getInt("DATA_TYPE");
            //log.info(String.format("== isSortableTypeColumn %s.%s.%s: Type: '%s'. Data type: '%d'. SQL_DATA_TYPE: '%d'.", schema_name, tableName, columnName, rs.getString("TYPE_NAME"), column_type, rs.getInt("SQL_DATA_TYPE")));
            /*
             * It's a little hack, place. We can not order by not internal types, and types like BLOB, CLOB, ARRAY and OTHERs. This types has a number higher then 1000 (magic number).
             * java.sql.Types can see here:
             *  https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types.ARRAY
             *  https://docs.oracle.com/javase/7/docs/api/java/sql/Types.html
             */
            if (column_type >= 1000) {
                return false;
            }
        }
        return true;
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

            //log.info(String.format("== getTableUniqueIdxColumn %s.%s: %s (%s | %s | matched by: %s)", schema_name, tableName, columnName, indexName, isNonUnique ? "true" : "false", sortingByColumnsRegexpList));
            if (sortingByColumnsRegexpList != null && !sortingByColumnsRegexpList.equals("") && columnName.toLowerCase().matches(sortingByColumnsRegexpList)) {
                return columnName;
            }
            last_result = columnName;
        }

        //if (last_result != null)
        //    log.info(String.format("== getTableUniqueIdxColumn %s.%s: %s", schema_name, tableName, last_result));
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
            //log.info(String.format("== getTableAutoIdentifyColumn %s.%s.%s (match by %s). Type: '%s'. Data type: '%d'.", schema_name, tableName, columnName, sortingByColumnsRegexpList, rs.getString("TYPE_NAME"), rs.getInt("DATA_TYPE")));
            if (sortingByColumnsRegexpList != null && !sortingByColumnsRegexpList.equals("") && columnName.toLowerCase().matches(sortingByColumnsRegexpList)) {
                return columnName;
            }
            last_result = columnName;
        }
        //rs.close();
        //if (last_result != null)
        //    log.info(String.format("== getTableAutoIdentifyColumn %s.%s: %s", schema_name, tableName, last_result));
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
                //log.info(String.format("== getTableNameIdentifyColumn %s.%s: %s", schema_name, tableName, columnName));
                return columnName;
            }
        }
        return null;
    }

    private static boolean isTableContainLobColumn(final DatabaseMetaData meta, final String schema_name, final String tableName)
        throws SQLException
    {
        ResultSet rs = meta.getColumns(null, schema_name, tableName, null);
        while(rs.next()) {
            int columnType = rs.getInt("DATA_TYPE");
            if (columnType == java.sql.Types.CLOB || columnType == java.sql.Types.BLOB)
                return true;
        }
        return false;
    }

    /**
     * Returns count of bytes (length) of symbol.
     *  'a' returns - 1
     *  'ю' returns - 2
     *  '茶' returns - 3
     * P.S. java - sux!
     */
    private int getBytesOfCharacter(final char symbol) {
        try {
            return encoder.encode(CharBuffer.wrap(new char[] { symbol })).limit();
        } catch (CharacterCodingException e) {
            return 1;
        }
    }

    private String formatColumnValue(final String value)
    {
        int currentColumnLength;
        try {
            currentColumnLength = value.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            currentColumnLength = value.length();
        }
        if (this.currentLineLength + currentColumnLength + 1 > maxSqlplusCmdLineLength) {
            this.currentLineLength = currentColumnLength;
            return newline + value;
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
                    eolChrs = "'" + newline + "||CHR(13)||CHR(10)||'";
                    this.currentLineLength=21;
                } else {
                    currentLine = result.substring(startLinePosition, position-1);
                    eolChrs = "'" + newline + "||CHR(10)||'";
                    this.currentLineLength=12;
                }

                convertedLines += currentLine + eolChrs;
                startLinePosition = position;
                continue;
            }
            this.currentLineLength += getBytesOfCharacter(character);
            if (this.currentLineLength >= (maxSqlplusCmdLineLength-1)) {
                if (position == 1) {
                    convertedLines = newline + "'";
                    this.currentLineLength=1;
                } else {
                    if (character == '\'' && result.charAt(position-2) != '\'') {
                        convertedLines += result.substring(startLinePosition, position) + newline;
                        this.currentLineLength=0;
                    } else {
                        convertedLines += result.substring(startLinePosition, position) + "'" + newline + "||'";
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
        result = result.replaceAll("\0", "#");

        return result;
    }

    /*
     *  generate DATA_TABLE/<tableName>.sql file (with CLOB/BLOB files additional)
     *
     *  TODO:
     *        - Require to create new java-jar-util for import CLOB/BLOB-files back to Oracle tables
     *  The usefull links:
     *        - http://www.idevelopment.info/data/Programming/java/jdbc/LOBS/BLOBFileExample.java
     *        - http://www.sql.ru/faq/faq_topic.aspx?fid=469
     *        - http://asktom.oracle.com/pls/asktom/f?p=100:11:::::P11_QUESTION_ID:6379798216275
     *        - http://stackoverflow.com/questions/8348427/how-to-write-update-oracle-blob-in-a-reliable-way
     *        - http://stackoverflow.com/questions/862355/overcomplicated-oracle-jdbc-blob-handling
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

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        boolean isPresentUnknownType = false;
        String primaryKeyColumn = null;     // column name, using for creating LOB-files
        boolean isPrimaryKeyColumnSearched = false;

        DatabaseMetaData conn_meta = conn.getMetaData();
        /*
         *  finding the Primary Key column in this table for CLOB/BLOB data exporting
         */
        if (isTableContainLobColumn(conn_meta, schema_name, tableName)) {
            primaryKeyColumn = getTablePrimaryKeyColumn(conn_meta, schema_name, tableName);
            if (primaryKeyColumn == null) {
                primaryKeyColumn = getTableUniqueIdxColumn(conn_meta, schema_name, tableName, true, sortingByColumnsRegexpList);
            }
            if (primaryKeyColumn == null) {
                /* primary key was not found. CLOB/BLOB columns can not be exported */
                log.info(String.format("   ---> Can not save blob/clob column(s) of the '%s' table, because can't find Primary Key for this table.", fullTableName));
            } else {
                /* Create the 'primary_key' file with primary key column name for this table (for BLOB/CLOB columns identity). */
                String primaryKeyFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                        + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, "_", "primary_key"));
                File filePK = new File(primaryKeyFileName);
                FileUtils.writeStringToFile(filePK, primaryKeyColumn);
                log.info(String.format("Save column name with primary key of the '%s' table for LOB column(s) to file: %s",
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
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query_string);
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        int[] columnTypes = new int[numColumns];
        String[] columnNamesArray = new String[numColumns];

        String columnNames = "";
        for (int i = 0; i < numColumns; i++) {
            final String columnName = rsmd.getColumnName(i + 1);
            columnTypes[i] = rsmd.getColumnType(i + 1);
            if (i != 0) {
                columnNames += ",";
            }
            columnNames += columnName;
            columnNamesArray[i] = columnName;
        }

        File file = new File(absoluteFileName);
        FileUtils.touch(file);  // create new file with directories hierarchy
        log.info(String.format("Export data table %s to file %s", fullTableName.toLowerCase(), file.getAbsolutePath()));

        String limitRowsComment = tableProperty.maxRowsExport != TableExportProperty.unlimitedExportData ? String.format("  [limited by %d rows]", tableProperty.maxRowsExport) : "";

        PrintWriter p = new PrintWriter(new FileWriter(file));
        p.println("-- INSERTING into " + fullTableName + " (" + columnNames + ")");
        p.println("-- taked by: " + query_string + limitRowsComment);
        p.println("set sqlt on");  // Sets the character used to end and execute SQL commands to ";"
        p.println("set sqlblanklines on"); // Controls whether SQL*Plus puts blank lines within a SQL command or script. ON interprets blank lines and new lines as part of a SQL command or script. OFF, the default value, does not allow blank lines or new lines in a SQL command or script or script.
        p.println("set define off");   // Sets off the character used to prefix variables to "&"

        while (rs.next()) {
            if (tableProperty.maxRowsExport != TableExportProperty.unlimitedExportData && ++numRows > tableProperty.maxRowsExport) {
                break;
            }

            String resultString = String.format("INSERT INTO %s (%s) values (", fullTableName, columnNames);
            try {
                this.currentLineLength = resultString.getBytes("UTF-8").length;
            } catch (UnsupportedEncodingException e) {
                this.currentLineLength = resultString.length();
            }

            for (int i = 0; i < numColumns; i++) {
                Date d = null;

                if (i != 0) {
                    resultString += ",";
                    this.currentLineLength++;
                }

                switch (columnTypes[i]) {
                    case java.sql.Types.BIGINT:
                    case java.sql.Types.BIT:
                    case java.sql.Types.BOOLEAN:
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.DOUBLE:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.TINYINT:
                        resultString += formatColumnValue(rs.getString(i + 1));
                        break;

                    case java.sql.Types.DATE:
                        d = rs.getDate(i + 1);
                    case java.sql.Types.TIME:
                        if (d == null) d = rs.getTime(i + 1);
                    case java.sql.Types.TIMESTAMP:
                        if (d == null) d = rs.getTimestamp(i + 1);

                        if (d == null) {
                            resultString += formatColumnValue("null");
                        }
                        else {
                            resultString += formatColumnValue("TO_DATE('"
                                      + dateFormat.format(d)
                                      + "', 'YYYY/MM/DD HH24:MI:SS')");
                        }
                        break;
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.NUMERIC:
                        String textColumnValue = rs.getString(i + 1);
                        if (textColumnValue != null) {
                            resultString += formatTextColumnValue(textColumnValue);
                        }
                        else {
                            resultString += formatColumnValue("null");
                        }
                        break;
                    case java.sql.Types.CLOB:
                    case java.sql.Types.BLOB:
                        /* LOB data will exported below to separate file */
                        resultString += formatColumnValue("null");

                        if (primaryKeyColumn == null) {
                            /*
                             * The Primary Key has not been found.
                             * Skip exporting any LOB data for this table.
                             */
                            break;
                        }

                        /*
                         * Import CLOB/BLOB data to "<column_name>.<current Primary Key value>.lob_data" file
                         */
                        String outputBinaryFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                                + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, columnNamesArray[i] + "." + rs.getString(primaryKeyColumn), "lob_data"));
                        File outputBinaryFile = new File(outputBinaryFileName);
                        log.debug(String.format("Export data table LOB '%s' column '%s' with id '%s' to file: %s",
                                    fullTableName.toLowerCase(), columnNamesArray[i], rs.getString(primaryKeyColumn), outputBinaryFile.getAbsolutePath()));

                        if (columnTypes[i] == java.sql.Types.CLOB) {
                            CLOB clob;
                            clob = ((OracleResultSet) rs).getCLOB(i + 1);
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
                            BLOB blob;
                            blob = ((OracleResultSet) rs).getBLOB(i + 1);
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
                    default:
                        String defaultColumnValue;
                        try {
                            defaultColumnValue = rs.getString(i + 1);
                        } catch (Exception e) {
                            if (!isPresentUnknownType) {
                                log.info(String.format("   !!!> Error with take data from the '%s' table and '%s' column with unknown column type: %s", fullTableName, columnNamesArray[i], rsmd.getColumnTypeName(i + 1)));
                                isPresentUnknownType = true;
                            }
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
            }

            if (this.currentLineLength + 2 > maxSqlplusCmdLineLength) {
                resultString += newline;
            }
            p.println(resultString + ");");
        }
        p.close();
    }

}
