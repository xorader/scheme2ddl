package com.googlecode.scheme2ddl.dao;

import com.googlecode.scheme2ddl.domain.UserObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.googlecode.scheme2ddl.TypeNamesUtil.map2TypeForConfig;
import com.googlecode.scheme2ddl.FileNameConstructor;
import static com.googlecode.scheme2ddl.FileNameConstructor.map2FileNameStatic;
import com.googlecode.scheme2ddl.UserObjectWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

// Needed since we will be using Oracle's BLOB, part of Oracle's JDBC extended
// classes. Keep in mind that we could have included Java's JDBC interfaces
// java.sql.Blob which Oracle does implement. The oracle.sql.BLOB class
// provided by Oracle does offer better performance and functionality.
import oracle.sql.*;
// Needed for Oracle JDBC Extended Classes
import oracle.jdbc.*;

/*
 *  The usefull documentation: http://docs.oracle.com/cd/B19306_01/appdev.102/b14258/d_metada.htm
 */

/**
 * @author A_Reshetnikov
 * @since Date: 17.10.2012
 */
public class UserObjectDaoImpl extends JdbcDaoSupport implements UserObjectDao {

    private static final Log log = LogFactory.getLog(UserObjectDaoImpl.class);
    private Map<String, Boolean> transformParams;
    @Value("#{jobParameters['schemaName']}")
    private String schemaName;
    @Value("#{jobParameters['launchedByDBA']}")
    private boolean isLaunchedByDBA = false;
    @Value("#{jobParameters['outputPath']}")
    private String outputPath;

    public List<UserObject> findListForProccessing() {
        String sql;
        if (isLaunchedByDBA)
            sql = "select t.object_name, t.object_type " +
                    "  from dba_objects t " +
                    " where t.generated = 'N' " +
                    "   and t.owner = '" + schemaName + "' " +
                    "   and not exists (select 1 " +
                    "          from user_nested_tables unt" +
                    "         where t.object_name = unt.table_name)" +
                    " UNION ALL " +
                    " select rname as object_name, 'REFRESH_GROUP' as object_type " +
                    " from dba_refresh a " +
                    " where a.rowner = '" + schemaName + "' ";
        else
            sql = "select t.object_name, t.object_type " +
                    "  from user_objects t " +
                    " where t.generated = 'N' " +
                    "   and not exists (select 1 " +
                    "          from user_nested_tables unt" +
                    "         where t.object_name = unt.table_name)" +
                    " UNION ALL " +
                    " select rname as object_name, 'REFRESH GROUP' as object_type " +
                    " from user_refresh ";
        return getJdbcTemplate().query(sql, new UserObjectRowMapper());
    }

    public List<UserObject> findPublicDbLinks() {
        List<UserObject> list = new ArrayList<UserObject>();
        try {
            list = getJdbcTemplate().query(
                    "select db_link as object_name, 'PUBLIC DATABASE LINK' as object_type " +
                            "from DBA_DB_LINKS " +
                            "where owner='PUBLIC'",
                    new UserObjectRowMapper());
        } catch (BadSqlGrammarException sqlGrammarException) {
            if (sqlGrammarException.getSQLException().getErrorCode() == 942) {
                String userName = null;
                try {
                    userName = getDataSource().getConnection().getMetaData().getUserName();
                } catch (SQLException e) {
                }
                log.warn("WARNING: processing of 'PUBLIC DATABASE LINK' will be skipped because " + userName + " no access to view it" +
                        "\n Possible decisions:\n\n" +
                        " 1) Exclude processPublicDbLinks option in advanced config to disable this warning\n    " +
                        " <bean id=\"reader\" ...>\n" +
                        "        <property name=\"processPublicDbLinks\" value=\"false\"/>\n" +
                        "        ...\n" +
                        "    </bean>\n" +
                        "\n" +
                        " 2) Or try give access to user " + userName + " with sql command\n " +
                        " GRANT SELECT_CATALOG_ROLE TO " + userName + "; \n\n");
            }
            return list;
        }

        for (UserObject userObject : list) {
            userObject.setSchema("PUBLIC");
        }
        return list;
    }

    public List<UserObject> findDmbsJobs() {
        String tableName = isLaunchedByDBA ? "dba_jobs" : "user_jobs";
        String whereClause = isLaunchedByDBA ? "schema_user = '" + schemaName + "'" : "schema_user != 'SYSMAN'";
        String sql = "select job || '' as object_name, 'DBMS JOB' as object_type " +
                "from  " + tableName + " where " + whereClause;
        return getJdbcTemplate().query(sql, new UserObjectRowMapper());
    }

    public String findPrimaryDDL(final String type, final String name) {
        if (isLaunchedByDBA)
            return executeDbmsMetadataGetDdl("select dbms_metadata.get_ddl(?, ?, ?) from dual", type, name, schemaName);
        else
            return executeDbmsMetadataGetDdl("select dbms_metadata.get_ddl(?, ?) from dual", type, name, null);
    }

    public List<UserObject> addUser() {
        UserObject userObject = new UserObject();
        List<UserObject> list = new ArrayList<UserObject>();

        if (!isLaunchedByDBA) {
            return list;
        }

        userObject.setName(schemaName);
        userObject.setType("USER");
        userObject.setSchema(schemaName);

        list.add(userObject);
        return list;
    }

    public String generateUserDDL(final String name) {
        return (String) getJdbcTemplate().execute(new ConnectionCallback() {
            public String doInConnection(Connection connection) throws SQLException, DataAccessException {
                String result = "-- User Creation\n";
                applyTransformParameters(connection);

                /* Generate -- User Creation */
                PreparedStatement ps = connection.prepareStatement("SELECT DBMS_METADATA.GET_DDL('USER', ?) FROM dual");
                ps.setString(1, name);
                ResultSet rs;

                try {
                    rs = ps.executeQuery();
                } catch (SQLException e) {
                    log.trace(String.format("Error during SELECT DBMS_METADATA.GET_DDL('USER', '%s') from dual", name));
                    return "";
                }
                try {
                    while (rs.next()) {
                        result += rs.getString(1).trim();
                    }
                } finally {
                    rs.close();
                    ps.close();
                }

                /* Generate -- User Role */
                result += "\n\n-- User Role\n";
                ps = connection.prepareStatement("SELECT 'GRANT \"'||u.name||'\" TO \"'||upper(?)||'\"'|| CASE WHEN min(sa.option$) = 1 THEN ' WITH ADMIN OPTION;' ELSE ';' END ddl_string FROM sys.sysauth$ sa, sys.user$ u WHERE sa.grantee# = (select u.user# FROM sys.user$ u WHERE u.name = UPPER(?)) AND u.user# = sa.privilege# AND sa.grantee# != 1 GROUP BY u.name");
                ps.setString(1, name);
                ps.setString(2, name);

                try {
                    rs = ps.executeQuery();
                } catch (SQLException e) {
                    log.trace(String.format("Error during create User Role for: %s", name));
                    return result;
                }

                try {
                    while (rs.next()) {
                        result += rs.getString(1).trim() + "\n  ";
                    }
                } finally {
                    rs.close();
                    ps.close();
                }

                /* Generate -- User System Privileges */
                result += "\n\n-- User System Privileges\n";
                ps = connection.prepareStatement("SELECT CASE WHEN COUNT(1) != 0 THEN DBMS_METADATA.GET_GRANTED_DDL('SYSTEM_GRANT', ?) ELSE NULL END ddl_string FROM sys.sysauth$ sa WHERE sa.grantee# = (SELECT u.user# FROM sys.user$ u WHERE u.name = UPPER(?))");
                ps.setString(1, name);
                ps.setString(2, name);

                try {
                    rs = ps.executeQuery();
                } catch (SQLException e) {
                    log.trace(String.format("Error during create User System Privileges for: %s", name));
                    return result;
                }

                try {
                    while (rs.next()) {
                        result += rs.getString(1).trim();
                    }
                } finally {
                    rs.close();
                    ps.close();
                }

                return result;
            }
        });
     }

    private String executeDbmsMetadataGetDdl(final String query, final String type, final String name, final String schema) {
        return (String) getJdbcTemplate().execute(new ConnectionCallback() {
            public String doInConnection(Connection connection) throws SQLException, DataAccessException {
                applyTransformParameters(connection);
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setString(1, type);
                ps.setString(2, name);
                if (schema != null) {
                    ps.setString(3, schema);
                }
                ResultSet rs = null;
                try {
                    rs = ps.executeQuery();
                } catch (SQLException e) {
                    log.error(String.format("Error during select dbms_metadata.get_ddl('%s', '%s') from dual\n" +
                            "Try to exclude type '%s' in advanced config excludes section\n", type, name, map2TypeForConfig(type)));
                    log.error(String.format("Sample:\n\n" +
                            " <util:map id=\"excludes\">\n" +
                            "...\n" +
                            "         <entry key=\"%s\">\n" +
                            "            <set>\n" +
                            "                <value>%s</value>\n" +
                            "            </set>\n" +
                            "        </entry>\n" +
                            "...\n" +
                            "</util:map>", map2TypeForConfig(type), name));
                    throw e;
                }
                try {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                } finally {
                    rs.close();
                }
                return null;
            }
        });
    }

    public String findDependentDLLByTypeName(final String type, final String parent_name, final String parent_type) {

        return (String) getJdbcTemplate().execute(new ConnectionCallback() {
            /*
             * Instead of using the "select dbms_metadata.get_dependent_ddl()" query, we use
             * the construcion "DBMS_METADATA.OPEN(); ... DBMS_METADATA.FETCH_CLOB(); DBMS_METADATA.CLOSE(handle);".
             * This is need for "DBMS_METADATA.SET_FILTER(handle,'SYSTEM_GENERATED', false);" for
             * exclude system-generated indexes/triggers. Example of system-generated index can
             * get by creating BLOB/CLOB column for any table.
             *
             * Example and info taked from:
             *  - http://docs.oracle.com/cd/E11882_01/server.112/e22490/metadata_api.htm
             *  - http://docs.oracle.com/cd/E11882_01/appdev.112/e40758/d_metada.htm
             */
            final String query =
            "DECLARE\n" +
            "  result CLOB := '';\n" +
            "  msg CLOB;\n" +
            "  handle NUMBER;\n" +
            "  tr_handle NUMBER;\n" +
            "BEGIN\n" +
            "  handle := DBMS_METADATA.OPEN('" + type + "');" +
            (schemaName != null ? ("  DBMS_METADATA.SET_FILTER(handle,'BASE_OBJECT_SCHEMA', '" + schemaName + "');") : "") +
            "  dbms_metadata.SET_FILTER(handle,'BASE_OBJECT_TYPE', '" + parent_type + "');" +
            "  dbms_metadata.SET_FILTER(handle,'BASE_OBJECT_NAME', '" + parent_name + "');" +
            //  -- Exclude system-generated indexes/triggers
            (type.equalsIgnoreCase("INDEX") || type.equalsIgnoreCase("TRIGGER") ?  "  dbms_metadata.SET_FILTER(handle,'SYSTEM_GENERATED', false);" : "") +
            "  tr_handle := DBMS_METADATA.ADD_TRANSFORM(handle,'DDL');" +
            //  -- inherits session-level parameters
            //  -- (takes parameters from 'SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,...' exhibited before)
            "  DBMS_METADATA.SET_TRANSFORM_PARAM(tr_handle, 'INHERIT', true);\n" +
            //  -- Fetch the objects:
            "  LOOP\n" +
            "    msg := DBMS_METADATA.FETCH_CLOB(handle);" +
            "    EXIT WHEN msg IS NULL;" +
            "    result := result || msg;" +
            "  END LOOP;\n" +
            "  DBMS_METADATA.CLOSE(handle);\n" +
            ":result := result;\n" +
            "END;";

            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                java.sql.Clob clob;
                applyTransformParameters(connection);
                //log.info(String.format("qq %s - %s => %s", parent_name, parent_type, type));
                CallableStatement cs = connection.prepareCall(query);
                cs.registerOutParameter(1, java.sql.Types.CLOB);
                try {
                    cs.execute();
                } catch (SQLException e) {
                    log.trace(String.format("Error during take the '%s' depends of the '%s' object with type '%s'", type, parent_name, parent_type));
                    return "";
                }
                try {
                    clob = cs.getClob(1);
                } finally {
                    cs.close();
                }
                if (clob == null)
                    return "";
                //log.info(String.format("result get dep %s - %s ==> %s: %s", parent_name, parent_type, type, clob.getSubString(1, (int) clob.length())));
                return clob.getSubString(1, (int) clob.length());
            }
        });
    }

    public String findDDLInPublicScheme(String type, String name) {
        return executeDbmsMetadataGetDdl("select dbms_metadata.get_ddl(?, ?, ?) from dual", type, name, "PUBLIC");
    }

    public String findDbmsJobDDL(String name) {
        String sql;
        if (isLaunchedByDBA)
            // The 'dbms_job.user_export' function does not work with sys/dba users (can't find users jobs). :(
            sql = "DECLARE\n" +
                    " callstr VARCHAR2(4096);\n" +
                    "BEGIN\n" +
                    "  dbms_ijob.full_export(" + name + ", callstr);\n" +
                    ":done := callstr; END;";
        else
            sql = "DECLARE\n" +
                    " callstr VARCHAR2(4096);\n" +
                    "BEGIN\n" +
                    "  dbms_job.user_export(" + name + ", callstr);\n" +
                    ":done := callstr; " +
                    "END;";

        return (String) getJdbcTemplate().execute(sql, new CallableStatementCallbackImpl());
    }

    public boolean isConnectionAvailable() {
        try {
            getJdbcTemplate().queryForInt("select 1 from dual");
        } catch (DataAccessException e) {
            return false;
        }
        return true;
    }

    public void applyTransformParameters(Connection connection) throws SQLException {
        String sql = "BEGIN ";
        for (String parameterName : transformParams.keySet()) {
            sql += String.format(
                " dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM,'%s',%s);",
                parameterName, transformParams.get(parameterName));
        }
        sql += " END;";
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareCall(sql);
        ps.execute();
    }

    public void setTransformParams(Map<String, Boolean> transformParams) {
        this.transformParams = transformParams;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setLaunchedByDBA(boolean launchedByDBA) {
        this.isLaunchedByDBA = launchedByDBA;
    }

    private class CallableStatementCallbackImpl implements CallableStatementCallback {
        public Object doInCallableStatement(CallableStatement callableStatement) throws SQLException, DataAccessException {
            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
            callableStatement.executeUpdate();
            return callableStatement.getString(1);
        }
    }

    private class UserObjectRowMapper implements RowMapper {
        public UserObject mapRow(ResultSet rs, int rowNum) throws SQLException {
            UserObject userObject = new UserObject();
            userObject.setName(rs.getString("object_name"));
            userObject.setType(rs.getString("object_type"));
            userObject.setSchema(schemaName == null ? "" : schemaName);
            return userObject;
        }
    }

    public void exportDataTable(UserObject userObject, final int maxRowsExport, final String add_where, final FileNameConstructor fileNameConstructor) {
        final String tableName = userObject.getName();
        final String schema_name = schemaName;
        final String preparedTemplate = fileNameConstructor.getPreparedTemplate();
        final String preparedTemplateDataLob = fileNameConstructor.getPreparedTemplateDataLob();

        String result_execute = (String) getJdbcTemplate().execute(new ConnectionCallback() {
            public String doInConnection(Connection connection) throws SQLException, DataAccessException {
                try {
                    generateInsertStatements(connection, schema_name, tableName, maxRowsExport, add_where, preparedTemplate, preparedTemplateDataLob, outputPath);
                } catch (IOException e) {
                    logger.error("Error with write to data file of '" + tableName + "' table: " + e.getMessage(), e);
                }
                return null;
            }
        });
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
    private static void generateInsertStatements(Connection conn, String schema_name, String tableName, final int maxRowsExport, final String add_where, final String preparedTemplate, final String preparedTemplateDataLob, final String outputPath)
            throws SQLException, DataAccessException, IOException
    {
        final String fullTableName;
        final String absoluteFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplate, null, "sql"));

        if (schema_name == null) {
            fullTableName = tableName;
        } else {
            fullTableName = schema_name + "." + tableName;
        }

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        boolean isPresentUnknownType = false;
        String primaryKeyColumn = null;
        boolean isPrimaryKeyColumnSearched = false;
        int numRows = 0;
        String query_string = "SELECT * FROM " + fullTableName;

        Statement stmt = conn.createStatement();
        if (add_where != null) {
            query_string += " WHERE " + add_where;
        }
        ResultSet rs = stmt.executeQuery(query_string);
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        int[] columnTypes = new int[numColumns];
        String[] columnNamesArray = new String[numColumns];

        String columnNames = "";
        for (int i = 0; i < numColumns; i++) {
            columnTypes[i] = rsmd.getColumnType(i + 1);
            if (i != 0) {
                columnNames += ",";
            }
            columnNames += rsmd.getColumnName(i + 1);
            columnNamesArray[i] = rsmd.getColumnName(i + 1);
        }

        File file = new File(absoluteFileName);
        FileUtils.touch(file);  // create new file with directories hierarchy
        log.info(String.format("Export data table '%s' to file %s", fullTableName.toLowerCase(), file.getAbsolutePath()));

        PrintWriter p = new PrintWriter(new FileWriter(file));
        p.println("REM INSERTING into " + fullTableName);
        p.println("set sqlt off;");
        p.println("set sqlblanklines on;");
        p.println("set define off;");

        Date d = null;
        while (rs.next()) {
            if (maxRowsExport > 0 && ++numRows > maxRowsExport) {
                break;
            }
            String columnValues = "";
            for (int i = 0; i < numColumns; i++) {
                if (i != 0) {
                    columnValues += ",";
                }

                switch (columnTypes[i]) {
                    case Types.BIGINT:
                    case Types.BIT:
                    case Types.BOOLEAN:
                    case Types.DECIMAL:
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        String v = rs.getString(i + 1);
                        columnValues += v;
                        break;

                    case Types.DATE:
                        d = rs.getDate(i + 1);
                    case Types.TIME:
                        if (d == null) d = rs.getTime(i + 1);
                    case Types.TIMESTAMP:
                        if (d == null) d = rs.getTimestamp(i + 1);

                        if (d == null) {
                            columnValues += "null";
                        }
                        else {
                            columnValues += "TO_DATE('"
                                      + dateFormat.format(d)
                                      + "', 'YYYY/MM/DD HH24:MI:SS')";
                        }
                        break;
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.NUMERIC:
                        v = rs.getString(i + 1);
                        if (v != null) {
                            columnValues += "'" + v.replaceAll("'", "''") + "'";
                        }
                        else {
                            columnValues += "null";
                        }
                        break;
                    case Types.CLOB:
                    case Types.BLOB:
                        /* LOB data will exported below to separate file */
                        columnValues += "null";

                        /*
                         *  finding the Primary Key in this table
                         */
                        if (primaryKeyColumn == null) {
                            if (!isPrimaryKeyColumnSearched) {
                                DatabaseMetaData meta = conn.getMetaData();
                                ResultSet rs_meta = meta.getPrimaryKeys(null, schema_name, tableName);
                                if (rs_meta.next()) {
                                    primaryKeyColumn = rs_meta.getString("COLUMN_NAME");
                                }
                                isPrimaryKeyColumnSearched = true;
                                if (primaryKeyColumn == null) {
                                    /* primary key was not found. CLOB/BLOB columns can not be exported */
                                    log.info(String.format("   ---> Can not save the '%s' blob column of the '%s' table, because can't find Primary Key for this table!!!", columnNamesArray[i], fullTableName));
                                    break;
                                } else {
                                    /* Create <column name>.primary_key file with primary key column name for this LOB. */
                                    String primaryKeyFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                                            + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, columnNamesArray[i], "primary_key"));
                                    File filePK = new File(primaryKeyFileName);
                                    FileUtils.writeStringToFile(filePK, primaryKeyColumn);
                                    log.info(String.format("Export data table LOB '%s' column primary key '%s' to file: %s",
                                                fullTableName.toLowerCase(), columnNamesArray[i], filePK.getAbsolutePath()));

                                }
                            } else {
                                /*
                                 * The Primary Key has not been found (was be searched already).
                                 * Skip exporting any LOB data for this table
                                 */
                                break;
                            }
                        }

                        /*
                         * Import CLOB/BLOB data to "<column_name>.<current Primary Key value>.lob_data" file
                         */
                        String outputBinaryFileName = FilenameUtils.separatorsToSystem(outputPath + "/"
                                + map2FileNameStatic(schema_name, "DATA_TABLE", tableName, preparedTemplateDataLob, columnNamesArray[i] + "." + rs.getString(primaryKeyColumn), "lob_data"));
                        File outputBinaryFile = new File(outputBinaryFileName);
                        log.debug(String.format("Export data table LOB '%s' column '%s' with id '%s' to file: %s",
                                    fullTableName.toLowerCase(), columnNamesArray[i], rs.getString(primaryKeyColumn), outputBinaryFile.getAbsolutePath()));

                        if (columnTypes[i] == Types.CLOB) {
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
                        try {
                            v = rs.getString(i + 1);
                        } catch (Exception e) {
                            if (!isPresentUnknownType) {
                                log.info(String.format("   !!!> Error with take data from the '%s' table and '%s' column with unknown column type: %s", fullTableName, columnNamesArray[i], rsmd.getColumnTypeName(i + 1)));
                                isPresentUnknownType = true;
                            }
                            v = null;
                        }

                        if (v != null) {
                            columnValues += "'" + v.replaceAll("'", "''") + "'";
                        }
                        else {
                            columnValues += "null";
                        }
                        break;
                }
            }
            p.println(String.format("INSERT INTO %s (%s) values (%s);",
                                    fullTableName,
                                    columnNames,
                                    columnValues));
        }
        p.close();
    }

}
