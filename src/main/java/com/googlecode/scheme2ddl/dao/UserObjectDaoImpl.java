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

import com.googlecode.scheme2ddl.TableExportProperty;
import com.googlecode.scheme2ddl.dao.InsertStatements;
import com.googlecode.scheme2ddl.FileNameConstructor;
import static com.googlecode.scheme2ddl.TypeNamesUtil.map2TypeForConfig;
import java.io.IOException;

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
                    "          from all_nested_tables unt" +
                    "         where t.object_name = unt.table_name and t.owner = unt.owner)" +
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

    public List<UserObject> findTablespaces() {
        String sql = "SELECT TABLESPACE_NAME AS object_name, 'TABLESPACE' AS object_type FROM dba_tablespaces";
        try {
            return getJdbcTemplate().query(sql, new UserObjectRowMapper());
        } catch (BadSqlGrammarException e) {
            log.info("Can not access to 'dba_tablespaces' table. This user has no grants for it, and so not getting tablespaces.");
            return new ArrayList<UserObject>();
        }
    }

    public String generateTablespaceDDL(final String name) {
        return executeDbmsMetadataGetDdl("select dbms_metadata.get_ddl(?, ?) from dual", "TABLESPACE", name, null);
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
             *
             * Anyway, I do not known how to exclude system generated CONSTRAINTS (for nested tables, for example).
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

        return "BEGIN\n" + ((String) getJdbcTemplate().execute(sql, new CallableStatementCallbackImpl())) + "\nEND;\n/";
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

    public void exportDataTable(UserObject userObject, final TableExportProperty tableProperty, final FileNameConstructor fileNameConstructor, final boolean isSortExportedDataTable, final String sortingByColumnsRegexpList, final String dataCharsetName) {
        final String tableName = userObject.getName();
        final String schema_name = schemaName;
        final String preparedTemplate = fileNameConstructor.getPreparedTemplate();
        final String preparedTemplateDataLob = fileNameConstructor.getPreparedTemplateDataLob();

        String result_execute = (String) getJdbcTemplate().execute(new ConnectionCallback() {
            public String doInConnection(Connection connection) throws SQLException, DataAccessException {
                InsertStatements insertStatements = new InsertStatements(dataCharsetName);
                try {
                    insertStatements.generateInsertStatements(connection, schema_name, tableName, tableProperty, preparedTemplate, preparedTemplateDataLob, outputPath, isSortExportedDataTable, sortingByColumnsRegexpList);
                } catch (IOException e) {
                    logger.error("Error with write to data file of '" + tableName + "' table: " + e.getMessage(), e);
                }
                return null;
            }
        });
    }

    public boolean isConstraintUniqueSysGenerated(final String ownerConstraint, final String tableConstraint, final String columnConstraint) {
        final String sqlQueryGenConst = "SELECT 1 FROM all_constraints cn, all_ind_columns ix "
            + "WHERE cn.owner = ? AND cn.table_name = ? AND ix.column_name = ? "
            + "AND cn.owner = ix.index_owner AND cn.constraint_type = 'U' "
            + "AND cn.generated = 'GENERATED NAME' AND cn.index_name = ix.index_name";
        try {
            final int resultCounter = getJdbcTemplate().queryForInt(sqlQueryGenConst, ownerConstraint, tableConstraint, columnConstraint);
            if (resultCounter > 0) {
                return true;
            }
        } catch (DataAccessException e) {
            return false;
        }
        return false;
    }
}
