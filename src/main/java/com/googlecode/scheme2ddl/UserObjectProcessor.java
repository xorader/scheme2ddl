package com.googlecode.scheme2ddl;

import com.googlecode.scheme2ddl.dao.UserObjectDao;
import com.googlecode.scheme2ddl.TableExportProperty;
import com.googlecode.scheme2ddl.domain.UserObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Properties;

import static com.googlecode.scheme2ddl.TypeNamesUtil.map2TypeForDBMS;

/**
 * @author A_Reshetnikov
 * @since Date: 17.10.2012
 */
public class UserObjectProcessor implements ItemProcessor<UserObject, UserObject> {

    private static final Log log = LogFactory.getLog(UserObjectProcessor.class);
    private UserObjectDao userObjectDao;
    private DDLFormatter ddlFormatter;
    private FileNameConstructor fileNameConstructor;
    private Map<String, Set<String>>    excludes;
    private Map<String, Set<String>>    dependencies;
    private Map<String, Boolean>        settingsUserObjectProcessor;
    private Map<String, Set<String>>    dependenciesInSeparateFiles;
    private ArrayList<String>           excludesDataTables;
    private Map<String, Properties>     includesDataTables;
    private Set<String>                 typesQuotesFormating;
    private boolean isUsedSchemaNamesInFilters = false;
    private boolean isExportDataTable = false;
    private boolean isSortExportedDataTable = false;
    private boolean replaceSequenceValues = false;
    private boolean fixTriggerWithoutObjectnameOwner = false;
    private boolean fixJobnameWithoutOwner = false;
    private String sortingByColumnsRegexpList = null;
    private String dataCharsetName = null;

    public UserObject process(UserObject userObject) throws Exception {
        String ddl;

        if (needToExclude(userObject)) {
            log.debug(String.format("Skipping processing of user object %s ", userObject));
            return null;
        }
        ddlFormatter.setEncoder(dataCharsetName);

        ddl = map2Ddl(userObject);
        userObject.setFileName(fileNameConstructor.map2FileName(userObject));

        if (isExportDataTable && userObject.getType().equals("TABLE")) {
            TableExportProperty tableProperty = getTableExportProperties(userObject);

            if (tableProperty.maxRowsExport != TableExportProperty.doesNotExportData) {
                userObjectDao.exportDataTable(userObject, tableProperty, fileNameConstructor, isSortExportedDataTable, sortingByColumnsRegexpList, dataCharsetName);
            } else {
                log.debug(String.format("Skipping processing of data table of: %s ", userObject));
            }
        }

        if (typesQuotesFormating != null && typesQuotesFormating.contains(userObject.getType())) {
            ddl = ddlFormatter.quoteVarcharDDLEols(ddl);
        }

        userObject.setDdl(ddl);
        return userObject;
    }

    private boolean needToExclude(UserObject userObject) {
        if (excludes == null || excludes.size() == 0) return false;

        String fullTableName;
        if (isUsedSchemaNamesInFilters && userObject.getSchema() != null) {
            fullTableName = userObject.getSchema() + "." + userObject.getName();
        } else {
            fullTableName = userObject.getName();
        }

        if (excludes.get("*") != null) {
            for (String pattern : excludes.get("*")) {
                if (matchesByPattern(fullTableName, pattern))
                    return true;
            }
        }
        for (String typeName : excludes.keySet()) {
            if (typeName.equalsIgnoreCase(userObject.getType())) {
                if (excludes.get(typeName) == null) return true;
                for (String pattern : excludes.get(typeName)) {
                    if (matchesByPattern(fullTableName, pattern))
                        return true;
                }
            }
        }
        return false;
    }

    /* http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Properties.html
     * http://docs.oracle.com/javase/6/docs/api/java/util/Map.html
     *
     * Return values in TableExportProperty.maxRowsExport:
     *  0  - need to export unlimited rows of the object's table (default value)
     *  -1 - do not export the whole table data
     *  >0 - limit exporting table rows by this value
     */
    private TableExportProperty getTableExportProperties(UserObject userObject) {
        String fullTableName;
        TableExportProperty result = new TableExportProperty(TableExportProperty.emptyMaxRowsExport, null, null);

        if (isUsedSchemaNamesInFilters && userObject.getSchema() != null) {
            fullTableName = userObject.getSchema() + "." + userObject.getName();
        } else {
            fullTableName = userObject.getName();
        }

        if (includesDataTables != null && includesDataTables.size() > 0) {
            for (String tableNamePattern : includesDataTables.keySet()) {
                if (matchesByPattern(fullTableName, tableNamePattern)) {
                    Properties props = includesDataTables.get(tableNamePattern);
                    String maxRowsExport = props.getProperty("maxRowsExport");
                    String where = props.getProperty("where");
                    String orderBy = props.getProperty("orderBy");

                    if (maxRowsExport != null && result.maxRowsExport == TableExportProperty.emptyMaxRowsExport)
                        result.maxRowsExport = Integer.parseInt(maxRowsExport);
                    if (where != null && result.where == null)
                        result.where = where;
                    if (orderBy != null && result.orderBy == null)
                        result.orderBy = orderBy;

                    if (result.maxRowsExport != TableExportProperty.emptyMaxRowsExport && result.where != null && result.orderBy != null)
                       return result;
                }
            }
            if (result.maxRowsExport != TableExportProperty.emptyMaxRowsExport)
                return result;
        }

        result.maxRowsExport = TableExportProperty.unlimitedExportData;

        if (excludesDataTables == null || excludesDataTables.size() == 0)
            return result;

        for (String tableNamePattern : excludesDataTables) {
            if (matchesByPattern(fullTableName, tableNamePattern))
                result.maxRowsExport = TableExportProperty.doesNotExportData;
                return result;
        }
        return result;
    }


    private boolean matchesByPattern(String s, String pattern) {
        pattern = pattern.replace("*", "(.*)").toLowerCase();
        return s.toLowerCase().matches(pattern);
    }

    private String map2Ddl(UserObject userObject) {
        if (userObject.getType().equals("DBMS JOB")) {
            return ddlFormatter.formatDDL(userObjectDao.findDbmsJobDDL(userObject.getName()));
        } else if (userObject.getType().equals("PUBLIC DATABASE LINK")) {
            return ddlFormatter.formatDDL(userObjectDao.findDDLInPublicScheme(map2TypeForDBMS(userObject.getType()), userObject.getName()));
        } else if (userObject.getType().equals("USER")) {
            if (userObject.getSchema().equals("PUBLIC")) {
                return userObjectDao.generatePublicGrants();
            }
            return ddlFormatter.formatDDL(userObjectDao.generateUserDDL(userObject.getName()));
        } else if (userObject.getType().equals("TABLESPACE")) {
            return ddlFormatter.formatDDL(userObjectDao.generateTablespaceDDL(userObject.getName()));
        }
        String res = userObjectDao.findPrimaryDDL(map2TypeForDBMS(userObject.getType()), userObject.getName());
        if (userObject.getType().equals("SEQUENCE") && replaceSequenceValues) {
            res = ddlFormatter.replaceActualSequenceValueWithOne(res);
        } else if (userObject.getType().equals("TRIGGER") && fixTriggerWithoutObjectnameOwner) {
            res = DDLFormatter.checkAndFixTriggerWithoutObjectnameOwner(res);
        } else if (userObject.getType().equals("JOB") && fixJobnameWithoutOwner) {
            res = DDLFormatter.checkAndFixJobnameWithoutOwner(res, userObject.getSchema());
        }

        Set<String> dependedTypes = dependencies.get(userObject.getType());
        if (dependedTypes != null) {
            for (String dependedType : dependedTypes) {
                String resultDDL = userObjectDao.findDependentDLLByTypeName(dependedType, userObject.getName(), userObject.getType());

                if (dependenciesInSeparateFiles != null && (
                        (dependenciesInSeparateFiles.get(userObject.getType()) != null && dependenciesInSeparateFiles.get(userObject.getType()).contains(dependedType))
                        || (dependenciesInSeparateFiles.get("*") != null && dependenciesInSeparateFiles.get("*").contains(dependedType))
                    ))
                {
                    if (dependedType.equals("CONSTRAINT")) {
                        resultDDL = filterSysGeneratedStrangeConstraintDDL(resultDDL);
                    } else if (dependedType.equals("INDEX")) {
                        resultDDL = filterSysGeneratedStrangeIndexDDL(resultDDL);
                    }

                    if (resultDDL != null && !resultDDL.equals(""))
                    {
                        userObject.setDependentDDL(dependedType,  ddlFormatter.formatDDL(resultDDL),
                                fileNameConstructor.map2FileNameRaw(userObject.getSchema(), dependedType, userObject.getName()));
                    }
                } else {
                    if (ddlFormatter.getIsMorePrettyFormat())
                        res += ddlFormatter.newline;
                    res += resultDDL;
                }
            }
        }
        return ddlFormatter.formatDDL(res);
    }

    /**
     * Filter ddl constraints:
     * 1) Oracle generate strange CONSTRAINT DDL for table columns with nested tables.
     * 2) Oracle generate ddls for PK CONSTRAINTs for IOT (index-organized table) tables. Not need it (because this PK defines in tables creating). http://www.orafaq.com/wiki/Index-organized_table
     */
    private String filterSysGeneratedStrangeConstraintDDL(final String allDdl) {
        String resultAllDdl = "";
        DDLListFormatter ddlList = new DDLListFormatter(allDdl);

        for (String oneDdl = ddlList.getNextOneDdl(); oneDdl != null; oneDdl = ddlList.getNextOneDdl()) {
            if (!ddlList.isDllConstraintUniqueSysGenerated(oneDdl, userObjectDao)
                && !ddlList.isDllConstraintPrimaryKeyWithIOT(oneDdl, userObjectDao))
            {
                resultAllDdl += oneDdl;
            }
        }
        return resultAllDdl;
    }

    /**
     * Index ddl constraints:
     * 1) Oracle generate index ddls for IOT (index-organized table) tables. Not need it (because all columns of IOT is indexed already).
     */
    private String filterSysGeneratedStrangeIndexDDL(final String allDdl) {
        String resultAllDdl = "";
        DDLListFormatter ddlList = new DDLListFormatter(allDdl);

        for (String oneDdl = ddlList.getNextOneDdl(); oneDdl != null; oneDdl = ddlList.getNextOneDdl()) {
            if (!ddlList.isDllIndexUniqueWithIOT(oneDdl, userObjectDao))
            {
                resultAllDdl += oneDdl;
            }
        }
        return resultAllDdl;
    }

    public void setExcludes(Map excludes) {
        this.excludes = excludes;
    }

    public void setExcludesDataTables(ArrayList excludesDataTables) {
        this.excludesDataTables = excludesDataTables;
    }

    public void setTypesQuotesFormating(Set typesQuotesFormating) {
        this.typesQuotesFormating = typesQuotesFormating;
    }

    public void setIncludesDataTables(Map includesDataTables) {
        this.includesDataTables = includesDataTables;
    }

    public void setDependencies(Map<String, Set<String>> dependencies) {
        this.dependencies = dependencies;
    }

    public void setDependenciesInSeparateFiles(Map<String, Set<String>> dependenciesInSeparateFiles) {
        this.dependenciesInSeparateFiles = dependenciesInSeparateFiles;
    }

    public void setUserObjectDao(UserObjectDao userObjectDao) {
        this.userObjectDao = userObjectDao;
    }

    public void setDdlFormatter(DDLFormatter ddlFormatter) {
        this.ddlFormatter = ddlFormatter;
    }

    public void setFileNameConstructor(FileNameConstructor fileNameConstructor) {
        this.fileNameConstructor = fileNameConstructor;
    }

    public void setSettingsUserObjectProcessor(Map<String, Boolean> settingsUserObjectProcessor) {
        this.settingsUserObjectProcessor = settingsUserObjectProcessor;

        if (settingsUserObjectProcessor.get("isUsedSchemaNamesInFilters") != null
                && settingsUserObjectProcessor.get("isUsedSchemaNamesInFilters"))
        {
            this.isUsedSchemaNamesInFilters = true;
        }
        if (settingsUserObjectProcessor.get("isExportDataTable") != null
                && settingsUserObjectProcessor.get("isExportDataTable"))
        {
            this.isExportDataTable = true;
        }
        if (settingsUserObjectProcessor.get("isSortExportedDataTable") != null
                && settingsUserObjectProcessor.get("isSortExportedDataTable"))
        {
            this.isSortExportedDataTable = true;
        }
        if (settingsUserObjectProcessor.get("replaceSequenceValues") != null
                && settingsUserObjectProcessor.get("replaceSequenceValues"))
        {
            this.replaceSequenceValues = true;
        }
        if (settingsUserObjectProcessor.get("fixTriggerWithoutObjectnameOwner") != null
                && settingsUserObjectProcessor.get("fixTriggerWithoutObjectnameOwner"))
        {
            this.fixTriggerWithoutObjectnameOwner = true;
        }
        if (settingsUserObjectProcessor.get("fixJobnameWithoutOwner") != null
                && settingsUserObjectProcessor.get("fixJobnameWithoutOwner"))
        {
            this.fixJobnameWithoutOwner = true;
        }
    }

    public void setSortingByColumnsRegexpList(String list) {
        this.sortingByColumnsRegexpList = list.toLowerCase();
    }

    public void setDataCharsetName(String dataCharsetName) {
        this.dataCharsetName = dataCharsetName;
    }

    public void setReplaceSequenceValues(boolean replaceSequenceValues) {
        this.replaceSequenceValues = replaceSequenceValues;
    }

    public void setFixTriggerWithoutObjectnameOwner(boolean fixTriggerWithoutObjectnameOwner) {
        this.fixTriggerWithoutObjectnameOwner = fixTriggerWithoutObjectnameOwner;
    }

    public void setFixJobnameWithoutOwner(boolean fixJobnameWithoutOwner) {
        this.fixJobnameWithoutOwner = fixJobnameWithoutOwner;
    }
}
