package com.googlecode.scheme2ddl.dao;

import com.googlecode.scheme2ddl.TableExportProperty;
import com.googlecode.scheme2ddl.domain.UserObject;
import com.googlecode.scheme2ddl.FileNameConstructor;

import java.util.Collection;
import java.util.List;

/**
 * @author A_Reshetnikov
 * @since Date: 17.10.2012
 */
public interface UserObjectDao {

    List<UserObject> findListForProccessing();

    List<UserObject> findPublicDbLinks();

    List<UserObject> findDmbsJobs();

    List<UserObject> addUser();

    List<UserObject> findTablespaces();

    String findPrimaryDDL(String type, String name);

    String findDependentDLLByTypeName(String type, String parent_name, String parent_type);

    String findDDLInPublicScheme(String type, String name);

    String findDbmsJobDDL(String name);

    String generateUserDDL(String name);

    String generateTablespaceDDL(String name);

    void exportDataTable(UserObject userObject, TableExportProperty tableProperty, FileNameConstructor fileNameConstructor, boolean isSortExportedDataTable, String sortingByColumnsRegexpList, String dataCharsetName);

    boolean isConnectionAvailable();

}
