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
    List<UserObject> addPublicGrants();

    String findPrimaryDDL(String type, String name);

    String findDependentDLLByTypeName(String type, String parent_name, String parent_type);

    String findDDLInPublicScheme(String type, String name);

    String findDbmsJobDDL(String name);

    String generateUserDDL(String name);

    String generateTablespaceDDL(String name);

    String generatePublicGrants();

    void exportDataTable(UserObject userObject, TableExportProperty tableProperty, FileNameConstructor fileNameConstructor, boolean isSortExportedDataTable, String sortingByColumnsRegexpList, String dataCharsetName);

    boolean isConstraintUniqueSysGenerated(String ownerConstraint, String tableConstraint, String columnConstraint);

    boolean isConstraintPKForIOT(String PKname, String PKOwner);

    boolean isIndexForIOT(String indexName, String indexOwner);

    boolean isConnectionAvailable();

}
