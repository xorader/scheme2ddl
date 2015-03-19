# Configuration #

Configuration provided by xml config. [Look at current one in svn](http://code.google.com/p/scheme2ddl/source/browse/trunk/src/main/resources/scheme2ddl.config.xml)

Default config placed inside scheme2ddl.jar.

You can use custom config with option

`java -jar scheme2ddl.jar --config <path_to_your_custom_config>`

Sample custom config placed in distribution package.

## What can be configured ##

| **name** |  **details** |
|:---------|:-------------|
| dataSource | Connection to Oracle |
| reader | Try to process Public Db Links and Dmbs Jobs |
| writer | Output path |
| taskExecutor | Number of threads |
| transformParams\_for\_dbms\_metadata |  |
| fileNameConstructor| Custom templates for construct filename <br> Ability to map file extension like 'TOAD' or 'PL/SQL Developer' (experimental) <br>
<tr><td> ddlFormatter </td><td> Some prettify options </td></tr>
<tr><td> dependencies </td><td> For example, indexes of tables </td></tr>
<tr><td> excludes </td><td> Patterns to exclude proccessing </td></tr>