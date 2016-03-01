# file2cell

 Command line util for import file(s) into Oracle cell(s). Supports cells types:
* BLOB
* CLOB
* XMLTYPE

This util can import files in whole directory into table column. For more info look in util help output - `java -jar file2cell-1.0.jar -h`:
```
This util load LOB or XMLTYPE data from the file (or whole directory) to the Oracle
table cell(s) (with BLOB, CLOB or XMLTYPE type).
usage: file2cell.jar [-c <arg>] [-D] -d <arg> | -f <arg>  [-h] [-i <arg>]
       [-l <arg>] [-n] [-r <arg>] [-s <arg>] [-S] [-t <arg>] -u <arg> [-v]
       [-w <arg>]
1G -c,--column <arg>      cell column name. If not specified - will get from
                        the part of filename (characters before first
                        dot).
 -D,--debug             do extremly verbosity.
 -d,--directory <arg>   export whole directory to table lobs/xmltype.
 -f,--file <arg>        export file to lob/xmltype cell.
 -h,--help              show this help message and quit.
 -i,--idcolumn <arg>    ID/PK column name for row identify of the cell. If
                        not specified - will get from the '_.primary_key'
                        file (in file(s) directory).
 -l,--lobtype <arg>     cell type. Can be 'CLOB', 'BLOB' or 'XMLTYPE'. If
                        not specified - will get from the file extention
                        (file extention can be 'blob_data', 'clob_data' or
                        'xmltype').
 -n,--spinner           show spinner, then processing whole directory lob
                        files (does not workes with verbose or debug
                        options).
 -r,--idvalue <arg>     ID/PK column value for row identify of the cell.
                        If not specified - will get from the part of
                        filename (characters(digits) before file extention
                        and after previous dot).
 -s,--schema <arg>      schema name. If not specified - will uses current
                        schema for connection.
 -S,--autoschema        get schema name from the 3 parent directory name
                        of specified file(s).
 -t,--table <arg>       cell table name. If not specified - will get from
                        the directory name (parent of the file).
 -u,--url <arg>         connection url to DB. Example:
                        SCOTT/TIGER@localhost:1521:SIDNAME
 -v,--verbose           do more verbosity.
 -w,--wildcard <arg>    filter filenames (uses for load whole directory).
Example autodetection parts from the full filename:
 ...anypath/SCHEMA_NAME/data_tables/CELL_TABLE_NAME/CELL_COLUMN_NAME.CELL_ID_VALUE.blob_data
Example usage: 
 java -Doracle.net.tns_admin=/etc/oracle -Djava.security.egd=file:/dev/./urandom -jar file2cell.jar -u 'sys as sysdba/somepass@SQL_TEST_HOST2' -d somepath/with_lobs_dir -s 'SCOTT'
```
 
### Howto build Jar

 In Linux just run: `make`.

 In Windows run `mvn clean package` and result jar will `target/file2cell-1.0-jar-with-dependencies.jar`.

