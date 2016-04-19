package ru.transset.projects.file2cell;

import java.sql.*;
import java.io.*;
//import java.util.*;
import org.apache.commons.io.FilenameUtils;

// Needed since we will be using Oracle's LOB, part of Oracle's JDBC extended
// classes. Keep in mind that we could have included Java's JDBC interfaces
// java.sql.Blob/Clob which Oracle does implement. The oracle.sql.BLOB/CLOB class
// provided by Oracle does offer better performance and functionality.
import oracle.sql.*;

// Needed for Oracle JDBC Extended Classes
import oracle.jdbc.*;

/**
 * Used for workes with Oracle DB
 *
 * @author Molchanov Alexander (xorader@mail.ru)
 */
public class OracleBackend
{
    private Connection conn = null;
    private boolean verbose = false;
    private boolean debug = false;
    private boolean spinner = false;

    public static int typeBLOB = 1;
    public static int typeCLOB = 2;
    public static int typeXMLTYPE = 3;

    public static String blobFileExtention = "blob_data";
    public static String clobFileExtention = "clob_data";
    public static String xmlFileExtention = "xmltype";



    public static String primaryKeyFile = "_.primary_key";
    /**
     * Obtain a connection to the Oracle database.
     *
     */
    public boolean openOracleConnection(String dbUrl)
    {
        if (dbUrl == null) {
            System.out.println("Error (openOracleConnection): empty dbUrl");
            return false;
        }
        String connectionUrl = "jdbc:oracle:thin:" + dbUrl;   // + "SCOTT/TIGER@192.168.8.7:1521:TRS"
        //String user = extractUserfromDbUrl(dbUrl);
        //String password = extractPasswordfromDbUrl(dbUrl);

        // dynamically load the driver's class file into memory, which automatically registers it
        try {
                // use newInstance() method to work around noncompliant JVMs
            Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
        } catch(ClassNotFoundException e) {
            System.out.println("Error (openOracleConnection): unable to load oracle sql driver class!");
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch(IllegalAccessException e) {
            System.out.println("Error (openOracleConnection): access problem while loading oracle sql driver!");
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch(InstantiationException e) {
            System.out.println("Error (openOracleConnection): unable to instantiate oracle sql driver!");
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        }

        // create  the connection object
        try {
            //conn = DriverManager.getConnection(connectionUrl, user, password);
            //conn = DriverManager.getConnection(connectionUrl, "SCOTT", "TIGER");
            conn = DriverManager.getConnection(connectionUrl);
        } catch(SQLException e) {
            System.out.println("Error (openOracleConnection): can not connect to DB!");
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        }
        return true;
    }

    private static String extractUserfromDbUrl(String dbUrl) {
        return dbUrl.split("/")[0];
    }

    private static String extractPasswordfromDbUrl(String dbUrl) {
        //scott/tiger@localhost:1521:ORCL
        return dbUrl.split("/|@")[1];
    }

    /**
     * Close Oracle database connection
     */
    public boolean closeOracleConnection()
    {
        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println("Error (closeOracleConnection): Caught SQL Exception.");
            if (verbose) {
                e.printStackTrace();
            }
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e2) {
                    System.out.println("Error (closeOracleConnection): Caught SQL (Rollback Failed) Exception.");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Return connection pointer
     */
    public Connection getConnection()
    {
        return conn;
    }

    public void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }

    public void setSpinner(boolean spinner)
    {
        this.spinner = spinner;
    }

    private int dotsCountOfFileName(File file)
    {
        String filename = file.getName();
        int counter = 0;
        for(int i=0; i < filename.length(); i++) {
            if(filename.charAt(i) == '.') {
                counter++;
            }
        }
        return counter;
    }

    private String getFileExtensionFromFile(File file)
    {
        String filename = file.getName();
        try {
            return filename.substring(filename.lastIndexOf(".") + 1);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
        // also, we can uses the 'FilenameUtils.getExtension(file.getName())'
    }

    public int lobTypeNameToInt(String lobName) {
        if (lobName.equalsIgnoreCase("BLOB")) {
            return typeBLOB;
        } else if (lobName.equalsIgnoreCase("CLOB")) {
            return typeCLOB;
        } else if (lobName.equalsIgnoreCase("XMLTYPE")) {
            return typeXMLTYPE;
        } else {
            return 0;
        }
    }

    private int getLobTypeFromFile(File file)
    {
        String fileExtention = getFileExtensionFromFile(file);

        if (fileExtention.equalsIgnoreCase(blobFileExtention)) {
            return typeBLOB;
        } else if (fileExtention.equalsIgnoreCase(clobFileExtention)) {
            return typeCLOB;
        } else if (fileExtention.equalsIgnoreCase(xmlFileExtention)) {
            return typeXMLTYPE;
        } else {
            return 0;
        }
    }

    /**
     * Get ID argument (typically, the number) from the part of filename.
     * Example:
     *   SOME_COLUMN.4.blob_data -- will '4'
     *   5.clob_data -- will '5'
     *   333.any_extention -- will '333'
     */
    private String getLobIdValueFromFile(File file)
    {
        String filename = file.getName();
        try {
            int extensionIndex = filename.lastIndexOf(".");

            return filename.substring(filename.lastIndexOf(".", extensionIndex - 1) + 1, extensionIndex);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private String getLobColumnNameFromFile(File file)
    {
        if (dotsCountOfFileName(file) < 2) {
            return null;
        }
        String filename = file.getName();
        try {
            int firstDotIndex = filename.indexOf(".");
            return filename.substring(0, firstDotIndex);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private String getLobTableFromFileParentDir(File file)
    {
        File parentDir = file.getAbsoluteFile().getParentFile();

        if (parentDir == null) {
            return null;
        }

        return parentDir.getName();
    }

    private String getLobTableSchemaFromFile(File file)
    {
        File parentParentParentDir;

        try {
            parentParentParentDir = file.getAbsoluteFile().getParentFile().getParentFile().getParentFile();
        } catch (NullPointerException e) {
            return null;
        }

        if (parentParentParentDir == null) {
            return null;
        }

        return parentParentParentDir.getName();
    }

    private String getLobTableSchemaFromDirectory(File dir)
    {
        File parentParentDir;

        try {
            parentParentDir = dir.getAbsoluteFile().getParentFile().getParentFile();
        } catch (NullPointerException e) {
            return null;
        }

        if (parentParentDir == null) {
            return null;
        }

        return parentParentDir.getName();
    }

    /**
     * Try to autodetect ID column (primary key) for table with LOB data.
     * Get this ID from the '_.primary_key' file (in the directory of the specified file).
     */
    private String getLobIdNameInFileDir(File file)
    {
        return getLobIdNameFromFile(new File(file.getAbsoluteFile().getParentFile().getAbsoluteFile() + "/" + primaryKeyFile));
    }

    /**
     * Read first line from the file.
     */
    private String getLobIdNameFromFile(File pkFile)
    {
        BufferedReader pkText = null;

        try {
            pkText = new BufferedReader(new FileReader(pkFile));
            return pkText.readLine();
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            return null;
        } finally {
            if (pkText != null) {
                try {
                    pkText.close();
                } catch (IOException e) {
                    pkText = null;
                }
            }
        }
    }

    /**
     * write data contained in a file to an Oracle LOB column.
     * @param lobType The type of the LOB column. Can be: typeBLOB, typeCLOB, typeXMLTYPE
     *
     * @return Boolean value, which mean success operation status
     */
    public boolean fromFileToLOB(String fileName, int lobType, String sqlLobSchemaName, String sqlLobTable, String sqlLobColumn, String sqlLobIdName, String sqlLobIdValue)
    {
        File inputFile = new File(fileName);
        final String fullLobTableName;

        if (!inputFile.exists()) {
            System.out.println("Error (fromFileToLOB): Can not find the file: " + fileName);
            return false;
        }

        if (lobType == 0) {
            lobType = getLobTypeFromFile(inputFile);
            if (lobType == 0)
            {
                System.out.println("Error (fromFileToLOB): can not autodetect lob type from file extention: " + inputFile.getName());
                return false;
            }
        }

        if (sqlLobTable == null) {
            sqlLobTable = getLobTableFromFileParentDir(inputFile);
            if (sqlLobTable == null) {
                System.out.println("Error (fromFileToLOB): can not autodetect table name from file parent name: " + inputFile.getName());
                return false;
            }
        }

        if (sqlLobSchemaName == null) {
            sqlLobSchemaName = getLobTableSchemaFromFile(inputFile);
            if (sqlLobSchemaName == null) {
                System.out.println("Error (fromFileToLOB): can not autodetect schema name from file 3 parent name: " + inputFile.getName());
                return false;
            } else {
                fullLobTableName = sqlLobSchemaName + "." + sqlLobTable;
            }
        } else {
            fullLobTableName = sqlLobSchemaName + sqlLobTable;
        }

        if (sqlLobColumn == null) {
            sqlLobColumn = getLobColumnNameFromFile(inputFile);
            if (sqlLobColumn == null) {
                System.out.println("Error (fromFileToLOB): can not autodetect column name from file name: " + inputFile.getName());
                return false;
            }
        }

        if (sqlLobIdName == null) {
            sqlLobIdName = getLobIdNameInFileDir(inputFile);
            if (sqlLobIdName == null) {
                System.out.println("Error (fromFileToLOB): can not autodetect primary key name(id column name) from the '" + primaryKeyFile + "' file from directory of file: " + inputFile.getName());
                return false;
            }
        }

        if (sqlLobIdValue == null) {
            sqlLobIdValue = getLobIdValueFromFile(inputFile);
            if (sqlLobIdValue == null) {
                System.out.println("Error (fromFileToLOB): can not autodetect lob id value from file name: " + inputFile.getName());
                return false;
            }
        }

        if (debug) {
            System.out.println("Fullname of processing file: " + inputFile.getAbsolutePath());
            System.out.println("lobType (1-blob, 2-clob, 3-xmltype): " + lobType);
            System.out.println("sqlLobTable: " + sqlLobTable);
            System.out.println("sqlLobSchemaName: " + sqlLobSchemaName + (sqlLobSchemaName.equals("") ? "current connection schema" : ""));
            System.out.println("fullLobTableName: " + fullLobTableName);
            System.out.println("sqlLobColumn: " + sqlLobColumn);
            System.out.println("sqlLobIdName: '" + sqlLobIdName + "'");
            System.out.println("sqlLobIdValue: " + sqlLobIdValue);
        }

        /*
         *  TODO: make workes for nested tables - for STRUCT and ARRAYs cells subtypes.
         */

        if (lobType == typeXMLTYPE) {
            return fromFileToXMLTYPE(fileName, fullLobTableName, sqlLobColumn, sqlLobIdName, sqlLobIdValue);
        }

        if (inputFile.length() < Integer.MAX_VALUE) {
            // this methods do not support big files (more then 2Gb~)
            if (lobType == typeBLOB) {
                return fromFileToBLOBFastVersion(fileName, fullLobTableName, sqlLobColumn, sqlLobIdName, sqlLobIdValue);
            } else if (lobType == typeCLOB) {
                return fromFileToCLOBFastVersion(fileName, fullLobTableName, sqlLobColumn, sqlLobIdName, sqlLobIdValue);
            } else {
                System.out.println("Error (fromFileToLOB): do not support this type: " + lobType);
                return false;
            }
        } else {
            if (lobType == typeBLOB) {
                return fromFileToBLOBBIGVersion(fileName, fullLobTableName, sqlLobColumn, sqlLobIdName, sqlLobIdValue);
            } else if (lobType == typeCLOB) {
                return fromFileToCLOBBIGVersion(fileName, fullLobTableName, sqlLobColumn, sqlLobIdName, sqlLobIdValue);
            } else {
                System.out.println("Error (fromFileToLOB): do not support this type: " + lobType);
                return false;
            }
        }
    }

    /**
     * Method used to write binary data contained in a file to an Oracle BLOB column.
     * Uses algorithm, which support big file size (more then 2GB - maximum of int - 2^31-1)
     */
    private boolean fromFileToBLOBBIGVersion(String binaryFileName, String sqlBlobTable, String sqlBlobColumn, String sqlBlobIdName, String sqlBlobIdValue)
    {
        OutputStream blobOutputStream = null;
        FileInputStream inputFileInputStream = null;
        Statement stmt = null;
        ResultSet rset = null;
        final String sqlUpdateBlobByEmpty = "UPDATE " + sqlBlobTable + " SET " + sqlBlobColumn + "=EMPTY_BLOB() WHERE " + sqlBlobIdName + " = " + sqlBlobIdValue;
        final String sqlQueryWhereBLOB = "SELECT " + sqlBlobColumn + " FROM " + sqlBlobTable + " WHERE " + sqlBlobIdName + " = " + sqlBlobIdValue +" FOR UPDATE";
        File inputBinaryFile = new File(binaryFileName);

        try {
            inputFileInputStream = new FileInputStream(inputBinaryFile);

            stmt = conn.createStatement();

            // The cell of updating BLOB must be filled by 'EMPTY_BLOB()' value. This create the BLOB locator.
            // Otherwise an error - the 'getBLOB(1)' below call method always returnes null.
            final int count = stmt.executeUpdate(sqlUpdateBlobByEmpty);
            if (count == 0) {
                return false;
            }

            // Get BLOB locator
            if (debug) {
                System.out.println("Processing BLOB locator by: " + sqlQueryWhereBLOB);
            }
            rset = stmt.executeQuery(sqlQueryWhereBLOB);
            if (!rset.next()) {
                return false;
            }
            BLOB blob = ((OracleResultSet) rset).getBLOB(1);
            if (blob == null) {
                System.out.println("Error (fromFileToBLOBBIGVersion): can not get BLOB locator. SQL query: " + sqlQueryWhereBLOB);
                return false;
            }

            // We can use obsoleted 'blob.getBinaryOutputStream()' call, also. Below the new JDBC 3.0 version:
            blobOutputStream = blob.setBinaryStream(0L);

            int bufferSize = blob.getBufferSize();
            byte[] byteBuffer = new byte[bufferSize];
            int bytesRead;
            while ((bytesRead = inputFileInputStream.read(byteBuffer)) != -1) {
                blobOutputStream.write(byteBuffer, 0, bytesRead);
            }

            inputFileInputStream.close();
            inputFileInputStream = null;
            // Keep in mind that we still have the stream open. Once the stream
            // gets open, you cannot perform any other database operations
            // until that stream has been closed. This even includes a COMMIT
            // statement. It is possible to loose data from the stream if this
            // rule is not followed. If you were to attempt to put the COMMIT in
            // place before closing the stream, Oracle will raise an
            // "ORA-22990: LOB locators cannot span transactions" error.
            blobOutputStream.close();
            blobOutputStream = null;

            conn.commit();
            return true;
        } catch (SQLException e) {
            System.out.println("Error (fromFileToBLOBBIGVersion): can not update BLOB in DB");
            System.out.println("SQL query: " + sqlQueryWhereBLOB);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch (IOException e) {
            System.out.println("Error (fromFileToBLOBBIGVersion): can not read binary data to BLOB from file: " + binaryFileName);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } finally {
            if (blobOutputStream != null) {
                try {
                    blobOutputStream.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToBLOBBIGVersion): can not close blobOutputStream");
                    System.out.println("SQL query: " + sqlQueryWhereBLOB);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (inputFileInputStream != null) {
                try {
                    inputFileInputStream.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToBLOBBIGVersion): can not close inputFileInputStream. File: " + binaryFileName);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToBLOBBIGVersion): can not close ResultSet");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToBLOBBIGVersion): can not close Statement");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
        }
    }

    /**
     * Method used to write binary data contained in a file to an Oracle BLOB column.
     * Uses fast and light algorithm, which not support big file size
     * (less then 2GB - maximum of int - 2^31-1)
     */
    private boolean fromFileToBLOBFastVersion(String binaryFileName, String sqlBlobTable, String sqlBlobColumn, String sqlBlobIdName, String sqlBlobIdValue)
    {
        File inputBinaryFile = new File(binaryFileName);
        FileInputStream inputFileInputStream = null;
        PreparedStatement pstmt = null;
        final String sqlUpdateBlobQuery = "UPDATE " + sqlBlobTable + " SET " + sqlBlobColumn + " = ? WHERE " + sqlBlobIdName + " = ?";


        try {
            if (debug) {
                System.out.println("Fast processing BLOB locator by: " + sqlUpdateBlobQuery + " (" + sqlBlobIdValue + ")");
            }

            if (inputBinaryFile.length() == 0) {
                // update by 'empty_blob()' value and exit
                final String sqlUpdateBlobByEmpty = "UPDATE " + sqlBlobTable + " SET " + sqlBlobColumn + "=EMPTY_BLOB() WHERE " + sqlBlobIdName + " = " + sqlBlobIdValue;
                Statement stmt = conn.createStatement();
                final int empty_count = stmt.executeUpdate(sqlUpdateBlobByEmpty);
                stmt.close();

                if (empty_count == 0) {
                    return false;
                } else {
                    return true;
                }
            }

            inputFileInputStream = new FileInputStream(inputBinaryFile);
            pstmt = conn.prepareStatement(sqlUpdateBlobQuery);

            // We can not use the 'pstmt.setBinaryStream(1, inputFileInputStream)' method with ulimited stream size and
            // method with 'long' input size argument. This methods added in JDBC 4.0 (Java 1.6), only.
            // So, we use method with 'int' input size argument, only (and this method not support size arguments,
            // more then maximum of 'int' = 2^31-1 = 2GB~
            pstmt.setBinaryStream(1, inputFileInputStream, (int) inputBinaryFile.length());
            pstmt.setString(2, sqlBlobIdValue);
            final int count = pstmt.executeUpdate();

            inputFileInputStream.close();
            inputFileInputStream = null;
            // If you were to attempt to put the COMMIT in
            // place before closing the stream, Oracle will raise an
            // "ORA-22990: LOB locators cannot span transactions" error.
            pstmt.close();
            pstmt = null;

            conn.commit();
            if (count == 0) {
                return false;
            } else {
                return true;
            }
        } catch (SQLException e) {
            System.out.println("Error (fromFileToBLOBFastVersion): can not update BLOB in DB");
            System.out.println("SQL query: " + sqlUpdateBlobQuery);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch (IOException e) {
            System.out.println("Error (fromFileToBLOBFastVersion): can not read binary data to BLOB from file: " + binaryFileName);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToBLOBFastVersion): can not close PreparedStatement");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (inputFileInputStream != null) {
                try {
                    inputFileInputStream.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToBLOBFastVersion): can not close inputFileInputStream. File: " + binaryFileName);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
        }
    }

    /**
     * Method used to write text contained in a file to an Oracle CLOB column.
     * Uses algorithm, which support big file size (more then 2GB - maximum of int - 2^31-1)
     */
    private boolean fromFileToCLOBBIGVersion(String textFileName, String sqlClobTable, String sqlClobColumn, String sqlClobIdName, String sqlClobIdValue)
    {
        Writer clobWriter = null;
        File file = new File(textFileName);
        BufferedReader fileReader = null;
        Statement stmt = null;
        ResultSet rset = null;
        final String sqlUpdateClobByEmpty = "UPDATE " + sqlClobTable + " SET " + sqlClobColumn + "=EMPTY_CLOB() WHERE " + sqlClobIdName + " = " + sqlClobIdValue;
        final String sqlQueryWhereCLOB = "SELECT " + sqlClobColumn + " FROM " + sqlClobTable + " WHERE " + sqlClobIdName + " = " + sqlClobIdValue +" FOR UPDATE";
        File inputTextFile = new File(textFileName);

        try {
            fileReader = new BufferedReader(new FileReader(file));  // here we can insert some encoder (codepage)

            stmt = conn.createStatement();

            // The cell of updating CLOB must be filled by 'EMPTY_CLOB()' value. This create the CLOB locator.
            // Otherwise an error - the 'getCLOB(1)' below call method always returnes null.
            final int count = stmt.executeUpdate(sqlUpdateClobByEmpty);
            if (count == 0) {
                return false;
            }

            // Get CLOB locator
            if (debug) {
                System.out.println("Processing CLOB locator by: " + sqlQueryWhereCLOB);
            }
            rset = stmt.executeQuery(sqlQueryWhereCLOB);
            if (!rset.next()) {
                return false;
            }
            CLOB clob = ((OracleResultSet) rset).getCLOB(1);

            /*
             * TODO: we can use the 'CLOB tmpClob = CLOB.createTemporary(conn, true, CLOB.DURATION_SESSION);' construction
             * and do not create the EMPTY_CLOB() cell with this method... Algorithm:
             * 1) create temporary CLOB tmpClob
             * 2) update CLOB cell (without intermediate operation: update cell by EMPTY_CLOB()):
             *      pstmt = conn.prepareStatement("UPDATE sqlClobTable SET sqlClobColumn = ? WHERE sqlClobIdName = ?");
             *      pstmt.setCLOB(1, tmpClob);
             *      pstmt.setString(2, sqlClobIdValue);
             *      ...
             *      tmpClob.freeTemporary(); // require ?
             * The same method for a BLOBs too...
             * But i am not sure, that creating temporary CLOB is more faster then doing two queries (one for update
             * cell to EMPTY_CLOB(), and second - fill LOB cell by data). Because copying a LOB data can be high cost.
             * Anyway need to test (measure time) before do this.
             */

            if (clob == null) {
                System.out.println("Error (fromFileToCLOBBIGVersion): can not get CLOB locator. SQL query: " + sqlQueryWhereCLOB);
                return false;
            }

            // We can use obsoleted 'blob.getCharacterOutputStream()' call, also. Below the new JDBC 3.0 version:
            clobWriter = clob.setCharacterStream(0L);   // return a Unicode output stream

            int bufferSize = clob.getBufferSize();  // 32528, for example for Oracle 11.2
            char[] charBuffer = new char[bufferSize];
            int charsRead;
            while ((charsRead = fileReader.read(charBuffer, 0, bufferSize)) != -1) {
                clobWriter.write(charBuffer, 0, charsRead);
            }

            fileReader.close();
            fileReader = null;
            // Keep in mind that we still have the stream open. Once the stream
            // gets open, you cannot perform any other database operations
            // until that stream has been closed. This even includes a COMMIT
            // statement. It is possible to loose data from the stream if this
            // rule is not followed. If you were to attempt to put the COMMIT in
            // place before closing the stream, Oracle will raise an
            // "ORA-22990: LOB locators cannot span transactions" error.
            clobWriter.close();
            clobWriter = null;

            conn.commit();
            return true;
        } catch (SQLException e) {
            System.out.println("Error (fromFileToCLOBBIGVersion): can not update CLOB in DB");
            System.out.println("SQL query: " + sqlQueryWhereCLOB);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch (IOException e) {
            System.out.println("Error (fromFileToCLOBBIGVersion): can not read text to CLOB from file: " + textFileName);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } finally {
            if (clobWriter != null) {
                try {
                    clobWriter.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToCLOBBIGVersion): can not close Writer to CLOB object.");
                    System.out.println("SQL query: " + sqlQueryWhereCLOB);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToCLOBBIGVersion): can not close file Reader object. File: " + textFileName);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToCLOBBIGVersion): can not close ResultSet");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToCLOBBIGVersion): can not close Statement");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
        }
    }

    /**
     * Method used to write text contained in a file to an Oracle CLOB column.
     * Uses fast and light algorithm, which not support big file size
     * (less then 2GB - maximum of int - 2^31-1)
     */
    private boolean fromFileToCLOBFastVersion(String textFileName, String sqlClobTable, String sqlClobColumn, String sqlClobIdName, String sqlClobIdValue)
    {
        File file = new File(textFileName);
        Reader fileReader = null;
        PreparedStatement pstmt = null;
        final String sqlUpdateClobQuery = "UPDATE " + sqlClobTable + " SET " + sqlClobColumn + " = ? WHERE " + sqlClobIdName + " = ?";

        try {
            if (debug) {
                System.out.println("Fast processing CLOB locator by: " + sqlUpdateClobQuery + " (" + sqlClobIdValue + ")");
            }

            if (file.length() == 0) {
                // update by 'empty_blob()' value and exit
                final String sqlUpdateClobByEmpty = "UPDATE " + sqlClobTable + " SET " + sqlClobColumn + "=EMPTY_CLOB() WHERE " + sqlClobIdName + " = " + sqlClobIdValue;
                Statement stmt = conn.createStatement();
                stmt.executeUpdate(sqlUpdateClobByEmpty);
                stmt.close();
                return true;
            }

            fileReader = (Reader) new BufferedReader(new FileReader(file));
            pstmt = conn.prepareStatement(sqlUpdateClobQuery);

            // We can not use the 'pstmt.setCharacterStream(1, fileReader)' method with ulimited stream size and
            // method with 'long' input size argument. This methods added in JDBC 4.0 (Java 1.6), only.
            // So, we use method with 'int' input size argument, only (and this method not support size arguments,
            // more then maximum of 'int' = 2^31-1 = 2GB~
            pstmt.setCharacterStream(1, fileReader, (int) file.length());
            pstmt.setString(2, sqlClobIdValue);
            final int count = pstmt.executeUpdate();

            fileReader.close();
            fileReader = null;
            // If you were to attempt to put the COMMIT in
            // place before closing the stream, Oracle can raise an
            // "ORA-22990: LOB locators cannot span transactions" error.
            pstmt.close();
            pstmt = null;

            conn.commit();
            if (count == 0) {
                return false;
            } else {
                return true;
            }
        } catch (SQLException e) {
            System.out.println("Error (fromFileToCLOBFastVersion): can not update CLOB in DB");
            System.out.println("SQL query: " + sqlUpdateClobQuery);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } catch (IOException e) {
            System.out.println("Error (fromFileToCLOBFastVersion): can not read text to CLOB from file: " + textFileName);
            if (verbose) {
                e.printStackTrace();
            }
            return false;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToCLOBFastVersion): can not close PreparedStatement");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e2) {
                    System.out.println("Error (fromFileToCLOBFastVersion): can not close Reader object. File: " + textFileName);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
        }
    }


    /**
     * Create and return the CLOB object that holds the XML data (taking from xmlFileName).
     * Example: http://docs.oracle.com/cd/B28359_01/appdev.111/b28369/xdb11jav.htm#CHDHGIIC
     */
    private CLOB createTmpClob(String xmlFileName)
    {
        File file = new File(xmlFileName);
        CLOB tempClob = null;
        BufferedReader fileReader = null;
        Writer tempClobWriter = null;

        try {
            fileReader = new BufferedReader(new FileReader(file));  // here we can insert some encoder (codepage)

            tempClob = CLOB.createTemporary(conn, true, CLOB.DURATION_SESSION); // using cache buffer
            // Open the temporary CLOB in readwrite mode, to enable writing
            tempClob.open(CLOB.MODE_READWRITE);

            // We can use obsoleted 'blob.getCharacterOutputStream()' call, also.
            tempClobWriter = tempClob.setCharacterStream(0L);    // return a Unicode output stream

            int bufferSize = tempClob.getBufferSize();  // 32528, for example for Oracle 11.2
            char[] charBuffer = new char[bufferSize];
            int charsRead;
            while ((charsRead = fileReader.read(charBuffer, 0, bufferSize)) != -1) {
                tempClobWriter.write(charBuffer, 0, charsRead);
            }
            fileReader.close();
            fileReader = null;

            tempClobWriter.flush();
            tempClobWriter.close();
            tempClobWriter = null;

            tempClob.close();
            return tempClob;
        } catch (IOException e) {
            System.out.println("Error (createTmpClob): can not read text to tmp CLOB from file: " + xmlFileName);
            if (verbose) {
                e.printStackTrace();
            }
            if (tempClob != null) {
                try {
                    tempClob.freeTemporary();
                } catch (SQLException e2) {
                    System.out.println("Error (createTmpClob): can't free temporary CLOB during error");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                }
            }
            return null;
        } catch (SQLException e) {
            System.out.println("Error (createTmpClob): can not create tmp CLOB in DB for xmltype from file: " + xmlFileName);
            if (tempClob != null) {
                try {
                    tempClob.freeTemporary();
                } catch (SQLException e2) {
                    System.out.println("Error (createTmpClob): can't free temporary CLOB during error");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                }
            }
            if (verbose) {
                e.printStackTrace();
            }
            return null;
        } finally {
            if (tempClobWriter != null) {
                try {
                    tempClobWriter.close();
                } catch (IOException e2) {
                    System.out.println("Error (createTmpClob): can not close Writer to temporary CLOB object for xml data.");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return null;
                }
            }
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e2) {
                    System.out.println("Error (createTmpClob): can not close file Reader object. File: " + xmlFileName);
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return null;
                }
            }
        }
    }

    /**
     *  Method used to write xml contained in a file to an Oracle XMLTYPE column.
     *  Supported any file size.
     *  Example: http://docs.oracle.com/cd/B28359_01/appdev.111/b28369/xdb11jav.htm#CHDHGIIC
     *  Usefull urls:
     *   - http://thinktibits.blogspot.ru/2013/01/Insert-XML-XMLType-Column-Java-JDBC-Example.html
     *   - http://thinktibits.blogspot.com.au/2013/01/read-xmltype-xml-data-oracle-java-jdbc.html
     *   - http://thinktibits.blogspot.com.au/2013/01/read-xmltype-as-clob-java-jdbc-example.html
     */
    private boolean fromFileToXMLTYPE(String xmlFileName, String sqlXmlTable, String sqlXmlColumn, String sqlXmlIdName, String sqlXmlIdValue)
    {
        final String sqlUpdateXml = "UPDATE " + sqlXmlTable + " SET " + sqlXmlColumn + "= XMLType(?) WHERE " + sqlXmlIdName + " = ?";

        CLOB clob = createTmpClob(xmlFileName);
        if (clob == null) {
            return false;
        }

        if (debug) {
            System.out.println("Processing XML table cell by: " + sqlUpdateXml + " (" + sqlXmlIdValue + ")");
        }

        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sqlUpdateXml);
            pstmt.setObject(1, clob);
            pstmt.setString(2, sqlXmlIdValue);
            final int status = pstmt.executeUpdate();

            pstmt.close();
            pstmt = null;

            conn.commit();
            if (status == 1) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            System.out.println("Error (fromFileToXMLTYPE): can not update XMLTYPE column in DB");
            System.out.println("SQL query: " + sqlUpdateXml);
            if (verbose) {
                e.printStackTrace();
            }
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e2) {
                    System.out.println("Error (fromFileToXMLTYPE):  can not close PreparedStatement");
                    if (verbose) {
                        e2.printStackTrace();
                    }
                    return false;
                }
            }
        }
        return true;
    }


    private File[] getLobXmlFilesFromDirectory(File dir, final String filesWildcard, final String sqlLobColumn)
    {
        FileFilter filter = new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file.isDirectory()
                    || (filesWildcard == null && sqlLobColumn == null && dotsCountOfFileName(file) != 2)
                    || file.getName().equalsIgnoreCase(primaryKeyFile))
                {
                    return false;
                }

                if (filesWildcard != null) {
                    if (FilenameUtils.wildcardMatchOnSystem(file.getName(), filesWildcard)) {
                        return true;
                    } else {
                        return false;
                    }
                }

                if ((file.getName().toLowerCase().endsWith("." + blobFileExtention)
                    || file.getName().toLowerCase().endsWith("." + clobFileExtention)
                    || file.getName().toLowerCase().endsWith("." + xmlFileExtention))
                && (sqlLobColumn == null
                    || dotsCountOfFileName(file) < 2
                    || (dotsCountOfFileName(file) >=2 && file.getName().toLowerCase().startsWith(sqlLobColumn.toLowerCase() + "."))
                    ))
                {
                    return true;
                } else {
                    return false;
                }
            }
        };

        return dir.listFiles(filter);
    }

    /**
     * Load all files (with 'blob_data' or 'clob_data' extentions) in specified directory to the Oracle table.
     *
     * @return Boolean value, which mean success operation status
     */
    public boolean fromDirToLOBS(String directoryName, int lobType, String filesWildcard, String sqlLobSchemaName, String sqlLobTable, String sqlLobColumn, String sqlLobIdName)
    {
        File directory = new File(directoryName);
        final String fullLobTableName;
        int counter = 0;
        int failCounter = 0;
        int spinnerCurrentType = 0;

        if (!directory.exists()) {
            System.out.println("Error (fromDirToLOBS): Can not find the directory: " + directoryName);
            return false;
        }

        if (!directory.isDirectory()) {
            System.out.println("Error (fromDirToLOBS): Specified name is not directory:  " + directoryName);
            return false;
        }

        if (lobType != 0) {
            if (filesWildcard == null && sqlLobColumn == null) {
                System.out.println("Error (fromDirToLOBS): You can use lobtype option for exporting whole directory with wildcard or column options only.");
                return false;
            }
        }

        if (sqlLobTable == null) {
            sqlLobTable = directory.getName();
            // TODO: check table exist
            // ... return false; // if not exist
            if (sqlLobTable == null) {
                System.out.println("Error (fromDirToLOBS): Can not autodetect table name for loading lobs files");
                return false;
            }
        }

        if (sqlLobSchemaName == null) {
            sqlLobSchemaName = getLobTableSchemaFromDirectory(directory);
            if (sqlLobSchemaName == null) {
                System.out.println("Error (fromDirToLOBS): can not autodetect schema name from directory 2 parent name: " + directory.getName());
                return false;
            } else {
                fullLobTableName = sqlLobSchemaName + "." + sqlLobTable;
            }
        } else {
            fullLobTableName = sqlLobSchemaName + sqlLobTable;
        }

        if (sqlLobIdName == null) {
            sqlLobIdName = getLobIdNameFromFile(new File(directory.getAbsoluteFile() + "/" + primaryKeyFile));
            if (sqlLobIdName == null) {
                System.out.println("Error (fromDirToLOBS): can not autodetect primary key name(id column name) from the '"
                        + directory.getName() + "/" + primaryKeyFile + "'.");
                return false;
            }
        }

        if (debug) {
            System.out.println("Fullname of processing directory: " + directory.getAbsolutePath());
            System.out.println("filesWildcard: " + filesWildcard);
            System.out.println("sqlLobTable: " + sqlLobTable);
            System.out.println("sqlLobSchemaName: " + sqlLobSchemaName + (sqlLobSchemaName.equals("") ? "current connection schema" : ""));
            System.out.println("fullLobTableName (uses for all LOB files): " + fullLobTableName);
            System.out.println("sqlLobColumn: " + sqlLobColumn + (sqlLobColumn == null ? "(autodetect for every file in dir)" : ""));
            System.out.println("sqlLobIdName: '" + sqlLobIdName + "'");
            System.out.println("===== dir processing begins =====");
        }

        if (!verbose && !spinner) {
            System.out.print("Process LOBS for the '" + fullLobTableName + "' table: ");
        }

        final File[] lobFilesList = getLobXmlFilesFromDirectory(directory, filesWildcard, sqlLobColumn);
        final int lobFilesListSize = lobFilesList.length;

        for (final File lobFile : lobFilesList) {
            if (verbose) {
                System.out.println("-- load LOB/XML to '" + fullLobTableName + "' from file: " + lobFile.getName());
            }
            boolean status = fromFileToLOB(directoryName + "/" + lobFile.getName(), lobType, "", fullLobTableName, sqlLobColumn, sqlLobIdName, null);

            if (spinner) {
                if (counter % 10 == 0) {
                    final String counterAndBeginLineChar = " LOBS " + fullLobTableName + ": " +  counter + "/" + lobFilesListSize + " (fails: " + failCounter + ")\r";
                    switch (spinnerCurrentType) {
                        case 0: System.out.print("-"  + counterAndBeginLineChar); spinnerCurrentType = 1; break;
                        case 1: System.out.print("\\" + counterAndBeginLineChar); spinnerCurrentType = 2; break;
                        case 2: System.out.print("|"  + counterAndBeginLineChar); spinnerCurrentType = 3; break;
                        case 3: System.out.print("/"  + counterAndBeginLineChar); spinnerCurrentType = 0; break;
                    }
                }
            } else if (!verbose) {
                if (status) {
                    System.out.print(".");
                } else {
                    System.out.print("X");
                }
            }

            if (!status) {
                failCounter++;
            }
            counter++;
        }
        if (debug) {
            System.out.println("===== dir processing ends =====");
        }

        if (!verbose && !spinner) {
            System.out.println();
        }
        System.out.println("LOBS for the '" + fullLobTableName + "' table loaded: " + counter + "/" + lobFilesListSize + " (fails: " + failCounter + ")");

        if (failCounter == 0) {
            return true;
        } else {
            return false;
        }
    }
}

