package ru.transset.projects.file2cell;

//import java.sql.*;
//import java.io.*;
//import java.io.IOException;
//import java.util.*;

// Needed since we will be using Oracle's BLOB, part of Oracle's JDBC extended
// // classes. Keep in mind that we could have included Java's JDBC interfaces
// // java.sql.Blob which Oracle does implement. The oracle.sql.BLOB class 
// // provided by Oracle does offer better performance and functionality.
//import oracle.sql.*;

// Needed for Oracle JDBC Extended Classes
//import oracle.jdbc.*;

import ru.transset.projects.file2cell.OracleBackend;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import java.util.Properties;
import java.util.Enumeration;

/**
 * The file2cell util main class.
 * This util load LOB/XML data from the file (or whole directory) to the Oracle
 * table cell(s) (with BLOB, CLOB or XMLTYPE type).
 *
 * @author Molchanov Alexander (xorader@mail.ru)
 * @since Date: 2015-12-16
 */
public class Main
{
    static String newline = System.getProperty("line.separator");

    public static void main( String[] args )
    {
        CommandLine line = createAndParseArgs(args);

        OracleBackend db = new OracleBackend();
        //if (!db.openOracleConnection("SCOTT/TIGER@192.168.8.7:1521:TRS")) {
        //if (!db.openOracleConnection("@//192.168.8.7:1521/TRS")) {
        if (!db.openOracleConnection(line.getOptionValue("url"))) {
            System.out.println("Can't open Oracle connection. Exit.");
            System.exit(2);
        }

        /*
        Statement stmt = db.getConnection().createStatement();
        //ResultSet rset = stmt.executeQuery("select 'Hello OCI driver tester '||USER||'!' result from dual");
        //
        ResultSet rset = stmt.executeQuery("SELECT * FROM DEPT");
        while (rset.next())
            System.out.println(rset.getString(1));

        rset.close();
        stmt.close();
        */

        //fromFileToBLOB(String binaryFileName, String sqlBlobTable, String sqlBlobColumn, String sqlBlobIdName, String sqlBlobIdValue)
        // "SELECT lob_file FROM table_test1 WHERE id = 4 FOR UPDATE"
        //System.out.println("go - 1:");
        //db.fromFileToLOB("../../tmp/scott-neftdev12/DATA_TABLES/TABLE_TEST1/LOB_FILE.4.blob_data", OracleBackend.typeBLOB, "SCOTT.table_test1", "lob_file", "id", "4");
        //System.out.println("go - 2:");
        //db.fromFileToLOB("../../tmp/scott-neftdev12/DATA_TABLES/TABLE_TEST2/CLOB_TEXT.1.clob_data", OracleBackend.typeCLOB, "TABLE_TEST2", "CLOB_TEXT", "ID", "1");
        //

        if(line.hasOption("debug")) {
            db.setVerbose(true);
            db.setDebug(true);
        } else if(line.hasOption("verbose")) {
            db.setVerbose(true);
        }

        if(line.hasOption("spinner")) {
            db.setSpinner(true);
        }

        String schemaName;
        if (line.hasOption("schema")) {
            schemaName = line.getOptionValue("schema") + ".";
        } else if (line.hasOption("autoschema")) {
            schemaName = null;
        } else {
            schemaName = "";    // uses current schema for connection
        }

        int lobType = 0;
        if (line.hasOption("lobtype")) {
            lobType = db.lobTypeNameToInt(line.getOptionValue("lobtype"));
            if (lobType == 0) {
                System.out.println("Unknown specified lob/xml type: '" + line.getOptionValue("lobtype") + "'. Can be: BLOB, CLOB or XMLTYPE.");
                System.exit(4);
            }
        }

        if(line.hasOption("file")) {
            if (db.fromFileToLOB(line.getOptionValue("file"), lobType, schemaName, line.getOptionValue("table"),
                    line.getOptionValue("column"), line.getOptionValue("idcolumn"), line.getOptionValue("idvalue")))
            {
                if (line.hasOption("debug")) {
                    System.out.println("Load LOB/XML data from the file '" + line.getOptionValue("file") + "' - success.");
                }
            } else {
                System.exit(3);
            }
        } else {    // line.hasOption("directory")
            if (db.fromDirToLOBS(line.getOptionValue("directory"), lobType, line.getOptionValue("wildcard"),
                    schemaName, line.getOptionValue("table"),
                    line.getOptionValue("column"), line.getOptionValue("idcolumn")))
            {
                if (line.hasOption("debug")) {
                    System.out.println("Load LOB/XML data from the directory '" + line.getOptionValue("directory") + "' - success.");
                }
            } else {
                System.exit(3);
            }
        }

        db.closeOracleConnection();
    }

    public static CommandLine createAndParseArgs(String[] args)
    {
        CommandLine line = null;
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "show this help message and quit.");
        options.addOption("v", "verbose", false, "do more verbosity.");
        options.addOption("D", "debug", false, "do extremly verbosity.");
        options.addOption(Option.builder("u").longOpt("url").required(true).hasArg().desc("connection url to DB. Example: SCOTT/TIGER@localhost:1521:SIDNAME").build());
        options.addOption("l", "lobtype", true, "cell type. Can be 'CLOB', 'BLOB' or 'XMLTYPE'. If not specified - will get from the file extention (file extention can be '" + OracleBackend.blobFileExtention + "', '" + OracleBackend.clobFileExtention + "' or '" + OracleBackend.xmlFileExtention + "').");
        options.addOption("t", "table", true, "cell table name. If not specified - will get from the directory name (parent of the file).");
        options.addOption("c", "column", true, "cell column name. If not specified - will get from the part of filename (characters before first dot).");
        options.addOption("i", "idcolumn", true, "ID/PK column name for row identify of the cell. If not specified - will get from the '" + OracleBackend.primaryKeyFile +"' file (in file(s) directory).");
        options.addOption("r", "idvalue", true, "ID/PK column value for row identify of the cell. If not specified - will get from the part of filename (characters(digits) before file extention and after previous dot).");
        options.addOption("s", "schema", true, "schema name. If not specified - will uses current schema for connection.");
        options.addOption("S", "autoschema", false, "get schema name from the 3 parent directory name of specified file(s).");
        options.addOption("w", "wildcard", true, "filter filenames (uses for load whole directory).");
        options.addOption("n", "spinner", false, "show spinner, then processing whole directory lob files (does not workes with verbose or debug options).");

        OptionGroup mainTargetOptions = new OptionGroup();
        mainTargetOptions.setRequired(true);
        mainTargetOptions.addOption(new Option("f", "file", true, "export file to lob/xmltype cell."));
        mainTargetOptions.addOption(new Option("d", "directory", true, "export whole directory to table lobs/xmltype."));
        options.addOptionGroup(mainTargetOptions);

        try {
            line = parser.parse( options, args );
            if(line.hasOption("help")) {
                printHelp(options);
                System.exit(0);
            }
        } catch(ParseException e) {
            System.out.println( "Error during parse cmd arguments: " + e.getMessage() );
            printHelp(options);
            System.exit(1);
        }
        return line;
    }

    public static void printHelp(final Options options)
    {
        final HelpFormatter helpFormatter = new HelpFormatter();
        System.out.println("This util load LOB or XMLTYPE data from the file (or whole directory) to the Oracle" + newline + "table cell(s) (with BLOB, CLOB or XMLTYPE type).");
        helpFormatter.printHelp( "file2cell.jar", options, true);
        System.out.println("Example autodetection parts from the full filename:" + newline + " ...anypath/SCHEMA_NAME/data_tables/CELL_TABLE_NAME/CELL_COLUMN_NAME.CELL_ID_VALUE.blob_data");
        System.out.println("Example usage: " + newline + " java -Doracle.net.tns_admin=/etc/oracle -Djava.security.egd=file:/dev/./urandom -jar file2cell.jar -u 'sys as sysdba/somepass@SQL_TEST_HOST2' -d somepath/with_lobs_dir -s 'SCOTT'");
    }
}

