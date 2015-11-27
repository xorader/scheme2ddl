package com.googlecode.scheme2ddl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author A_Reshetnikov
 * @since Date: 18.10.2012
 */
public class DDLFormatter {

    private boolean noFormat;
    private boolean statementOnNewLine;
    private boolean isMorePrettyFormat = false;

    static String newline = System.getProperty("line.separator");   //todo make it platform independent

    public String formatDDL(String ddl) {
        if (noFormat)
            return ddl;

        ddl = ddl.trim() + "\n";

        if (!isMorePrettyFormat)
            return ddl;

        /* smart formatting */
        ddl = ddl.replaceAll(newline + "GRANT ", newline + newline + "GRANT ");
        ddl = ddl.replaceAll(newline + "COMMENT ", newline + newline + "COMMENT ");
        ddl = ddl.replaceAll(newline + "  GRANT ", newline + "GRANT ");
        ddl = ddl.replaceAll(newline + "   COMMENT ", newline + "COMMENT ");
        ddl = ddl.replaceAll(newline + "  CREATE ", newline + "CREATE ");
        ddl = ddl.replaceAll(newline + "  ALTER ", newline + "ALTER ");
        return ddl;
    }

    public void setNoFormat(Boolean noFormat) {
        this.noFormat = noFormat;
    }

    @Deprecated
    public void setStatementOnNewLine(Boolean statementOnNewLine) {
        this.statementOnNewLine = statementOnNewLine;
    }

    public void setIsMorePrettyFormat(boolean isMorePrettyFormat) {
        this.isMorePrettyFormat = isMorePrettyFormat;
    }

    public boolean getIsMorePrettyFormat() {
        return this.isMorePrettyFormat;
    }

    public String replaceActualSequenceValueWithOne(String res) {

        String output;
        Pattern p = Pattern.compile("CREATE SEQUENCE (.*) START WITH (\\d+) (.*)");
        Matcher m = p.matcher(res);
        if (m.find()) {
             output = m.replaceFirst("CREATE SEQUENCE " + m.group(1) + " START WITH 1 " + m.group(3) );
            if (!"1".equals(m.group(2)))
             output = output + newline + "/* -- actual sequence value was replaced by scheme2ddl to 1 */";
        }
        else {
            output = res;
        }
        return output;
    }

    /**
     * Parse multistring varchar values and quote every new lines around by '
     * TODO: Split lines if them is greater than maxSqlplusCmdLineLength (1497).
     *      Look example in dao/InsertStatements.java:formatTextColumnValue method
     */
    public String quoteVarcharDDLEols(String ddl) {
        String result = "";
        int position = 0;
        int startLinePosition = 0;
        int quoteAtLineCounter = 0;

        for (char character: ddl.toCharArray()) {
            position++;
            if (character == '\n') {
                if (quoteAtLineCounter % 2 > 0) {
                    final String eolChrs;
                    final String currentLine;
                    if (position > 1 && ddl.charAt(position-2) == '\r') {
                        eolChrs = "'" + newline + "||CHR(13)||CHR(10)||'";
                        currentLine = ddl.substring(startLinePosition, position-2);
                    } else {
                        eolChrs = "'" + newline + "||CHR(10)||'";
                        currentLine = ddl.substring(startLinePosition, position-1);
                    }
                    result += currentLine + eolChrs;
                    startLinePosition = position;
                    quoteAtLineCounter = 1;
                } else {
                    // Uncomment then need split lines if them is greater than maxSqlplusCmdLineLength
                    //... result += ddl.substring(startLinePosition, position);
                    //... startLinePosition = position;
                    quoteAtLineCounter = 0;
                }
            } else if (character == '\'') {
                quoteAtLineCounter++;
            }
        }

        if (result.equals("")) {
            return ddl;
        }
        return result + ddl.substring(startLinePosition, position);
    }
}
