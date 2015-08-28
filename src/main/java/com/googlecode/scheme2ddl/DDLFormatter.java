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
        ddl = ddl.replaceAll(newline + "GRANT ", newline + newline + "  GRANT ");
        ddl = ddl.replaceAll(newline + "COMMENT ", newline + newline + "   COMMENT ");
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
}
