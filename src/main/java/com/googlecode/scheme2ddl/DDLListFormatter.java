package com.googlecode.scheme2ddl;

import com.googlecode.scheme2ddl.dao.UserObjectDao;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author A_Molchanov
 * @since Date: 21.01.2016
 */
public class DDLListFormatter {
    private final String allDdl;
    private int currentDdlBeginPointer;
    private final int allDdlLength;

    public DDLListFormatter(String allDdl) {
        this.allDdl = allDdl;
        this.currentDdlBeginPointer = 0;
        this.allDdlLength = allDdl.length();
    }

    public String getNextOneDdl() {
        if (currentDdlBeginPointer >= allDdlLength) {
            return null;
        }
        int endDdlPosition;

        // not needs for CONSTRAINTs (because it is simple DDLs).
        // TODO: skip comment lines and areas
        // TODO: skip names and varchar values (in the "'" and '"' symbols)

        endDdlPosition = currentDdlBeginPointer+1;
        while (endDdlPosition != allDdlLength && allDdl.charAt(endDdlPosition) != ';') {
            endDdlPosition++;
        }

        if (endDdlPosition != allDdlLength) {
            endDdlPosition++;   // add the ';' symbol in this ddl too
        }

        // skip (save in end of current 'oneDdl') all spaces and eols characters
        while (endDdlPosition != allDdlLength) {
            final char currentSymbol = allDdl.charAt(endDdlPosition);
            if (currentSymbol == '\n') {
                endDdlPosition++;
                break;
            } else if (currentSymbol == '\r' || currentSymbol == ' ' || currentSymbol == '\t') {
                endDdlPosition++;
            } else {
                break;
            }
        }

        final int resultDdlBeginPointer = currentDdlBeginPointer;
        currentDdlBeginPointer = endDdlPosition;
        return allDdl.substring(resultDdlBeginPointer, endDdlPosition);
    }

    private static final Pattern patternConstraintUnique = Pattern.compile("^[ \t\r\n]*ALTER TABLE \"([^\"]+)\"\\.\"([^\"]+)\" ADD UNIQUE \\(\"([^\"]+)\"\\).*", Pattern.DOTALL);
    /**
     *  Find ddl's like: ALTER TABLE "SCOTT"."TAB9_NESTED" ADD UNIQUE ("OWNERS") ...;
     *  and return true if it is system generated object ddl;
     */
    public boolean isDllConstraintUniqueSysGenerated(final String oneDdl, final UserObjectDao userObjectDao) {
        Matcher matchDdl = patternConstraintUnique.matcher(oneDdl);
        if (!matchDdl.find()) {
            return false;
        }

        // arguments: owner, tableName, columnName
        return userObjectDao.isConstraintUniqueSysGenerated(matchDdl.group(1), matchDdl.group(2), matchDdl.group(3));
    }
}

