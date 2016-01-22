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
     *  Find ddl like: ALTER TABLE "SCOTT"."TAB9_NESTED" ADD UNIQUE ("OWNERS") ...;
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

    private static final Pattern patternConstraintPK = Pattern.compile("^[ \t\r\n]*ALTER TABLE \"([^\"]+)\"\\.\"[^\"]+\" ADD CONSTRAINT \"([^\"]+)\" PRIMARY KEY \\(\"[^\"]+\"\\).*", Pattern.DOTALL);
    /**
     * Find ddl like: ALTER TABLE "SCOTT"."TABA_03_INDXORG" ADD CONSTRAINT "TABA_03_INDXORG_PK" PRIMARY KEY ("ID") ...;
     * and return true if it is PK(primary key) dll for IOT(index-organized table) tables.
     */
    public boolean isDllConstraintPrimaryKeyWithIOT(final String oneDdl, final UserObjectDao userObjectDao) {
        Matcher matchDdl = patternConstraintPK.matcher(oneDdl);
        if (!matchDdl.find()) {
            return false;
        }
        // arguments: PKname, PKowner
        return userObjectDao.isConstraintPKForIOT(matchDdl.group(2), matchDdl.group(1));
    }

    private static final Pattern patternUniqueIndex = Pattern.compile("^[ \t\r\n]*CREATE UNIQUE INDEX \"([^\"]+)\"\\.\"([^\"]+)\" ON \"[^\"]+\"\\.\"[^\"]+\" \\(\"[^\"]+\"\\).*", Pattern.DOTALL);
    /**
     * Find ddl like: CREATE UNIQUE INDEX "SCOTT"."TABA_03_INDXORG_PK" ON "SCOTT"."TABA_03_INDXORG" ("ID") ...;
     * and return true if it is index ddl for IOT(index-organized table) tables.
     */
    public boolean isDllIndexUniqueWithIOT(final String oneDdl, final UserObjectDao userObjectDao) {
        Matcher matchDdl = patternUniqueIndex.matcher(oneDdl);
        if (!matchDdl.find()) {
            return false;
        }
        // arguments: indexName, indexOwner
        return userObjectDao.isIndexForIOT(matchDdl.group(2), matchDdl.group(1));
    }

}

