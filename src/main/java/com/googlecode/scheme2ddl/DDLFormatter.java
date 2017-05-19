package com.googlecode.scheme2ddl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;

import com.googlecode.scheme2ddl.dao.UserObjectDao;

/**
 * @author A_Reshetnikov
 * @author A_Molchanov
 * @since Date: 18.10.2012
 */
public class DDLFormatter {

    private boolean noFormat;
    private boolean statementOnNewLine;
    private boolean isMorePrettyFormat = false;

    public static String newline = System.getProperty("line.separator");
    private CharsetEncoder encoder;
    private static String charsetNameDefault = "UTF-8";
    private static String charsetName;
    public static final int maxSqlplusCmdLineLength = 2495;    // 2500 is the maximum of sqlplus CMD line length (and minus 5 bytes - sqlplus uses for eols and one character)

    public DDLFormatter() {
        this.charsetName = this.charsetNameDefault;
        this.encoder = Charset.forName(this.charsetName).newEncoder();
    }

    public DDLFormatter(String charsetName) {
        if (charsetName == null || charsetName.equals("")) {
            this.charsetName = this.charsetNameDefault;
        } else {
            this.charsetName = charsetName;
        }
        this.encoder = Charset.forName(this.charsetName).newEncoder();
    }

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

        ddl = splitBigLinesByNewline(ddl);
        return ddl;
    }

    public void setEncoder(String charsetName) {
        if (charsetName == null || charsetName.equals("")) {
            this.charsetName = this.charsetNameDefault;
        }
        this.encoder = Charset.forName(this.charsetName).newEncoder();
    }

    /**
     * Returns count of bytes (length) of symbol.
     *  'a' returns - 1
     *  'ю' returns - 2
     *  '茶' returns - 3
     * P.S. ugly java (for simple operation) :(
     */
    public int getBytesOfCharacter(final char symbol) {
        try {
            return encoder.encode(CharBuffer.wrap(new char[] { symbol })).limit();
        } catch (CharacterCodingException e) {
            return 1;
        }
    }

    private class WordsList {
        String text;
        int currentWordBeginPointer;
        int textLength;
        boolean quoteSymbolHasBegun;
        char currentQuoteSymbol;
        boolean isWordContentEol;

        public WordsList(String text) {
            this.text = text;
            this.currentWordBeginPointer = 0;
            this.textLength = text.length();
            this.quoteSymbolHasBegun = false;
            this.currentQuoteSymbol = '\0';
        }

        private boolean isWordEndCharacter(final char symbol) {
            switch (symbol) {
                case '\'':
                case ' ':
                case ',':
                case ';':
                case '\t':
                case '\n':
                    return true;
            }
            return false;
        }

        public boolean currentWordContentsEol() {
            return this.isWordContentEol;
        }

        public String getNextWord() {
            isWordContentEol = false;
            if (currentWordBeginPointer >= textLength) {
                return null;
            }

            int endWordPosition;

            if (quoteSymbolHasBegun || text.charAt(currentWordBeginPointer) == '\'' || text.charAt(currentWordBeginPointer) == '"')
            {  // Do not split a quoted word(s).

                if (!quoteSymbolHasBegun) {
                    // We are start searching for a closing quote: ' or "
                    quoteSymbolHasBegun = true;
                    currentQuoteSymbol = text.charAt(currentWordBeginPointer);
                    endWordPosition = currentWordBeginPointer+1;
                } else {
                    // continued on the next line quote
                    endWordPosition = currentWordBeginPointer;
                }
                for(;; endWordPosition++) {
                    if (endWordPosition == textLength) {
                        quoteSymbolHasBegun = false;
                        break;
                    }

                    final char currentSymbol = text.charAt(endWordPosition);
                    if (currentSymbol == currentQuoteSymbol)
                    {
                        endWordPosition++;
                        if (currentQuoteSymbol == '\'') {
                            // skip quoting quote characters (double quote): ''
                            if (endWordPosition != textLength && text.charAt(endWordPosition) == '\'') {
                                endWordPosition++;
                                continue;
                            }
                        }
                        quoteSymbolHasBegun = false;
                        break;
                    } else if (currentSymbol == '\n') {
                        isWordContentEol = true;
                        endWordPosition++;
                        break;
                    }
                }
            } else {
                endWordPosition = currentWordBeginPointer+1;
                while (endWordPosition != textLength && !isWordEndCharacter(text.charAt(endWordPosition))) {
                    endWordPosition++;
                }
            }

            // skip (save in end of current 'word') all spaces and '-' characters to eol
            while (endWordPosition != textLength) {
                final char currentSymbol = text.charAt(endWordPosition);
                if (currentSymbol == '\n') {
                    isWordContentEol = true;
                    endWordPosition++;
                    break;
                } else if (currentSymbol == '\r' || currentSymbol == ' ' || currentSymbol == '\t' || currentSymbol == '-') {
                    endWordPosition++;
                } else {
                    break;
                }
            }
            final int resultStartWordPosition = currentWordBeginPointer;
            this.currentWordBeginPointer = endWordPosition;
            return text.substring(resultStartWordPosition, endWordPosition);
        }
    }

    public int getStringBytes(String word) {
        int wordLength;
        try {
            wordLength = word.getBytes(charsetName).length;
        } catch (UnsupportedEncodingException e) {
            wordLength = word.length();
        }
        return wordLength;
    }

    public String splitBigLinesByNewline(String ddl) {
        String result = "";
        int currentLineLength = 0;
        boolean isCurrentLineComment = false;

        WordsList wordsList = new WordsList(ddl);

        for (String word = wordsList.getNextWord(); word != null; word = wordsList.getNextWord()) {
            int wordLength = getStringBytes(word);
            if (currentLineLength == 0) {
                // check 1st word of line
                isCurrentLineComment = isCommentLine(word);
            }

            if (currentLineLength != 0 && currentLineLength + wordLength + 1 > maxSqlplusCmdLineLength) {
                result += newline + (isCurrentLineComment ? "--" : "")  + word;
                if (wordsList.currentWordContentsEol()) {
                    currentLineLength = 0;
                } else {
                    currentLineLength = wordLength + (isCurrentLineComment ? 2 : 0);
                }
            } else {
                /*
                 * TODO: think about split quoted text-values, then it greater then maxSqlplusCmdLineLength
                 */
                result += word;
                if (wordsList.currentWordContentsEol()) {
                    currentLineLength = 0;
                } else {
                    currentLineLength += wordLength;
                }
            }
        }

        return result;
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

    private static final Pattern checkCommentLinePattern = Pattern.compile("^[ \t]*--.*");

    public static boolean isCommentLine(String line) {
        Matcher checkCommentLine = checkCommentLinePattern.matcher(line);
        return checkCommentLine.matches();
    }

    private static final Pattern checkAtSignInBeginingLinePattern = Pattern.compile("^[ \t]*@.*", Pattern.DOTALL);
    private boolean checkAtSignInNextLine(final String ddl, int positionBeginNextLine) {
        try {
            final String nextLines = ddl.substring(positionBeginNextLine);
            Matcher checkAtSignInBeginingLine = checkAtSignInBeginingLinePattern.matcher(nextLines);
            return checkAtSignInBeginingLine.matches();
        } catch (IndexOutOfBoundsException e) {
            return false;
        }
    }

    private static final Pattern checkLanguageJavaNamePattern = Pattern.compile(".*[ \t\r\n]+LANGUAGE[ \t]+JAVA[ \t]+NAME[^\r\n]*[\r\n]*",
                Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    private static boolean checkLanguageJavaNameInLastLine(final String ddl, int currentStartLinePosition, int currentEndLinePosition) {
        try {
            final String line = ddl.substring(currentStartLinePosition, currentEndLinePosition);
            Matcher checkLanguageJavaName = checkLanguageJavaNamePattern.matcher(line);
            return checkLanguageJavaName.matches();
        } catch (IndexOutOfBoundsException e) {
            return false;
        }
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
        boolean isCommentArea = false;
        boolean isCommentToEol = false;
        boolean isLanguageJavaNameArea = false;

        for (char character: ddl.toCharArray()) {
            position++;

            // Ignore and skip area, like: ... LANGUAGE JAVA NAME 'some_foo_with_eols_characters' ...
            if (isLanguageJavaNameArea) {
                if (character == '\'') {
                    isLanguageJavaNameArea = false;
                }
                continue;
            }

            // Ignore and skip commented area, like: /* blablabla */
            if (isCommentArea) {
                if (character == '/' && ddl.charAt(position-2) == '*') {
                    isCommentArea = false;
                }
                if (character == '\n' && checkAtSignInNextLine(ddl, position)) {
                    // anyway (for sqlplus) we need quote by ' character in begining of line with @ character
                    result += ddl.substring(startLinePosition, position) + "'";
                    startLinePosition = position;
                }
                continue;
            }
            if (isCommentToEol) {
                if (character == '\n') {
                    isCommentToEol = false;
                    //result += ddl.substring(startLinePosition, position);
                    //startLinePosition = position;
                }
                continue;
            }
            if (character == '*' && position > 1 && quoteAtLineCounter % 2 == 0 && ddl.charAt(position-2) == '/') {
                isCommentArea = true;
                continue;
            }
            if (character == '-' && position > 1 && quoteAtLineCounter % 2 == 0 && ddl.charAt(position-2) == '-') {
                isCommentToEol = true;
                continue;
            }

            if (character == '\n') {
                if (quoteAtLineCounter % 2 > 0) {
                    if (checkLanguageJavaNameInLastLine(ddl, startLinePosition, position)) {
                        isLanguageJavaNameArea = true;
                        quoteAtLineCounter = 0;
                        continue;
                    }
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

    private static final Pattern patternTriggerWithoutObjectnameOwner = Pattern.compile(
            "(?<part1>[ \t\r\n]*CREATE OR REPLACE TRIGGER \")(?<owner>[^\"]+)(?<part2>\"\\.\")(?<triggername>[^\"]+)"
            + "(?<part3>\"[ \r\n\t]+[^\n]+[ \t\r\n]+ON[ \t]+)(?<ownerobj>[^\\. \t]+\\.|)(?<obj>[^\\. \r\n\"]+)(?<part4>[ \t\r\n].*)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    /**
     * Find trigger ddl like:  CREATE OR REPLACE TRIGGER "SCOTT"."TRIGGER_DEPT" AFTER INSERT ON DEPT
     * and replace "DEPT" to "SCOTT.DEPT" (if found).
     */
    public static String checkAndFixTriggerWithoutObjectnameOwner(final String oneDdl) {
        Matcher matchDdl = patternTriggerWithoutObjectnameOwner.matcher(oneDdl);

        if (!matchDdl.find()) {
            return oneDdl;
        }

        if (!matchDdl.group("ownerobj").equals("")) {
            return oneDdl;
        }

        return matchDdl.group("part1") + matchDdl.group("owner") + matchDdl.group("part2")
            + matchDdl.group("triggername") + matchDdl.group("part3")
            + matchDdl.group("owner") + "." + matchDdl.group("obj") + matchDdl.group("part4");
    }

    private static final Pattern patternJobWithoutOwner = Pattern.compile(
            "(?<part1>[ \t\r\n]*BEGIN[ \t\r\n]+dbms_scheduler.create_job\\(([ \t\r\n]*job_name[ ]*=>[ ]*|)')(?<owner>\"[^\"]+\"\\.|)(?<jobname>\"[^\"]+\")"
            + "(?<part2>'[ \t\r\n]*,.*)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    /**
     * Find JOB without owner in name, ddl like:  BEGIN dbms_scheduler.create_job('"SOME_JOB"', job_type...
     * and add owner in name like: BEGIN dbms_scheduler.create_job('"SCOTT"."SOME_JOB"', job_type...
     */
    public static String checkAndFixJobnameWithoutOwner(final String oneDdl, final String schemaName) {
        if (schemaName == null || schemaName.equals("")) {
            return oneDdl;
        }
        Matcher matchDdl = patternJobWithoutOwner.matcher(oneDdl);

        if (!matchDdl.find()) {
            return oneDdl;
        }

        if (!matchDdl.group("owner").equals("")) {
            return oneDdl;
        }

        return matchDdl.group("part1") + "\"" + schemaName + "\"." + matchDdl.group("jobname") + matchDdl.group("part2");
    }

    private static final Pattern patternScheduleWithoutOwner = Pattern.compile(
            "(?<part1>[ \t\r\n]*BEGIN[ \t\r\n]+.*dbms_scheduler.create_schedule\\(')(?<owner>\"[^\"]+\"\\.|)(?<schedname>\"[^\"]+\")"
            + "(?<part2>'[ \t\r\n]*,.*)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    /**
     * Find Scheduler without owner in name, ddl like:  BEGIN .* dbms_scheduler.create_schedule('"SOME_SCHED"', start_date, repeat_interval, end_date, comments); .* ...
     * and add owner in name like: BEGIN .* dbms_scheduler.create_schedule('"SCOTT"."SOME_SCHED"', start_date, repeat_interval, end_date, comments); .* ...
     */
    public static String checkAndFixScheduleNameWithoutOwner(String oneDdl, final String schemaName) {
        // Fix the comma sign to the dot sign in the "TO_TIMESTAMP_TZ('23-APR-2017 08.36.18,000000000 PM +03:00'" expressions.
        // This is the Oracle DB bug after the "dbms_metadata.get_ddl()" executions.
        oneDdl = oneDdl.replaceAll("(TO_TIMESTAMP_TZ\\('[^']*)(,)([^']*')", "$1.$3");

        if (schemaName == null || schemaName.equals("")) {
            return oneDdl;
        }
        Matcher matchDdl = patternScheduleWithoutOwner.matcher(oneDdl);

        if (!matchDdl.find()) {
            return oneDdl;
        }

        if (!matchDdl.group("owner").equals("")) {
            return oneDdl;
        }

        return matchDdl.group("part1") + "\"" + schemaName + "\"." + matchDdl.group("schedname") + matchDdl.group("part2");
    }

    private static String tmp_procedure_name_prefix = "TMP_DOIT_20160212_";

    public static String fixCreateDBLink(final String ddl, final String objectName, final String schemaName, final UserObjectDao userObjectDao) {
        String resultDdl =
              "-- Special way for CREATE DATABASE LINK objects with correct OWNER (for current schema)\n"
            + "-- This is need because simple 'CREATE DATABASE LINK ...' query creates db_link with owner which executes this query ('SYS' for example),\n"
            + "--   but we need create with 'user_name' owner!\n"
            + "-- So, this is hack place (but it works good!).\n\n";

        int counter = 0;
        while (userObjectDao.isObjectExist(tmp_procedure_name_prefix + counter, schemaName)) {
            counter++;
        }
        final String tmpProcedureName = tmp_procedure_name_prefix + counter;

        /*
         *  Fix ':1' string value to real hash password. This fix workes only for sysdba connections.
         *  This is will fix error during upload this ddl back to DB:
         *    'ORA-02153: Invalid VALUES Password String' when creating a Database Link using BY VALUES with
         *    obfuscated password after upgrade to 11.2.0.4 (Doc ID 1905221.1).
         *  Usefull links:
         *    - http://www.ludovicocaldara.net/dba/ora-02153-create-database-link/
         *    - https://www.krenger.ch/blog/find-password-for-database-link/
         */
        final String hashDBLinkPassword = userObjectDao.getHashDBLinkPassword(objectName, schemaName);

        resultDdl += "CREATE PROCEDURE "+schemaName+"."+tmpProcedureName+"\n"
            + "IS\n"
            + "BEGIN\n"
            + "EXECUTE IMMEDIATE '"
            + ddl.replace("'", "''").replace(";", "").replace("':1'", hashDBLinkPassword == null ? "':1'" : "'"+hashDBLinkPassword+"'") + "\n"
            + "';\n"
            + "END;\n"
            + "/\n"
            + "BEGIN\n"
            + schemaName+"."+tmpProcedureName+";\n"
            + "END;\n"
            + "/\n"
            + "DROP PROCEDURE "+schemaName+"."+tmpProcedureName+";\n";

        return resultDdl;
    }

    private static final Pattern patternJobNumberInRefreshGroup = Pattern.compile(
            "(?<part1>[ \t\r\n]*BEGIN[ \t\r\n]+dbms_refresh.make\\(.+job[ \t\r\n]*=>[ \t\r\n]*)[0-9]+(?<part2>[ \t\r\n]*,.*)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    public static String fixJobNumberInRefreshGroup(final String oneDdl) {
        Matcher matchDdl = patternJobNumberInRefreshGroup.matcher(oneDdl);

        if (!matchDdl.find()) {
            return oneDdl;
        }

        return matchDdl.group("part1") + "0" + matchDdl.group("part2");
    }
}
