package com.googlecode.scheme2ddl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;

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
        this.charsetName = charsetNameDefault;
        encoder = Charset.forName(charsetNameDefault).newEncoder();
    }

    public DDLFormatter(String charsetName) {
        this.charsetName = charsetName;
        if (charsetName != null && !charsetName.equals("")) {
            encoder = Charset.forName(charsetName).newEncoder();
        } else {
            encoder = Charset.forName(charsetNameDefault).newEncoder();
        }
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
        if (charsetName != null && !charsetName.equals("")) {
            this.charsetName = charsetName;
            encoder = Charset.forName(charsetName).newEncoder();
        }
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
        boolean isWordContentEol;

        public WordsList(String text) {
            this.text = text;
            this.currentWordBeginPointer = 0;
            this.textLength = text.length();
            this.quoteSymbolHasBegun = false;
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

            if (quoteSymbolHasBegun || text.charAt(currentWordBeginPointer) == '\'')
            {  // Do not split a quoted word(s).

                if (!quoteSymbolHasBegun) {
                    // We are start searching for a closing quote: '
                    quoteSymbolHasBegun = true;
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
                    if (currentSymbol == '\'')
                    {
                        endWordPosition++;
                        if (endWordPosition != textLength && text.charAt(endWordPosition) == '\'') {
                            // skip quoting quote characters (double quote): ''
                            endWordPosition++;
                            continue;
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

        for (char character: ddl.toCharArray()) {
            position++;

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
                    /*
                     * Think, change and Uncomment then need split lines if them is greater than maxSqlplusCmdLineLength
                     * This is need do in the 'splitBigLinesByNewline' method above (may be, or may be here).
                     *... result += ddl.substring(startLinePosition, position);
                     *... startLinePosition = position;
                     */
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
}
