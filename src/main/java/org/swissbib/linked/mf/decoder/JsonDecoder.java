package org.swissbib.linked.mf.decoder;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 25.10.16
 */
@Description("Parses json records. Null values are returned as empty string by default and can be changed by setting" +
        "parameter nullValue.")
@In(String.class)
@Out(StreamReceiver.class)
public class JsonDecoder extends DefaultObjectPipe<String, StreamReceiver> {
    private static final char STARTOBJECT = '{';
    private static final char ENDOBJECT = '}';
    private static final char STARTARRAY = '[';
    private static final char ENDARRAY = ']';
    private static final char STARTLITERAL = '"';
    private static final char ENDLITERAL = '"';
    private static final char SEPLITERALS = ':';
    private static final char SEPOBJECTS = ',';
    private static final char WHITESPACE = ' ';
    private static final char ESCAPECHAR = '\'';

    private static String nullValue = "";
    private static StringBuilder literal = new StringBuilder();
    private byte objectLevel = 0;
    private boolean inLiteral = false;
    private boolean inArray = false;
    private boolean ignoreNextChar = false;
    private boolean inSpecialLiteral = false;
    private String key = "";

    public static void setNullValue(String nullValue) {
        JsonDecoder.nullValue = nullValue;
    }

    private static boolean charArrayContains(char[] array, char character) {
        boolean contains = false;
        for (char elem : array) {
            if (elem == character) {
                contains = true;
                break;
            }
        }
        return contains;
    }

    private static boolean checkValidNumber(String s) {
        boolean valid = true;
        boolean followedBySign = false;
        char[] validFirstChars = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        char[] validMiddleChars = {'-', '+', 'e', 'E', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        char[] validLastChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        if (!charArrayContains(validFirstChars, s.charAt(0))) {
            valid = false;
        }
        for (int i = 1; i < s.length() - 1; i++) {
            if (!charArrayContains(validMiddleChars, s.charAt(i))) {
                valid = false;
                break;
            } else if (s.charAt(i) == 'e' || s.charAt(i) == 'E') {
                followedBySign = true;
            } else if (followedBySign) {
                if (!(s.charAt(i) == '-' || s.charAt(i) == '+')) { // e not followed by - or +
                    valid = false;
                } else {
                    followedBySign = false;
                }
            }
        }
        if (!charArrayContains(validLastChars, s.charAt(s.length() - 1))) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void process(String obj) {

        for (int i = 0; i < obj.length(); i++) {
            char c = obj.charAt(i);
            if (objectLevel == 0) {
                if (c == '{') {
                    objectLevel++;
                    getReceiver().startRecord("");
                }
            } else {
                if (ignoreNextChar) {
                    literal.append(c);
                    ignoreNextChar = false;
                } else {
                    if (inLiteral) {
                        switch (c) {
                            case ESCAPECHAR:
                                ignoreNextChar = true;
                                break;
                            case ENDLITERAL:
                                // Is value, else key
                                if (key.length() > 0) {
                                    getReceiver().literal(key, literal.toString());
                                    if (!inArray) {
                                        key = "";
                                    }
                                } else {
                                    key = literal.toString();
                                }
                                inLiteral = false;
                                literal.setLength(0);
                                break;
                            default:
                                literal.append(c);
                        }
                    } else if (inSpecialLiteral) { // If value is not delimited by apostrophes
                        switch (c) {
                            case SEPOBJECTS:
                            case ENDOBJECT:
                            case ENDARRAY:
                            case WHITESPACE:
                                if (literal.toString().equals("null")) {
                                    getReceiver().literal(key, nullValue);
                                } else if (literal.toString().equals("true") ||
                                        literal.toString().equals("false") ||
                                        checkValidNumber(literal.toString())) {
                                    getReceiver().literal(key, literal.toString());
                                } else {
                                    // Todo: Better exception handling...
                                    getReceiver().literal(key, "");
                                }
                                literal.setLength(0);
                                if (!inArray) {
                                    key = "";
                                }
                                inSpecialLiteral = false;
                                break;
                            case ESCAPECHAR:
                                ignoreNextChar = true;
                            default:
                                literal.append(c);
                        }
                    } else {
                        switch (c) {
                            case STARTOBJECT:
                                if (objectLevel == 0) {
                                    getReceiver().startRecord("");
                                } else {
                                    getReceiver().startEntity(key);
                                    key = "";
                                }
                                objectLevel++;
                                break;
                            case ENDOBJECT:
                                if (objectLevel > 1) {
                                    getReceiver().endEntity();
                                } else {
                                    getReceiver().endRecord();
                                }
                                objectLevel--;
                                break;
                            case STARTARRAY:
                                inArray = true;
                                break;
                            case ENDARRAY:
                                inArray = false;
                                key = "";
                                break;
                            case STARTLITERAL:
                                inLiteral = true;
                                break;
                            case ESCAPECHAR:
                                ignoreNextChar = true;
                                break;
                            case SEPOBJECTS:
                            case SEPLITERALS:
                            case WHITESPACE:
                                break;
                            default:
                                literal.append(c);
                                inSpecialLiteral = true;
                        }
                    }
                }
            }
        }
    }

    private class IllegalCharacterException extends Exception {

    }


}

