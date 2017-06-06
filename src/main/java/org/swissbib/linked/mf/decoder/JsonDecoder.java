package org.swissbib.linked.mf.decoder;

import org.culturegraph.mf.framework.FluxCommand;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;

import java.util.ArrayList;
import java.util.List;

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
@FluxCommand("decode-json")
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
    private static final byte OBJECT = 0;
    private static final byte ARRAY = 1;
    private static final byte LITERAL = 2;
    private static final byte SPECIALLITERAL = 3;
    private static StringBuilder literal = new StringBuilder();
    private String nullValue = "";
    private boolean ignoreNextChar = false;
    private Path path = new Path();
    private String key = "";

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

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    @Override
    public void process(String obj) {

        for (int i = 0; i < obj.length(); i++) {
            char c = obj.charAt(i);
            if (path.empty()) {
                if (c == '{') {
                    path.add(new JsonObject(""));
                    getReceiver().startRecord("");
                }
            } else {
                if (ignoreNextChar) {
                    literal.append(c);
                    ignoreNextChar = false;
                } else {
                    if (path.literal()) {
                        switch (c) {
                            case ESCAPECHAR:
                                ignoreNextChar = true;
                                break;
                            case ENDLITERAL:
                                path.remove();
                                // Is value, else key
                                if (key.length() > 0) {
                                    getReceiver().literal(key, literal.toString());
                                    key = "";
                                } else {
                                    key = literal.toString();
                                }
                                literal.setLength(0);
                                break;
                            default:
                                literal.append(c);
                        }
                    } else if (path.specialLiteral()) { // If value is not delimited by apostrophes
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
                                if (!path.array()) {
                                    key = "";
                                }
                                path.remove();
                                break;
                            case ESCAPECHAR:
                                ignoreNextChar = true;
                            default:
                                literal.append(c);
                        }
                    } else {
                        switch (c) {
                            case STARTOBJECT:
                                if (path.empty()) {
                                    getReceiver().startRecord("");
                                } else {
                                    if (path.array()) { // In case objects are in an array
                                        key = path.last().key();
                                    }
                                    getReceiver().startEntity(key);
                                }
                                path.add(new JsonObject(key));
                                key = "";
                                break;
                            case ENDOBJECT:
                                if (path.size() > 1) {
                                    getReceiver().endEntity();
                                } else {
                                    getReceiver().endRecord();
                                }
                                path.remove();
                                break;
                            case STARTARRAY:
                                path.add(new JsonArray(key));
                                break;
                            case ENDARRAY:
                                path.remove();
                                key = "";
                                break;
                            case STARTLITERAL:
                                path.add(new JsonLiteral());
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
                                path.add(new JsonSpecialLiteral());
                        }
                    }
                }
            }
        }
    }

    abstract private class JsonToken {
        String key;

        abstract byte type();

        abstract String key();
    }

    private class JsonArray extends JsonToken {
        JsonArray(String name) {
            this.key = name;
        }

        @Override
        byte type() {
            return ARRAY;
        }

        @Override
        String key() {
            return this.key;
        }
    }

    private class JsonObject extends JsonToken {
        JsonObject(String name) {
            this.key = name;
        }

        @Override
        byte type() {
            return OBJECT;
        }

        @Override
        String key() {
            return this.key;
        }
    }

    private class JsonLiteral extends JsonToken {
        @Override
        byte type() {
            return LITERAL;
        }

        @Override
        String key() {
            return "";
        }
    }

    private class JsonSpecialLiteral extends JsonToken {
        @Override
        byte type() {
            return SPECIALLITERAL;
        }

        @Override
        String key() {
            return "";
        }
    }

    private class Path {
        private List<JsonToken> path = new ArrayList<>();

        boolean empty() {
            return path.size() == 0;
        }

        void add(JsonToken token) {
            path.add(token);
        }

        void remove() {
            path.remove(indexLast());
        }

        private JsonToken last() {
            return path.get(indexLast());
        }

        int size() {
            return path.size();
        }

        boolean array() {
            return !empty() && last().type() == ARRAY;
        }

        boolean literal() {
            return !empty() && last().type() == LITERAL;
        }

        boolean specialLiteral() {
            return !empty() && last().type() == SPECIALLITERAL;
        }

        private int indexLast() {
            return path.size() - 1;
        }
    }

}

