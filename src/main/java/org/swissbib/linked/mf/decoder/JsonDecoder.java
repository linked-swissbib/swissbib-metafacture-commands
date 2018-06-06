package org.swissbib.linked.mf.decoder;

import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultObjectPipe;

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
    private static final char STARTSTRINGVALUE = '"';
    private static final char ENDSTRINGVALUE = '"';
    private static final char KEYVALUESEPARATOR = ':';
    private static final char ELEMENTSEPARATOR = ',';
    private static final char WHITESPACE = ' ';
    private static final char ESCAPESPECIALCHAR = '\'';
    private static final byte OBJECT = 0;
    private static final byte ARRAY = 1;
    private static final byte STRINGVALUE = 2;
    private static final byte NONSTRINGVALUE = 3;
    private static final StringBuilder charCollector = new StringBuilder();
    private String nullValue = "";
    private boolean escapeSpecialChar = false;
    private final Path path = new Path();
    private String key = "";

    private static boolean charNotInArray(char character, char[] array) {
        for (char elem : array) {
            if (elem == character) {
                return false;
            }
        }
        return true;
    }

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    @Override
    public void process(String obj) {

        for (int i = 0; i < obj.length(); i++) {
            char c = obj.charAt(i);
            if (path.empty()) {
                if (c == STARTOBJECT) {
                    path.add(new JsonObject(""));
                    getReceiver().startRecord("");
                }
            } else {
                if (escapeSpecialChar) {
                    charCollector.append(c);
                    escapeSpecialChar = false;
                } else {
                    if (path.insideStringValue()) {
                        switch (c) {
                            case ESCAPESPECIALCHAR:
                                escapeSpecialChar = true;
                                break;
                            case ENDSTRINGVALUE:
                                path.remove();
                                if (key.length() > 0) {
                                    getReceiver().literal(key, charCollector.toString());
                                    if (!path.array()) key = "";
                                } else {
                                    key = charCollector.toString();
                                }
                                charCollector.setLength(0);
                                break;
                            default:
                                charCollector.append(c);
                        }
                    } else if (path.insideNonStringValue()) {
                        switch (c) {
                            case ELEMENTSEPARATOR:
                            case ENDOBJECT:
                            case ENDARRAY:
                            case WHITESPACE:
                                if (valueIsNull()) {
                                    getReceiver().literal(key, nullValue);
                                } else if (valueIsBoolean() || valueIsNumber()) {
                                    getReceiver().literal(key, charCollector.toString());
                                } else {
                                    // Todo: Better exception handling...
                                    getReceiver().literal(key, "");
                                }
                                charCollector.setLength(0);
                                path.remove();
                                if (!path.array()) {
                                    key = "";
                                }
                                if (path.empty() || path.onRootLevel()) {
                                    getReceiver().endRecord();
                                }
                                if (c == ENDARRAY) {
                                    key = "";
                                    path.remove();
                                }
                                break;
                            case ESCAPESPECIALCHAR:
                                escapeSpecialChar = true;
                            default:
                                charCollector.append(c);
                        }
                    } else {  // Not inside value
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
                                if (path.aboveRootLevel()) {
                                    getReceiver().endEntity();
                                } else {
                                    getReceiver().endRecord();
                                }
                                path.remove();
                                if (path.array()) {
                                    key = path.last().key;
                                }
                                break;
                            case STARTARRAY:
                                path.add(new JsonArray(key));
                                break;
                            case ENDARRAY:
                                path.remove();
                                key = "";
                                break;
                            case STARTSTRINGVALUE:
                                path.add(new JsonLiteral());
                                break;
                            case ESCAPESPECIALCHAR:
                                escapeSpecialChar = true;
                                break;
                            case ELEMENTSEPARATOR:
                            case KEYVALUESEPARATOR:
                            case WHITESPACE:
                                break;
                            default:
                                charCollector.append(c);
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
            return STRINGVALUE;
        }

        @Override
        String key() {
            return "";
        }
    }

    private class JsonSpecialLiteral extends JsonToken {
        @Override
        byte type() {
            return NONSTRINGVALUE;
        }

        @Override
        String key() {
            return "";
        }
    }

    private class Path {
        private final List<JsonToken> path = new ArrayList<>();

        boolean empty() {
            return path.size() == 0;
        }

        boolean onRootLevel() { return path.size() == 1; }

        boolean aboveRootLevel() { return path.size() > 1;}

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

        boolean insideStringValue() {
            return !empty() && last().type() == STRINGVALUE;
        }

        boolean insideNonStringValue() {
            return !empty() && last().type() == NONSTRINGVALUE;
        }

        private int indexLast() {
            return path.size() - 1;
        }
    }

    private boolean valueIsNumber() {
        String value = charCollector.toString();
        boolean valid = true;
        boolean followedBySign = false;
        char[] validFirstChars = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        char[] validMiddleChars = {'-', '+', 'e', 'E', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        char[] validLastChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        if (charNotInArray(value.charAt(0), validFirstChars)) {
            valid = false;
        }
        for (int i = 1; i < value.length() - 1; i++) {
            if (charNotInArray(value.charAt(i), validMiddleChars)) {
                valid = false;
                break;
            } else if (value.charAt(i) == 'e' || value.charAt(i) == 'E') {
                followedBySign = true;
            } else if (followedBySign) {
                if (!(value.charAt(i) == '-' || value.charAt(i) == '+')) { // e not followed by - or +
                    valid = false;
                } else {
                    followedBySign = false;
                }
            }
        }
        if (charNotInArray(value.charAt(value.length() - 1), validLastChars)) {
            valid = false;
        }
        return valid;
    }

    private boolean valueIsNull() {
       return charCollector.toString().equals("null");
    }

    private boolean valueIsBoolean() {
        return charCollector.toString().equals("true") ||
                                        charCollector.toString().equals("false");
    }

}

