package org.swissbib.linked.mf.writer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.util.List;


/**
 * Serialises an object as JSON-LD.
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 *
 */
@Description("Serialises an object as JSON-LD")
@In(StreamReceiver.class)
@Out(String.class)
public final class ESBulkEncoderNG extends DefaultStreamPipe<ObjectReceiver<String>> {

    protected static final char COLON 	     = ':'; // Colon transition (e.g. between key and value)
    protected static final char COMMA 	     = ','; // Comma transition (e.g. between value and key)
    protected static final String NONE 	     = "";	// No transition
    protected static final byte ROOT_OBJECT	 = 0;	// Dummy "key" for root object
    protected static final byte START_OBJECT = 1;	// Equals literal '{'
    protected static final byte START_ARRAY  = 2;	// Equals literal '['
    protected static final byte END_OBJECT 	 = 3;	// Equals literal '}'
    protected static final byte END_ARRAY 	 = 4;	// Equals literal ']'
    protected static final byte KEY 	     = 5;	// Equals literal ""<Name>""
    protected static final byte VALUE 	     = 6;	// Equals literal ""<Name""

    List<JsonToken> ctx; 		// List of keys to which content to be working on is attributed (Problem of two concurrent merging processes, of which one is embedded in the other
    Multimap<JsonToken, JsonToken> ctxRegistry = HashMultimap.create();	// Registers root key of every key
    List<JsonToken> jsonList;	// Sequential list of json-tokens
    int curIndex = 0;   // Current index in jsonList
    int id = 0;
    boolean skipToken = false;  // Should the following token (i.e. object or array) be skipped?
    boolean higherCtx = false;  // Next key adds itself as new element in List ctx (i.e. no replacement of last registered key)


    private class JsonToken {

        byte type;		    // Type of the token
        String name;		// Name (only if type == key / value, else null)
        JsonToken lastElem;	// Last element belonging to the key (key only, else null)
        JsonToken key;		// Key which token belongs to (for key: root key)
        int id;


        JsonToken(int id, byte type, String name, JsonToken lastElem, JsonToken key) {
            this.id = id;
            this.type = type;
            this.name = name;
            this.lastElem = lastElem;
            this.key = key;
        }

        /**
         * Prints respective literal
         * @return Literal
         */
        @Override
        public String toString() {
            switch (this.getType()) {
                case ROOT_OBJECT:
                    return NONE;
                case START_OBJECT:
                    return "{";
                case START_ARRAY:
                    return "[";
                case END_OBJECT:
                    return "}";
                case END_ARRAY:
                    return "]";
                case KEY:
                    return "\"" + this.getName() + "\"";
                case VALUE:
                    return "\"" + this.getName() + "\"";
                default:
                    return NONE;
            }
        }

        /**
         * Gets index of this instance of JsonToken in list jsonList
         * @return index
         */
        int getIndex() {
            int index = 0;
            for (int i = 0; i < jsonList.size(); i++) {
                if (jsonList.get(i).getId() == id) {
                    index = i;
                    break;
                }
            }
            return index;
        }

        /**
         * Gets id of this JSON token
         * @return id
         */
        int getId() {
            return id;
        }

        /**
         * Gets name of this JSON token
         */
        String getName() {
            return name;
        }

        /**
         * Gets last element which still belongs to key
         * @return Last element
         */
        JsonToken getLastElem() {
            return lastElem;
        }

        /**
         * Sets last element which still belongs to key
         * @param jt Respective JSON token
         */
        void setLastElem(JsonToken jt) {
            lastElem = jt;
        }

        /**
         * Gets type of JSON token
         * @return Type of JSON token
         */
        byte getType() {
            return type;
        }

    }


    @Override
    public void startRecord(String identifier) {
        buildRootObject();
        startEntity(identifier);
    }

    @Override
    public void endRecord() {
        String jsonString = buildJsonString();
    }

    @Override
    public void startEntity(String name) {
        buildKey(name);
        buildStartObject();
    }

    @Override
    public void endEntity() {
        buildEndObject();
    }

    @Override
    public void literal(String name, String value) {
        buildKey(name);
        buildValue(name);
    }

    /**
     * Creates a new object token
     */
    void buildStartObject() {
        if (!skipToken) {
            JsonToken jt = new JsonToken(++id, START_OBJECT, null, null, ctx.get(ctx.size() - 1));
            jsonList.add(curIndex, jt);
            ctx.get(ctx.size() - 1).setLastElem(jt);
            higherCtx = true;
            curIndex++;
        }
    }

    /**
     * Creates a new array token
     */
    void buildStartArray() {
        JsonToken jt = new JsonToken(++id, START_ARRAY, null, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        ctx.get(ctx.size() - 1).setLastElem(jt);
        curIndex++;
    }

    /**
     * Closes an object token
     */
    void buildEndObject() {
        // TODO: Do not write token if respective key has been merged
        skipToken = false;
        JsonToken jt = new JsonToken(++id, END_OBJECT, null, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        ctx.get(ctx.size() - 1).setLastElem(jt);
        curIndex++;
    }

    /**
     * Closes an array token
     */
    void buildEndArray() {
        // TODO: Write token if two key/value pairs have been merged
        skipToken = false;
        JsonToken jt = new JsonToken(++id, END_ARRAY, null, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        ctx.get(ctx.size() - 1).setLastElem(jt);
        curIndex++;
    }

    /**
     * Creates a new key token
     * @param name Name of key
     */
    void buildKey(String name) {
        skipToken = false;
        JsonToken identicalKey = checkKeyExists(ctx.get(ctx.size() - 1), name);
        if (identicalKey != null) {
            // TODO Check if index is correct!
            ctx.add(ctx.size() - 2, identicalKey);
            // TODO Check if index is correct!
            curIndex = identicalKey.getLastElem().getIndex();
            // Depending on what the identical key refers to (value, array or object),
            // additional steps are required (e.g. create new array or skip the following opening curly brace)
            switch (identicalKey.getLastElem().getType()) {
                case END_OBJECT:
                    // TODO Merge two objects
                    break;
                case END_ARRAY:
                    // TODO Add value to existing array
                    break;
                case VALUE:
                    // TODO Merge two values into array
                    break;
            }
            skipToken = true;
        } else {
            JsonToken jt = new JsonToken(++id, KEY, name, null, ctx.get(ctx.size() - 1));
            if (higherCtx) {
                ctx.add(jt);
                higherCtx = false;
            } else {
                // TODO Check if index is correct!
                ctx.add(ctx.size() - 2, jt);
            }
            jsonList.add(curIndex, jt);
            ctx.get(ctx.size() - 1).setLastElem(jt);
            curIndex++;
        }
    }

    /**
     * Creates a new value token
     * @param name Name of value
     */
    void buildValue(String name) {
        skipToken = false;
        JsonToken jt = new JsonToken(++id, VALUE, name, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        ctx.get(ctx.size() - 1).setLastElem(jt);
        curIndex++;
    }

    /**
     * Creates a root object token ("dummy key")
     */
    void buildRootObject() {
        // TODO Set ctx
        JsonToken jt = new JsonToken(++id, ROOT_OBJECT, null, null, null);
        jsonList.add(curIndex, jt);
        ctx.get(ctx.size() - 1).setLastElem(jt);
        curIndex++;
    }

    /**
     * Checks if a key with same name and same root key (context) exists
     * @param rootKey Reference to root key
     * @param name Name of key
     * @return JsonToken Root key, if a key has been found, otherwise null
     */
    JsonToken checkKeyExists(JsonToken rootKey, String name) {
        JsonToken foundKey = null;
        if (ctxRegistry.containsKey(rootKey)) {
            for (JsonToken jt: ctxRegistry.get(rootKey)) {
                if (jt.getName().equals(name)) foundKey = jt;
            }
        }
        return foundKey;
    }

    /**
     * Builds JSON string
     * @return JSON string
     */
    String buildJsonString() {
        StringBuilder stringBuilder = new StringBuilder();
        byte lastTokenType = -1;
        for (JsonToken jt: jsonList) {
            if (lastTokenType == -1 || lastTokenType == ROOT_OBJECT ||
                    lastTokenType == START_OBJECT || lastTokenType == START_ARRAY) {
                stringBuilder.append(NONE);
            } else if (lastTokenType == END_OBJECT || lastTokenType == END_ARRAY) {
                stringBuilder.append((jt.getType() == KEY) ? COMMA : NONE);
            } else if (lastTokenType == KEY) {
                stringBuilder.append(COLON);
            } else if (lastTokenType == VALUE) {
                stringBuilder.append((jt.getType() == KEY || jt.getType() == VALUE) ? COMMA : NONE);
            }
            stringBuilder.append(jt.toString());
            lastTokenType = jt.getType();
        }
        return stringBuilder.toString();
    }
}
