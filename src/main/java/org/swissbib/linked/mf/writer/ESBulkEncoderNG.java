package org.swissbib.linked.mf.writer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.util.ArrayList;
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

    protected static final char COLON 	     = ':'; // Colon transition (e.g. between parent and value)
    protected static final char COMMA 	     = ','; // Comma transition (e.g. between value and parent)
    protected static final String NONE 	     = "";	// No transition
    protected static final byte ROOT_OBJECT	 = 0;	// Dummy "parent" for root object
    protected static final byte START_OBJECT = 1;	// Equals literal '{'
    protected static final byte START_ARRAY  = 2;	// Equals literal '['
    protected static final byte END_OBJECT 	 = 3;	// Equals literal '}'
    protected static final byte END_ARRAY 	 = 4;	// Equals literal ']'
    protected static final byte KEY 	     = 5;	// Equals literal ""<Name>""
    protected static final byte VALUE 	     = 6;	// Equals literal ""<Name""

    List<JsonToken> ctx; 		// List of keys to which content to be working on is attributed (Problem of two concurrent merging processes, of which one is embedded in the other
    Multimap<JsonToken, JsonToken> ctxRegistry;	// Registers root parent of every parent
    List<JsonToken> jsonList;	// Sequential list of json-tokens
    int curIndex;   // Current index in jsonList
    int id;         // Id for the next built JsonToken object
    boolean skipToken;  // Should the following token (i.e. object or array) be skipped?
    boolean higherCtx;  // Next parent adds itself as new element in List ctx (i.e. no replacement of last registered parent)


    /**
     * Contains information on a Json token (e.g. parent) and methods to query, modify and serialize it.
     */
    private class JsonToken {

        byte type;		    // Type of the token
        String name;		// Name (only if type == parent / value, else null)
        JsonToken lastElem;	// Last element belonging to the parent (parent only, else null)
        JsonToken key;		// Key which token belongs to (for parent: root parent)
        int id;             // Id of the token


        /**
         * Constructor for class JsonToken
         * @param id Id of token
         * @param type Type of token (one of ROOT_NODE, OBJECT, ARRAY, END_OBJECT, END_ARRAY, KEY, VALUE)
         * @param name Name of token (only if type == KEY or VALUE, else null)
         * @param lastElem Last element belonging to a parent (for keys only, else null)
         * @param key Refers to parent id of which a token belongs to (parent refer to their root parent)
         */
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
         * Gets last element which still belongs to parent
         * @return Last element
         */
        JsonToken getLastElem() {
            return lastElem;
        }

        /**
         * Get root parent
         * @return Root parent
         */
        JsonToken getKey() {
            return key;
        }

        /**
         * Sets last element which still belongs to parent
         * @param jt Respective JSON token
         */
        void setLastElem(JsonToken jt) {
            lastElem = jt;
        }

        /**
         * Set this Json token as last element of root parent
         */
        void setAsLastElem() {
            if (key != null) key.setLastElem(this);
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
        //(Re)set global variables
        ctx = new ArrayList<>();
        ctxRegistry = HashMultimap.create();
        jsonList = new ArrayList<>();
        curIndex = 0;
        id = 0;
        skipToken = false;
        higherCtx = false;

        buildRootObject();
        buildStartObject();
        //startEntity(identifier);
    }

    @Override
    public void endRecord() {
        buildEndObject();
        getReceiver().process(buildJsonString());
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
        buildValue(value);
    }

    /**
     * Creates a new object token
     */
    void buildStartObject() {
        if (!skipToken) {
            JsonToken jt = new JsonToken(++id, START_OBJECT, null, null, ctx.get(ctx.size() - 1));
            jsonList.add(curIndex, jt);
            higherCtx = true;
            jt.setAsLastElem();
            curIndex++;
        }
    }

    /**
     * Creates a new array token
     */
    void buildStartArray() {
        JsonToken jt = new JsonToken(++id, START_ARRAY, null, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        curIndex++;
    }

    /**
     * Closes an object token
     */
    void buildEndObject() {
        if (skipToken) {
            skipToken = false;
        } else {
            JsonToken jt = (ctx.size() > 1) ?
                    new JsonToken(++id, END_OBJECT, null, null, ctx.get(ctx.size() - 2)) :
                    new JsonToken(++id, END_OBJECT, null, null, ctx.get(ctx.size() - 1));
            jsonList.add(curIndex, jt);
            jt.setAsLastElem();
            curIndex++;
        }
        ctx.remove(ctx.size() -1);
    }

    /**
     * Closes an array token
     */
    void buildEndArray() {
        skipToken = false;
        JsonToken jt = new JsonToken(++id, END_ARRAY, null, null, ctx.get(ctx.size() - 1));
        jsonList.add(curIndex, jt);
        jt.setAsLastElem();
        curIndex++;
    }

    /**
     * Creates a new parent token
     * @param name Name of parent
     */
    void buildKey(String name) {
        skipToken = false;
        // If new (i.e. higher) context, get last parent token in ctx else second last
        JsonToken identicalKey = higherCtx ?
                checkKeyExists(ctx.get(ctx.size() - 1), name) :
                checkKeyExists(ctx.get(ctx.size() - 2), name);
        if (identicalKey != null) {
            // TODO Check if index is correct!
            if (higherCtx) {
                ctx.add(identicalKey);
                higherCtx = false;
            } else {
                ctx.set(ctx.size() - 1, identicalKey);
            }
            // Depending on what the identical parent refers to (value, array or object),
            // additional steps are required (e.g. create new array or skip the following opening curly brace)
            switch (identicalKey.getLastElem().getType()) {
                // Merging of two objects
                case END_OBJECT:
                    curIndex = identicalKey.getLastElem().getIndex();
                    skipToken = true;
                    higherCtx = true;
                    break;
                // Add value to existing array
                case END_ARRAY:
                    curIndex = identicalKey.getLastElem().getIndex();
                    break;
                // Merge two parent-value-pairs to an array
                case VALUE:
                    // Set start array token
                    curIndex = identicalKey.getIndex() + 1;
                    buildStartArray();
                    // Set end array token
                    curIndex = identicalKey.getLastElem().getIndex() + 1;
                    buildEndArray();
                    curIndex--;
                    break;
            }
            skipToken = true;
        } else {
            JsonToken jt = higherCtx ?
                    new JsonToken(++id, KEY, name, null, ctx.get(ctx.size() - 1)) :
                    new JsonToken(++id, KEY, name, null, ctx.get(ctx.size() - 2));
            if (higherCtx) {
                ctx.add(jt);
                higherCtx = false;
            } else {
                // TODO Check if index is correct!
                ctx.set(ctx.size() - 1, jt);
            }
            jsonList.add(curIndex, jt);
            ctxRegistry.put(jt.getKey(), jt);
            jt.setAsLastElem();
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
        jt.setAsLastElem();
        curIndex++;
    }

    /**
     * Creates a root object token ("dummy parent")
     */
    void buildRootObject() {
        JsonToken jt = new JsonToken(++id, ROOT_OBJECT, null, null, null);
        ctx.add(jt);
        jt.setLastElem(jt);
        jsonList.add(curIndex, jt);
        jt.setAsLastElem();
        curIndex++;
    }

    /**
     * Checks if a parent with same name and same root parent (context) exists
     * @param rootKey Reference to root parent
     * @param name Name of parent
     * @return JsonToken Root parent, if a parent has been found, otherwise null
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
