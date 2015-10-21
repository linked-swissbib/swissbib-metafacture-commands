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
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

/**
 * Serialises an object as JSON-LD.
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 *
 */
@Description("Serialises an object as JSON-LD")
@In(StreamReceiver.class)
@Out(String.class)
public final class ESBulkEncoderNG2 extends DefaultStreamPipe<ObjectReceiver<String>> {

    protected static final char COMMA 	    = ',';      // Comma transition (e.g. between value and parent)
    protected static final String NONE 	    = "";	    // No transition
    protected static final byte ROOT_NODE   = 0;	    // Root node
    protected static final byte OBJECT      = 1;
    protected static final byte ARRAY       = 2;
    protected static final byte KEY 	    = 3;	    // Equals literal ""<Name>""
    protected static final byte VALUE 	    = 4;	    // Equals literal ""<Name>""

    Multimap<JsonToken, JsonToken> ctxRegistry;         // Registers root parent of every parent
    boolean makeChildNode;                              // Set next key as child node of current node
    JsonToken node;                                     // Current node
    JsonToken rootNode;                                 // Current root node

    boolean multipleObjects                 = false;    // Multiple objects in one record
    boolean header                          = true;     // Should bulk-header be printed?
    String id;                                          // Id of record
    //String idKeyName;                                   // Name of id key
    String type;                                        // Type of record
    String index;                                       // Index of record


    /**
     * Contains information on a Json token (e.g. parent) and methods to query, modify and serialize it.
     */
    private class JsonToken {

        byte type;		    // Type of the token
        String name;		// Name (only if type == parent / value, else null)
        List<JsonToken> children = new ArrayList<>();	// Last element belonging to the parent (parent only, else null)
        JsonToken parent;		// Key which token belongs to (for parent: root parent)
        byte parentheses = -1;  // Parentheses which surrounds descendants (none, brackets or braces)


        /**
         * Constructor for class JsonToken
         * @param type Type of token (one of ROOT_NODE, OBJECT, ARRAY, END_OBJECT, END_ARRAY, KEY, VALUE)
         * @param name Name of token (only if type == KEY or VALUE, else null)
         * @param parent Refers to parent id of which a token belongs to (parent refer to their root parent)
         */
        JsonToken(byte type, String name, JsonToken parent) {
            this.type = type;
            this.name = name;
            this.parent = parent;
            if (parent != null) parent.setChildren(this);
        }

        /**
         * Prints respective literal
         * @return Literal
         */
        @Override
        public String toString() {
            switch (this.getType()) {
                case ROOT_NODE:
                    return NONE;
                case KEY:
                    return "\"" + this.getName() + "\"";
                case VALUE:
                    return "\"" + this.getName() + "\"";
                default:
                    return NONE;
            }
        }

        /**
         * Gets name of this JSON token
         */
        String getName() {
            return name;
        }

        /**
         * Gets descendants of node
         * @return Last element
         */
        List<JsonToken> getChildren() {
            return children;
        }

        /**
         * Gets parent node
         * @return Parent node
         */
        JsonToken getParent() {
            return parent;
        }

        /**
         * Adds a new descendant to node and probably sets new value for variable parentheses
         * @param jt Respective JSON token
         */
        void setChildren(JsonToken jt) {
            if (jt.getType() == KEY) parentheses = OBJECT;
            if (parentheses == 0 && jt.getType() == VALUE) parentheses = ARRAY;
            if (parentheses == -1 && jt.getType() == VALUE) parentheses = 0;
            children.add(jt);
        }

        /**
         * Gets type of JSON token (ROOT_NODE, KEY, VALUE)
         * @return Type of JSON token
         */
        byte getType() {
            return type;
        }

        /**
         * Gets parentheses category (none, brackets or braces)
         * @return Parantheses type (in byte)
         */
        public byte getParentheses() {
            return parentheses;
        }

    }

    /**
     * Are there more than one possible objects in one record?
     * @param multipleObjects true, false
     */
    public void setMultipleObjects(String multipleObjects) {
        this.multipleObjects = Boolean.parseBoolean(multipleObjects);
    }

    /**
     * Should header be created?
     * @param header true, false
     */
    public void setHeader(String header) {
        this.header = Boolean.parseBoolean(header);
    }

    /**
     * Sets index of record
     * @param index Index of record
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * Sets type of record
     * @param type Type of record
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Sets id key name
     * //@param idKeyName Name of key where value is stored
     */
/*    public void setIdKeyName(String idKeyName) {
        this.idKeyName = idKeyName;
    }*/

    @Override
    public void startRecord(String identifier) {
        ctxRegistry = HashMultimap.create();
        node = new JsonToken(ROOT_NODE, null, null);
        rootNode = node;
        makeChildNode = true;
        id = identifier;
    }

    @Override
    public void endRecord() {
        if (!multipleObjects) flushObject();
    }

    @Override
    public void startEntity(String name) {
        buildKey(name);
        makeChildNode = true;
    }

    @Override
    public void endEntity() {
        node = node.getParent();
        if (multipleObjects && node.getParent().getType() == ROOT_NODE) {
            flushObject();
            startRecord(null);
        }
    }

    @Override
    public void literal(String name, String value) {
        buildKey(name);
        new JsonToken(VALUE, value, node);
        // setId(node.getParent(), name, value);
    }

    /**
     * Creates a new parent token
     * @param name Name of parent
     */
    void buildKey(String name) {
        JsonToken sameNode = checkKeyExists(getParentNode(), name);
        node = (sameNode == null) ?
                new JsonToken(KEY, name, getParentNode()) :
                sameNode;
        ctxRegistry.put(node.getParent(), node);
        makeChildNode = false;
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
     * Gets parent node
     * @return Parent node
     */
    JsonToken getParentNode() {
        return makeChildNode ? node : node.getParent();
    }

    /**
     * Builds JSON string
     * @return JSON string
     */
    String buildJsonString(byte lastTokenType, JsonToken jt) {
        StringBuilder stringBuilder = new StringBuilder();
        if (jt.getType() == ROOT_NODE) {
            stringBuilder.append(generateHeader());
            stringBuilder.append("{");
        }
        for (JsonToken child: jt.getChildren()) {
            // Set prefixes if required
            if (lastTokenType == OBJECT || lastTokenType == ARRAY) {
                stringBuilder.append((child.getType() == KEY) ? COMMA : NONE);
            } else if (lastTokenType == VALUE) {
                stringBuilder.append((child.getType() == KEY || child.getType() == VALUE) ? COMMA : NONE);
            }
            // Set name of key / value
            stringBuilder.append(child.toString());
            // Descend to child nodes if present
            if (child.getType() == KEY) {
                switch (child.getParentheses()) {
                    case 0:
                        stringBuilder.append(":");
                        stringBuilder.append(buildJsonString((byte)0, child));
                        lastTokenType = VALUE;
                        break;
                    case OBJECT:
                        stringBuilder.append(":{");
                        stringBuilder.append(buildJsonString((byte)0, child));
                        stringBuilder.append("}");
                        lastTokenType = OBJECT;
                        break;
                    case ARRAY:
                        stringBuilder.append(":[");
                        stringBuilder.append(buildJsonString((byte)0, child));
                        stringBuilder.append("]");
                        lastTokenType = ARRAY;
                        break;
                }
            } else {
                lastTokenType = VALUE;
            }
        }
        if (jt.getType() == ROOT_NODE) stringBuilder.append("}\n");
        return stringBuilder.toString();
    }

    /**
     * Extracts id from indicated value
     * @param parentNode Parent node of current node
     * @param name Name of key
     * @param value Name of value
     */
/*    void setId(JsonToken parentNode, String name, String value) {
        if (name.equals(idKeyName) && parentNode.getParent().getType() == ROOT_NODE) {
           id = value;
        }
    }*/

    /**
     * Generates header line if header == true
     * @return Header line
     */
    String generateHeader() {
        return (header) ?
                "{\"index\":{\"_type\":\"" + type + "\",\"_index\":\"" + index + "\",\"_id\":\"" + id + "\"}}\n" :
                "";
    }

    /**
     * Flushes content to receiver
     */
    void flushObject() {
        getReceiver().process(buildJsonString((byte) -1, rootNode));
    }

}
