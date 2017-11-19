package org.swissbib.linked.mf.pipe;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Serialises an object as JSON-LD.
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Serialises an object as JSON-LD")
@In(StreamReceiver.class)
@Out(String.class)
public final class ESBulkEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {

    private static final char COMMA = ',';      // Comma transition (e.g. between value and parent)
    private static final String NONE = "";        // No transition
    private static final byte BNODE = 0;        // Node without key
    private static final byte OBJECT = 1;
    private static final byte ARRAY = 2;
    private static final byte KEY = 3;        // Equals literal ""<Name>""
    private static final byte VALUE = 4;        // Equals literal ""<Name>""
    private final static Logger LOG = LoggerFactory.getLogger(ESBulkEncoder.class);
    private Multimap<JsonToken, JsonToken> ctxRegistry;         // Registers root parent of every parent
    private boolean makeChildNode;                              // Set next key as child node of current node
    private JsonToken node;                                     // Current node
    private JsonToken rootNode;                                 // Current root node
    private String outputString = "";

    private boolean header = true;     // Should bulk-header be printed?
    private boolean escapeChars = true;     // Should prohibited characters in JSON string be escaped?
    private String type;                                        // Type of record
    private String index;                                       // Index of record
    private boolean avoidMergers = false;

    /**
     * Should header be created?
     *
     * @param header true, false
     */
    public void setHeader(String header) {
        this.header = Boolean.parseBoolean(header);
        LOG.debug("Settings - Write header: {}", header);
    }

    /**
     * Escape prohibited characters in JSON strings
     *
     * @param escapeChars true, false
     */
    public void setEscapeChars(String escapeChars) {
        this.escapeChars = Boolean.parseBoolean(escapeChars);
        LOG.debug("Settings - Escape problematic characters: {}", escapeChars);
    }

    /**
     * Sets index of record
     *
     * @param index Index of record
     */
    public void setIndex(String index) {
        this.index = index;
        LOG.debug("Settings - Set index name: {}", index);
    }

    /**
     * Sets type of record
     *
     * @param type Type of record
     */
    public void setType(String type) {
        this.type = type;
        LOG.debug("Settings - Set type: {}", type);
    }

    public void setAvoidMergers(String avoidMergers) {
        this.avoidMergers = Boolean.parseBoolean(avoidMergers);
    }

    @Override
    public void startRecord(String id) {
        LOG.debug("Parsing record {}", id);
        ctxRegistry = HashMultimap.create();
        node = new JsonToken(BNODE, null, null);
        rootNode = node;
        makeChildNode = true;
        outputString = (header) ? "{\"index\":{\"_type\":\"" + type + "\",\"_index\":\"" +
                index + "\",\"_id\":\"" + id + "\"}}\n" :
                "";
    }

    @Override
    public void endRecord() {
        if (rootNode != null) {
            LOG.debug("Serializing record to JSON-LD");
            outputString += "{" + buildJsonString((byte) -1, rootNode) + "}\n";
            LOG.trace("Sending record to {}", getReceiver().getClass());
            getReceiver().process(outputString);
        }
    }

    @Override
    public void startEntity(String name) {
        buildKey(name);
        makeChildNode = true;
        if (name.endsWith("{}") || avoidMergers) node = new JsonToken(BNODE, null, getParentNode());
    }

    @Override
    public void endEntity() {
        if (node.getChildren().size() == 0) {
            new JsonToken(VALUE, "", node);
            LOG.debug("Entity {} does not have a child node. Creating one with empty name (preventing key with no value.",
                    node.getName());
        }
        node = node.getParent();
        if (node.getType() == BNODE) node = node.getParent();
    }

    @Override
    public void literal(String name, String value) {
        buildKey(name);
        new JsonToken(VALUE, value, node);
    }

    /**
     * Creates a new parent token
     *
     * @param name Name of parent
     */
    private void buildKey(String name) {
        JsonToken sameNode = checkKeyExists(getParentNode(), name);
        node = (sameNode == null) ?
                new JsonToken(KEY, name, getParentNode()) :
                sameNode;
        ctxRegistry.put(node.getParent(), node);
        makeChildNode = false;
    }

    /**
     * Checks if a parent with same name and same root parent (context) exists
     *
     * @param rootKey Reference to root parent
     * @param name    Name of parent
     * @return JsonToken Root parent, if a parent has been found, otherwise null
     */
    private JsonToken checkKeyExists(JsonToken rootKey, String name) {
        if (name.endsWith("{}")) name = name.substring(0, name.length() - 2);
        JsonToken foundKey = null;
        if (ctxRegistry.containsKey(rootKey)) {
            for (JsonToken jt : ctxRegistry.get(rootKey)) {
                if (jt.getName().equals(name)) {
                    LOG.trace("Merging key {}", name);
                    foundKey = jt;
                }
            }
        }
        return foundKey;
    }

    /**
     * Gets parent node
     *
     * @return Parent node
     */
    private JsonToken getParentNode() {
        return makeChildNode ? node : node.getParent();
    }

    /**
     * Builds JSON string
     *
     * @return JSON string
     */
    private String buildJsonString(byte lastTokenType, JsonToken jt) {
        StringBuilder stringBuilder = new StringBuilder();
        for (JsonToken child : jt.getChildren()) {
            // Set prefixes if required
            if (lastTokenType == OBJECT || lastTokenType == ARRAY) {
                stringBuilder.append((child.getType() == KEY || child.getType() == BNODE) ? COMMA : NONE);
            } else if (lastTokenType == VALUE) {
                stringBuilder.append((child.getType() == KEY || child.getType() == VALUE) ? COMMA : NONE);
            }
            // Set name of key / value
            stringBuilder.append(child.toString());
            // Descend to child nodes if present
            if (child.getType() == KEY || child.getType() == BNODE) {
                switch (child.getParentheses()) {
                    case 0:
                        stringBuilder.append(":");
                        stringBuilder.append(buildJsonString((byte) 0, child));
                        lastTokenType = VALUE;
                        break;
                    case OBJECT:
                        if (child.getType() != BNODE) stringBuilder.append(":");
                        stringBuilder.append("{");
                        stringBuilder.append(buildJsonString((byte) 0, child));
                        stringBuilder.append("}");
                        lastTokenType = OBJECT;
                        break;
                    case ARRAY:
                        stringBuilder.append(":[");
                        stringBuilder.append(buildJsonString((byte) 0, child));
                        stringBuilder.append("]");
                        lastTokenType = ARRAY;
                        break;
                }
            } else {
                lastTokenType = VALUE;
            }
        }
        return stringBuilder.toString();
    }

    /**
     * Contains information on a Json token (e.g. parent) and methods to query, modify and serialize it.
     */
    private class JsonToken {

        byte type;            // Type of the token
        String name;        // Name (only if type == parent / value, else null)
        List<JsonToken> children = new ArrayList<>();    // Last element belonging to the parent (parent only, else null)
        JsonToken parent;        // Key which token belongs to (for parent: root parent)
        byte parentheses = -1;  // Parentheses which surrounds descendants (none, brackets or braces)


        /**
         * Constructor for class JsonToken
         *
         * @param type   Type of token (one of ROOT_NODE, OBJECT, ARRAY, END_OBJECT, END_ARRAY, KEY, VALUE)
         * @param name   Name of token (only if type == KEY or VALUE, else null)
         * @param parent Refers to parent id of which a token belongs to (parent refer to their root parent)
         */
        JsonToken(byte type, String name, JsonToken parent) {
            this.type = type;
            if (name == null) name = "";
            if (name.endsWith("{}")) name = name.substring(0, name.length() - 2);
            this.name = (escapeChars && !(name.equals(""))) ? escChars(name) : name;
            this.parent = parent;
            if (parent != null) parent.setChildren(this);
            if (type == BNODE) this.parentheses = OBJECT;
        }

        /**
         * Prints respective literal
         *
         * @return Literal
         */
        @Override
        public String toString() {
            switch (this.getType()) {
                case BNODE:
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
         *
         * @return Last element
         */
        List<JsonToken> getChildren() {
            return children;
        }

        /**
         * Adds a new descendant to node and probably sets new value for variable parentheses
         *
         * @param jt Respective JSON token
         */
        void setChildren(JsonToken jt) {
            if (jt.getType() == KEY) parentheses = OBJECT;
            if ((parentheses == 0 && (jt.getType() == VALUE) | jt.getType() == BNODE)) parentheses = ARRAY;
            if (parentheses == -1 && (jt.getType() == VALUE | jt.getType() == BNODE)) parentheses = 0;
            children.add(jt);
        }

        /**
         * Gets parent node
         *
         * @return Parent node
         */
        JsonToken getParent() {
            return parent;
        }

        /**
         * Gets type of JSON token (ROOT_NODE, KEY, VALUE)
         *
         * @return Type of JSON token
         */
        byte getType() {
            return type;
        }

        /**
         * Gets parentheses category (none, brackets or braces)
         *
         * @return Parantheses type (in byte)
         */
        byte getParentheses() {
            return parentheses;
        }

        String escChars(String value) {
            StringBuilder stringBuilder = new StringBuilder();
            String t;
            for (char c : value.toCharArray()) {
                switch (c) {
                    case '\\':
                    case '"':
                        stringBuilder.append('\\');
                        stringBuilder.append(c);
                        break;
                    case '\b':
                        stringBuilder.append("\\b");
                        break;
                    case '\t':
                        stringBuilder.append("\\t");
                        break;
                    case '\n':
                        stringBuilder.append("\\n");
                        break;
                    case '\f':
                        stringBuilder.append("\\f");
                        break;
                    case '\r':
                        stringBuilder.append("\\r");
                        break;
                    default:
                        if (c < ' ') {
                            t = "000" + Integer.toHexString(c);
                            stringBuilder.append("\\u");
                            stringBuilder.append(t.substring(t.length() - 4));
                        } else {
                            stringBuilder.append(c);
                        }
                }
            }
            return stringBuilder.toString();
        }

    }

}
