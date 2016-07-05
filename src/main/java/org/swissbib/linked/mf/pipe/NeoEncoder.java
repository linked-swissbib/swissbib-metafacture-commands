package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Encodes objects as csv files fit for batch uploading to Neo4j
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Serialises an object as JSON-LD")
@In(StreamReceiver.class)
@Out(String.class)
public final class NeoEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {

    private final static Logger LOG = LoggerFactory.getLogger(NeoEncoder.class);

    private Map<String, String> node = new HashMap<>();
    private Map<String, ArrayList<String>> relations = new HashMap<>();
    private String nodeId;


    @Override
    public void startRecord(String id) {
        LOG.debug("Parsing record {}", id);

        nodeId = id;

        node.put("id", quote(nodeId));
        node.put("entity", "");
        node.put("subentity", "");
        node.put("name", "");
        node.put("addName", "");
        node.put("date", "");
    }

    @Override
    public void endRecord() {
        LOG.trace("Sending record to {}", getReceiver().getClass());
        getReceiver().process(serializer());
    }

    @Override
    public void literal(String name, String value) {
        value = clean(value);
        if (name.equals("rela")) {
            String[] tokens = value.split("#");
            if (relations.containsKey(tokens[0])) {
                relations.get(tokens[0]).add(quote(nodeId) + "," + quote(tokens[1]) + "," + quote(tokens[2]));
            } else {
                ArrayList<String> newRela = new ArrayList<>();
                newRela.add(quote(nodeId) + "," + quote(tokens[1]) + "," + quote(tokens[2]));
                relations.put(tokens[0], newRela);
            }
        } else {
            node.put(name, quote(value));
        }
    }


    private String serializer() {
        StringBuilder sb = new StringBuilder();
        sb.append(node.get("entity"))
                .append("#")
                .append(node.get("id"))
                .append(",")
                .append(node.get("subentity"))
                .append(",")
                .append(node.get("name"))
                .append(",")
                .append(node.get("addName"))
                .append(",")
                .append(node.get("date"))
                .append("||");
        for (Map.Entry<String, ArrayList<String>> entry : relations.entrySet()) {
            for (String e : entry.getValue()) {
                sb.append(entry.getKey())
                        .append("#")
                        .append(e)
                        .append("||");
            }
        }
        node.clear();
        relations.clear();
        return sb.toString();
    }

    private String clean(String rawString) {
        if (rawString.endsWith("\\")) {
            rawString = rawString.substring(0, rawString.length() - 2);
        }
        return rawString
                .replaceAll("\"", "")
                .replaceAll("\\|\\|", "");
    }

    private String quote(String rawString) {
        return "\"" + rawString + "\"";
    }

}
