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
 * Serialises an object as JSON-LD.
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Serialises an object as JSON-LD")
@In(StreamReceiver.class)
@Out(String.class)
public final class NeoEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {


    private final static Logger LOG = LoggerFactory.getLogger(NeoEncoder.class);

    Map<String, String> node = new HashMap<>();
    Map<String, ArrayList<String>> relations = new HashMap<>();


    @Override
    public void startRecord(String id) {
        LOG.debug("Parsing record {}", id);

        node.put("id", numericHash(id));
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
        if (name.equals("rela")) {
            String[] tokens = value.split("#");
            if (relations.containsKey(tokens[1])) {
                relations.get(tokens[1]).add(tokens[0]);
            } else {
                ArrayList<String> newRela = new ArrayList<>();
                newRela.add(tokens[0]);
                relations.put(tokens[1], newRela);
            }
        } else {
            node.put(name, value);
        }
    }

    private String numericHash(String id) {
        char[] ca = id.toCharArray();
        StringBuilder sb = new StringBuilder();
        for (char aCa : ca) {
            sb.append(((Character) aCa).hashCode());
        }
        return sb.toString();
    }

    private String serializer() {
        StringBuilder sb = new StringBuilder();
        sb.append(node.get("entity"))
                .append("#")
                .append(node.get("id"))
                .append("\",\"")
                .append(node.get("subentity"))
                .append("\",\"")
                .append(node.get("name"))
                .append("\",\"")
                .append(node.get("addName"))
                .append("\",\"")
                .append(node.get("date"))
                .append("\"")
                .append("|");
        for (Map.Entry<String, ArrayList<String>> entry : relations.entrySet()) {
            sb.append(entry.getKey())
                    .append("#");
            for (String e : entry.getValue()) {
                sb.append(node.get("id"))
                        .append(",")
                        .append(e)
                        .append("\n");
            }
            sb.append("|");
        }
        return sb.toString();
    }


}


