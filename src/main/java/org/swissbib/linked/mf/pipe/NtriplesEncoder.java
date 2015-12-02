package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes data in the Formeta format to the N-triples RDF serialization.
 *
 * @author Sebastian Schüpbach
 * @version 1.0
 * Created on 17.11.15
 */
@Description("Encodes data in Formeta format to N-triples RDF serialization.")
@In(StreamReceiver.class)
@Out(String.class)
public class NtriplesEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {

    List<String> subjects = new ArrayList<>();
    int bnodeCounter = 0;
    StringBuilder resource = new StringBuilder();


    @Override
    public void startRecord(String identifier) {
        subjects.add(identifier);
    }

    @Override
    public void endRecord() {
        getReceiver().process(resource.toString());
        resource.setLength(0);
    }

    @Override
    public void startEntity(String name) {
        resource.append(createStatement(subjects.get(subjects.size() - 1), name, createBnode(++bnodeCounter)));
    }

    @Override
    public void endEntity() {
        subjects.remove(subjects.size() - 1);
    }

    @Override
    public void literal(String name, String value) {
        resource.append(createStatement(subjects.get(subjects.size() - 1), name, value));
    }

    /**
     * Performs a simple check if a string could be a URI
     * @param uri String to be checked
     * @return true if string could be a URI, false if otherwise
     */
    static Boolean simpleUriCheck(String uri) {
        // scheme:[//[user:password@]host[:port]][/]path[?query][#fragment]
        return uri.matches("[^ ]+:[^ ]+");
    }

    /**
     * Creates a blank node "identifier"
     * @param counter Number of identifier
     * @return Identifier as string
     */
    static String createBnode(int counter) {
        return "_:b" + ("00" + String.valueOf(counter)).substring(String.valueOf(counter).length());
    }

    /**
     * Creates an N-triple compliant statement of a given subject, predicate and object
     * @param subject Subject of the triple
     * @param predicate Predicate of the triple
     * @param object Object of the triple
     * @return N-triple statement as string
     */
    static String createStatement(String subject, String predicate, String object) {
        StringBuilder statement = new StringBuilder();
        statement.append((subject.startsWith("_")) ? subject : "<" + subject + ">");
        statement.append(" <");
        statement.append(predicate);
        statement.append("> ");
        if (simpleUriCheck(object)) {
            statement.append(object.startsWith("_") ? object : "<" + object + ">");
        } else {
            statement.append("\"");
            statement.append(object);
            statement.append("\"");
        }
        statement.append(" .\n");
        return statement.toString();
    }

}
