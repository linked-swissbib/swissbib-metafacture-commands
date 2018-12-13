package org.swissbib.linked.linkeddata;

import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;

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

    private final List<String> subjects = new ArrayList<>();
    private int bnodeCounter = 0;
    private final StringBuilder resource = new StringBuilder();

    /**
     * Performs a simple check if a string could be a URI
     * @param uri String to be checked
     * @return true if string could be a URI, false if otherwise
     */
    private static Boolean simpleUriCheck(String uri) {
        // scheme:[//[user:password@]host[:port]][/]path[?query][#fragment]
        return uri.matches("[^ ]+:[^ ]+");
    }

    /**
     * Creates a blank node "identifier"
     * @param counter Number of identifier
     * @return Identifier as string
     */
    private static String createBnode(int counter) {
        return "_:b" + ("0000000" + String.valueOf(counter)).substring(String.valueOf(counter).length());
    }

    /**
     * Creates an N-triple compliant statement of a given subject, predicate and object
     * @param subject Subject of the triple
     * @param predicate Predicate of the triple
     * @param object Object of the triple
     * @return N-triple statement as string
     */
    private static String createStatement(String subject, String predicate, String object) {
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

    @Override
    public void startRecord(String identifier) {
        subjects.add(identifier);
    }

    @Override
    public void endRecord() {
        getReceiver().process(resource.toString());
        resource.setLength(0);
        subjects.clear();
    }

    @Override
    public void startEntity(String name) {
        subjects.add(createBnode(++bnodeCounter));
        resource.append(createStatement(subjects.get(subjects.size() - 2), name, subjects.get(subjects.size() - 1)));
    }

    @Override
    public void endEntity() {
        subjects.remove(subjects.size() - 1);
    }

    @Override
    public void literal(String name, String value) {
        resource.append(createStatement(subjects.get(subjects.size() - 1), name, value));
    }

}
