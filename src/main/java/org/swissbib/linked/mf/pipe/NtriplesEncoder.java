package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 17.11.15
 */
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

    static Boolean simpleUriCheck(String uri) {
        // scheme:[//[user:password@]host[:port]][/]path[?query][#fragment]
        return uri.matches("[^ ]+:[^ ]+");
    }

    static String createBnode(int counter) {
        return "_:b" + ("00" + String.valueOf(counter)).substring(String.valueOf(counter).length());
    }

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
