package org.swissbib.linked.mf.decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;


/**
 * Decodes data in N-triple format to Formeta
 *
 * @author Sebastian Schüpbach
 * @version 1.0
 * Created on 12.11.15
 */
@Description("Decodes lines of N-Triple files.")
@In(Reader.class)
@Out(StreamReceiver.class)
public final class NtriplesDecoder extends DefaultObjectPipe<Reader, StreamReceiver> {
    private String resource;
    private Boolean startDocument = true;

    @Override
    public void process(Reader reader) {

        BufferedReader lineReader = new BufferedReader(reader, 16777216);

        try {
            for(String e = lineReader.readLine(); e != null; e = lineReader.readLine()) {
                if (!e.startsWith("#")) {
                    List<String> statement = parseLine(e);

                    String subject = statement.get(0);
                    String predicate = statement.get(1);
                    String object = statement.get(2);

                    if (subject.equals(resource)) {
                        // this.getReceiver().startEntity(predicate);
                        this.getReceiver().literal(predicate, object);
                        // this.getReceiver().endEntity();
                    } else {
                        if (!startDocument) {
                            this.getReceiver().endRecord();
                        } else {
                            startDocument = false;
                        }
                        resource = subject;
                        this.getReceiver().startRecord(subject);
                    }
                }
            }

        } catch (IOException var4) {
            throw new MetafactureException(var4);
        }
        this.getReceiver().endRecord();

    }

    /**
     * Parses a N-triples statement and returns elements (subject, predicate, object) as three-part ArrayList
     * @param string Statement to be parsed
     * @return List with subject, predicate and object
     */
    List<String> parseLine(String string) {

        List<String> statement = new ArrayList<>();

        Boolean inLiteral = false;
        Boolean inURI = false;
        Boolean inBnode = false;
        Boolean ignoreEndLiteral = false;
        Boolean endLiteralChar = false;
        StringBuilder elem = new StringBuilder();

        for (char c : string.toCharArray()) {

            if (c == '<' && !inLiteral) {                               // Start of a URI
                inURI = true;
            } else if (c == '>' && !inLiteral) {                        // End of a URI
                inURI = false;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (c == '\"' && !ignoreEndLiteral) {                // Start / end of a literal
                if (inLiteral) {
                    elem.append(c);
                    endLiteralChar = true;
                } else {
                    elem.append(c);
                    inLiteral = true;
                }
                ignoreEndLiteral = false;
            } else if (c == ' ' && endLiteralChar) {                    // Whitespace after the end of a literal
                endLiteralChar = false;
                inLiteral = false;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (c == '_' && !inLiteral && !inURI) {              // Start of a blank node
                inBnode = true;
            } else if (c == ' ' && inBnode) {                           // End of a blank node
                inBnode = false;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (c == '.' && !inLiteral && !inURI && !inBnode) {  // End of statement
                break;
            } else if (c == '\\' && inLiteral) {                        // Don't recognize an escaped " as end of literal
                ignoreEndLiteral = true;
                elem.append(c);
            }
            else {                                                      // Record content
                if (inURI || inLiteral || inBnode) elem.append(c);
                ignoreEndLiteral = false;
            }

        }

        if (statement.size() != 3)
            throw(new MetafactureException("Statement must have exactly three elements: " + string));

        return statement;
    }

}
