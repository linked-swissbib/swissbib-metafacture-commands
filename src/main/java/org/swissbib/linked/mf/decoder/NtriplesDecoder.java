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
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 12.11.15
 */
@Description("Decodes lines of N-Triple files.")
@In(Reader.class)
@Out(StreamReceiver.class)
public final class NtriplesDecoder extends DefaultObjectPipe<Reader, StreamReceiver> {
    private String resource;
    private Boolean startDocument = true;

    public void process(Reader reader) {

/*      Each line of the file has either the form of a comment or of a statement:
        A statement consists of three parts, separated by whitespace: the subject, the predicate and the object,
        and is terminated with a full stop.
        Subjects may take the form of a URI or a Blank node;
        predicates must be a URI;
        objects may be a URI, blank node or a literal.
        URIs are delimited with angle brackets.
        Blank nodes are represented by an alphanumeric string, prefixed with an underscore and colon (_:).
        Literals are represented as printable ASCII strings (with backslash escapes),delimited with double-quote characters,
        and optionally suffixed with a language or datatype indicator.
        Language indicators are an at sign followed by an RFC 3066 language tag;
        datatype indicators are a double-caret followed by a URI. Comments consist of a line beginning with a hash sign. */

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
            } else if (c == '\"' && !ignoreEndLiteral) {                                     // Start / end of a literal
                if (inLiteral) {
                    elem.append(c);
                    endLiteralChar = true;
                } else {
                    elem.append(c);
                    inLiteral = true;
                }
                ignoreEndLiteral = false;
            } else if (c == ' ' && endLiteralChar) {
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
            } else if (c == '\\' && inLiteral) {
                ignoreEndLiteral = true;
                elem.append(c);
            }
            else {                                                    // Record content
                if (inURI || inLiteral || inBnode) elem.append(c);
                ignoreEndLiteral = false;
            }

        }

        if (statement.size() != 3)
            throw(new MetafactureException("Statement must have exactly three elements: " + string));

        return statement;
    }

}
