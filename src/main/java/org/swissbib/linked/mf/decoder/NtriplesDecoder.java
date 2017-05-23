package org.swissbib.linked.mf.decoder;

import org.culturegraph.mf.framework.MetafactureException;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;


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

    public Boolean unicodeEscapeSeq = true;

    /**
     * Converts Unicode escape sequences in strings to regular UTF-8 encoded characters
     *
     * @param literal String to be checked for Unicode escape sequences
     * @return String with converted Unicode escape sequences
     */
    static String toutf8(String literal) {
        StringBuilder utf8String = new StringBuilder();
        StringBuilder tempString = new StringBuilder();
        char lastArtifact = ' ';
        char secondLastArtifact = ' ';
        int i = 0;
        int stringLength = literal.length();
        boolean unicodeChar = false;
        while (i < stringLength) {
            char c = literal.charAt(i);
            if (unicodeChar) {
                tempString.append(c);
                if (tempString.length() >= 4) {
                    // Converts hexadecimal code represented as String to integer and casts it afterwards to char
                    c = (char) (int) Integer.valueOf(tempString.toString(), 16);
                    tempString.setLength(0);
                    unicodeChar = false;
                }
            } else {
                // Unicode escape sequences begin are initialized with \\u. So we have to check for such a tuple,
                // but at the same time have to make sure that this tuple is not escaped either (i.e. a literal \\u).
                if (secondLastArtifact != '\\' && lastArtifact == '\\' && c == 'u') {
                    unicodeChar = true;
                } else {
                    utf8String.append(lastArtifact);
                }
            }
            secondLastArtifact = lastArtifact;
            lastArtifact = c;
            i++;
            if (i == stringLength) utf8String.append(c);
        }
        return utf8String.toString().substring(1);
    }

    public void setUnicodeEscapeSeq(String unicodeEscapeSeq) {
        this.unicodeEscapeSeq = Boolean.valueOf(unicodeEscapeSeq);
    }

    @Override
    public void process(Reader reader) {

        BufferedReader lineReader = new BufferedReader(reader, 16777216);

        try {
            for(String e = lineReader.readLine(); e != null; e = lineReader.readLine()) {
                if (!e.startsWith("#")) {
                    List<String> statement = parseLine(e);
                    this.getReceiver().startRecord(statement.get(0));
                    this.getReceiver().literal(statement.get(1),
                            unicodeEscapeSeq ? toutf8(statement.get(2)) : statement.get(2));
                    this.getReceiver().endRecord();
                }
            }

        } catch (IOException var4) {
            throw new MetafactureException(var4);
        }

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
                elem.append(c);
                if (inLiteral) {
                    endLiteralChar = true;
                } else if (!inURI) {
                    inLiteral = true;
                }
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
