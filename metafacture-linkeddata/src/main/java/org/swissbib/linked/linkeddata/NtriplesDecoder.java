package org.swissbib.linked.linkeddata;

import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultObjectPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Decodes data in N-triple format to Formeta
 *
 * @author Sebastian Schüpbach
 * @version 1.1
 */
@Description("Decodes lines of N-Triple files.")
@In(Reader.class)
@Out(StreamReceiver.class)
public final class NtriplesDecoder extends DefaultObjectPipe<Reader, StreamReceiver> {

    private Boolean unicodeEscapeSeq = true;
    private Boolean keepLanguageTags = true;
    private Boolean keepTypeAnnotations = true;
    private static final Logger LOG = LoggerFactory.getLogger(NtriplesDecoder.class);

    /**
     * Converts Unicode escape sequences in strings to regular UTF-8 encoded characters
     *
     * @param literal String to be checked for Unicode escape sequences
     * @return String with converted Unicode escape sequences
     */
    private static String toutf8(String literal) {
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
                // Unicode escape sequences are initialized with \\u. So we have to check for such a tuple,
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

    @SuppressWarnings("unused")
    public void setUnicodeEscapeSeq(String unicodeEscapeSeq) {
        this.unicodeEscapeSeq = Boolean.valueOf(unicodeEscapeSeq);
    }

    @SuppressWarnings("unused")
    public void setKeepLanguageTags(String keepLanguageTags) {
       this.keepLanguageTags = Boolean.valueOf(keepLanguageTags);
    }

    @SuppressWarnings("unused")
    public void setKeepTypeAnnotations(String keepTypeAnnotations) {
        this.keepTypeAnnotations = Boolean.valueOf(keepTypeAnnotations);
    }

    @Override
    public void process(Reader reader) {

        BufferedReader lineReader = new BufferedReader(reader, 16777216);
        Map<String, Element> bnodeMap = new HashMap<>();
        List<Element> rootBnodes = new ArrayList<>();

        try {
            int i = 0;
            for (String e = lineReader.readLine(); e != null; e = lineReader.readLine()) {
                i++;
                if (!e.startsWith("#")) {
                    LOG.debug("Processing triple on line {}: {}", i, e);
                    List<String> statement;
                    try {
                        statement = parseLine(e);
                    } catch (Exception ex) {
                        LOG.error("Parse error for triple on line {}: {}", i, ex.getClass().getName());
                        LOG.warn("Skipping triple on line {} because of error", i);
                        continue;
                    }
                    if (isBnode(statement.get(0))) {
                        if (isBnode(statement.get(2))) {
                            Element entity = new Element(statement.get(1));
                            bnodeMap.get(statement.get(0)).addEntity(entity);
                            bnodeMap.put(statement.get(2), entity);
                        } else {
                            Element literal = new Element(statement.get(1));
                            literal.addValue(statement.get(2));
                            bnodeMap.get(statement.get(0)).addEntity(literal);
                        }
                    } else if (isBnode(statement.get(2))) {
                        Element elem = new Element(statement.get(1));
                        bnodeMap.put(statement.get(2), elem);
                        rootBnodes.add(elem);
                    } else {
                        this.getReceiver().startRecord(statement.get(0));
                        this.getReceiver().literal(statement.get(1),
                                (unicodeEscapeSeq && statement.get(2).length() > 0)
                                        ? toutf8(statement.get(2))
                                        : statement.get(2));
                        this.getReceiver().endRecord();
                    }
                }
            }
            for (Element r : rootBnodes) {
                r.serialise();
            }

        } catch (IOException var4) {
            throw new MetafactureException(var4);
        }

    }

    private static boolean isBnode(String str) {
        return str.startsWith("_:");
    }

    /**
     * Parses a N-triples statement and returns elements (subject, predicate, object) as three-part ArrayList
     *
     * @param string Statement to be parsed
     * @return List with subject, predicate and object
     */
    private List<String> parseLine(String string) throws MetafactureException {

        List<String> statement = new ArrayList<>();

        final byte NOCTX = 0;
        final byte INURI = 1;
        final byte INBNODE = 2;
        final byte INLITERAL = 3;
        final byte INIGNOREDCHAR = 4;
        final byte INDATATYPEURI = 5;
        final byte AFTERSINGLECARET = 6;
        final byte INLANGUAGETAG = 7;
        byte ctx = NOCTX;
        StringBuilder elem = new StringBuilder();

        Predicate beginOfUri = (char character, byte context) -> character == '<' && context == NOCTX;
        Predicate endOfUri = (char character, byte context) -> character == '>' && context == INURI;
        Predicate startOfLiteral = (char character, byte context) -> character == '"' && context == NOCTX;
        Predicate endOfLiteral = (char character, byte context) -> character == '"' && context == INLITERAL;
        Predicate startOfBNode = (char character, byte context) -> character == '_' && context == NOCTX;
        Predicate endOfBNode = (char character, byte context) -> (character == '.' || character == ' ' || character == '\t') && context == INBNODE;
        Predicate endOfTriple = (char character, byte context) -> character == '.' && context == NOCTX;
        Predicate escapeChar = (char character, byte context) -> character == 0x005c && context == INLITERAL;
        Predicate languageTag = (char character, byte context) -> character == 0x0040 && context == NOCTX;
        Predicate endOfLanguageTag = (char character, byte context) -> (character == '.' || character == ' ' || character == '\t') && context == INLANGUAGETAG;
        Predicate possibleDatatypeUri = (char character, byte context) -> character == '^' && context == NOCTX;
        Predicate noDatatypeUri = (char character, byte context) -> character != '^' && context == AFTERSINGLECARET;
        Predicate datatypeUri = (char character, byte context) -> character == '^' && context == AFTERSINGLECARET;
        Predicate endOfDatatypeUri = (char character, byte context) -> character == '>' && context == INDATATYPEURI;

        for (char c : string.toCharArray()) {

            if (is(beginOfUri, c, ctx)) {
                ctx = INURI;
            } else if (is(endOfUri, c, ctx)) {
                ctx = NOCTX;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (is(startOfLiteral, c, ctx)) {
                ctx = INLITERAL;
            } else if (is(endOfLiteral, c, ctx)) {
                ctx = NOCTX;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (is(startOfBNode, c, ctx)) {
                ctx = INBNODE;
                elem.append(c);
            } else if (is(endOfBNode, c, ctx)) {
                ctx = NOCTX;
                statement.add(elem.toString());
                elem.setLength(0);
            } else if (is(endOfTriple, c, ctx)) {
                break;
            } else if (is(escapeChar, c, ctx)) {
                ctx = INIGNOREDCHAR;
            } else if (is(languageTag, c, ctx)){
                ctx = INLANGUAGETAG;
                elem.append("##");
                elem.append(c);
            } else if (is(possibleDatatypeUri, c, ctx)) {
                ctx = AFTERSINGLECARET;
                elem.append("##");
                elem.append(c);
            } else if (is(noDatatypeUri, c, ctx)) {
                elem.setLength(0);
            } else if (is(datatypeUri, c, ctx)) {
                ctx = INDATATYPEURI;
                elem.append(c);
            } else if (ctx == INIGNOREDCHAR) {
                if (c == '"') elem.append(c);
                ctx = INLITERAL;
            } else if (is(endOfLanguageTag, c, ctx)) {
                ctx = NOCTX;
                if (keepLanguageTags) {
                    statement.set(statement.size() -1, statement.get(statement.size() -1 ) + elem.toString());
                }
                elem.setLength(0);
            } else if (is(endOfDatatypeUri, c, ctx)) {
                ctx = NOCTX;
                elem.append(c);
                if (keepTypeAnnotations) {
                    statement.set(statement.size() -1, statement.get(statement.size() -1 ) + elem.toString());
                }
                elem.setLength(0);
            } else if (ctx == INURI || ctx == INLITERAL || ctx == INBNODE || ctx == INLANGUAGETAG || ctx == INDATATYPEURI) {                                                      // Record content
                elem.append(c);
            }

        }

        if (statement.size() != 3)
            throw (new MetafactureException("Statement must have exactly three elements: " + string));

        return statement;
    }

    private boolean is(Predicate p, char c, byte ctx) {
        return p.check(c, ctx);
    }

    interface Predicate {
        boolean check(char c, byte ctx);
    }

    class Element {
        String name;
        List<Element> elems = new ArrayList<>();
        String value;

        Element(String name) {
            this.name = name;
        }

        void addValue(String val) {
            this.value = val;
        }

        void addEntity(Element e) {
            this.elems.add(e);
        }

        void serialise(boolean asRoot) {
            if (value != null && !asRoot) {
                NtriplesDecoder.this.getReceiver().literal(this.name, this.value);
            } else if (elems.size() > 0) {
                if (asRoot) {
                    NtriplesDecoder.this.getReceiver().startRecord(name);
                } else {
                    NtriplesDecoder.this.getReceiver().startEntity(name);
                }
                for (Element e : elems) {
                    e.serialise(false);
                }
                if (asRoot) {
                    NtriplesDecoder.this.getReceiver().endRecord();
                } else {
                    NtriplesDecoder.this.getReceiver().endEntity();
                }
            }
        }

        void serialise() {
            this.serialise(true);
        }

    }

}
