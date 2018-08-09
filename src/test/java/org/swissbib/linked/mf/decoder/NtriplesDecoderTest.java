package org.swissbib.linked.mf.decoder;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.StreamReceiver;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
class NtriplesDecoderTest {

    private NtriplesDecoder decoder;
    private StringReader stringReader;

    @Mock
    private StreamReceiver receiver;

    @BeforeEach
    void setup() {
        decoder = new NtriplesDecoder();
        decoder.setReceiver(receiver);
    }

    @AfterEach
    void teardown() {
        decoder.closeStream();
        stringReader.close();
    }

    @Test
    void ntripleWithOnlyURIs() {
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Document> ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Document");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithEmptyLiteral() {
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"\" ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithLanguageTag() {
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"N-Triples\"@en-US ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "N-Triples##@en-US");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithIgnoredLanguageTag() {
        decoder.setKeepLanguageTags("false");
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"N-Triples\"@en-US ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "N-Triples");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithTypedLiteral() {
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"2\"^^<http:/www.w3.org/2001/XMLSchema#integer> ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "2##^^<http:/www.w3.org/2001/XMLSchema#integer>");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithIgnoredTypedLiteral() {
        decoder.setKeepTypeAnnotations("false");
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"2\"^^<http:/www.w3.org/2001/XMLSchema#integer> ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void ntripleWithLiteralWithEscapedApostropheAndSpecialChars() {
        stringReader = new StringReader(
                "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"\\\"$çäöÜ\" ."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://purl.org/dc/terms/title", "\"$çäöÜ");
        ordered.verify(receiver).endRecord();
    }

    @Disabled("For stability reasons this exception is normally caught in process()")
    @Test
    void invalidNtripleShouldThrowMetafactureException() {
        assertThrows(
                MetafactureException.class,
                () -> {
                    stringReader = new StringReader(
                            "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> ."
                    );
                    decoder.process(stringReader);
                }
        );
    }

    @Test
    void ntriplesWithBlankNodes() {
        stringReader = new StringReader(
                " <http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Document> .\n" +
                        "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:art .\n " +
                        "_:art <http://xmlns.com/foaf/0.1/name> \"Art Barstow\".\n" +
                        " <http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:dave .\n" +
                        " _:art <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .\n" +
                        " _:art <http://example.org/hasAddress> _:address .\n" +
                        " _:dave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .\n" +
                        " _:address <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Address> .\n" +
                        " _:dave <http://xmlns.com/foaf/0.1/name> \"Dave Beckett\"."
        );
        final InOrder ordered = inOrder(receiver);
        decoder.process(stringReader);
        ordered.verify(receiver).startRecord("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        ordered.verify(receiver).literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Document");
        ordered.verify(receiver).endRecord();
        ordered.verify(receiver).startRecord("http://xmlns.com/foaf/0.1/maker");
        ordered.verify(receiver).literal("http://xmlns.com/foaf/0.1/name", "Art Barstow");
        ordered.verify(receiver).literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person");
        ordered.verify(receiver).startEntity("http://example.org/hasAddress");
        ordered.verify(receiver).literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Address");
        ordered.verify(receiver).endEntity();
        ordered.verify(receiver).endRecord();
        ordered.verify(receiver).startRecord("http://xmlns.com/foaf/0.1/maker");
        ordered.verify(receiver).literal("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person");
        ordered.verify(receiver).literal("http://xmlns.com/foaf/0.1/name", "Dave Beckett");
        ordered.verify(receiver).endRecord();
    }
}
