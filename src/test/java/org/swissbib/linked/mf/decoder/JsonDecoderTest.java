package org.swissbib.linked.mf.decoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.metafacture.framework.StreamReceiver;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.inOrder;


@ExtendWith(MockitoExtension.class)
class JsonDecoderTest {

    private JsonDecoder decoder;

    @Mock
    private StreamReceiver receiver;

    @BeforeEach
    void setUp() throws Exception {
        decoder = new JsonDecoder();
        decoder.setReceiver(receiver);
    }

    @AfterEach
    void tearDown() throws Exception {
        decoder.closeStream();
    }

    @Test
    void onlyStrings() {
        decoder.process("{\"k1\": \"v1\", \"k2\": \"v2\", \"k3\": \"v3\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).literal("k3", "v3");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void intAtBeginning() {
        decoder.process("{\"k1\": 1, \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "1");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void intAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"k2\": 2}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void floatAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"k2\": 2.2}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "2.2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void floatAtBeginning() {
        decoder.process("{\"k1\": 1.1, \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "1.1");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void expoAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"k2\": 2e2}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "2e2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void expoAtBeginning() {
        decoder.process("{\"k1\": 1e1, \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "1e1");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void stringArrayAtBeginning() {
        decoder.process("{\"k1\": [\"1\", \"2\"], \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "1");
        ordered.verify(receiver).literal("k1", "2");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void stringArrayAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"k2\": [\"2\", \"3\"]}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "2");
        ordered.verify(receiver).literal("k2", "3");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void intArrayAtBeginning() {
        decoder.process("{\"k1\": [1, 2], \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "1");
        ordered.verify(receiver).literal("k1", "2");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void intArrayAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"k2\": [2,3]}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).literal("k2", "2");
        ordered.verify(receiver).literal("k2", "3");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void objectAtEnd() {
        decoder.process("{\"k1\": \"v1\", \"e2\": {\"k22\": \"v21\"}}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).literal("k1", "v1");
        ordered.verify(receiver).startEntity("e2");
        ordered.verify(receiver).literal("k22", "v21");
        ordered.verify(receiver).endEntity();
        ordered.verify(receiver).endRecord();
    }

    @Test
    void objectAtBeginning() {
        decoder.process("{\"e1\": {\"k11\":\"v1\"}, \"k2\": \"v2\"}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).startEntity("e1");
        ordered.verify(receiver).literal("k11", "v1");
        ordered.verify(receiver).endEntity();
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).endRecord();
    }

    @Test
    void complexJson() {
        decoder.process("{\"k1\": [{\"k11\":\"v1\"}, 2e3, null, true], \"k2\": \"v2\", \"e3\": {\"k31\": 3, \"k32\": [1, 2, 3]}}");
        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("");
        ordered.verify(receiver).startEntity("k1");
        ordered.verify(receiver).literal("k11", "v1");
        ordered.verify(receiver).endEntity();
        ordered.verify(receiver).literal("k1", "2e3");
        ordered.verify(receiver).literal("k1", "");
        ordered.verify(receiver).literal("k1", "true");
        ordered.verify(receiver).literal("k2", "v2");
        ordered.verify(receiver).startEntity("e3");
        ordered.verify(receiver).literal("k31", "3");
        ordered.verify(receiver).literal("k32", "1");
        ordered.verify(receiver).literal("k32", "2");
        ordered.verify(receiver).literal("k32", "3");
        ordered.verify(receiver).endEntity();
        ordered.verify(receiver).endRecord();
    }
}