/*
 *  Copyright 2013, 2014 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.swissbib.linked.mf.writer;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;
import org.apache.commons.lang.StringEscapeUtils;
import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Serialises an object as JSON. Records and entities are represented
 * as objects unless their name ends with []. If the name ends with [],
 * an array is created.
 *
 * @author Christoph Böhme
 * @author Michael Büchner
 *
 */
@Description("Serialises an object as JSON")
@In(StreamReceiver.class)
@Out(String.class)
public final class ESBulkAPIEncoder extends
        DefaultStreamPipe<ObjectReceiver<String>> {

    public static final String ARRAY_MARKER = "[]";
    public static final String BNODE_MARKER = "{}";
    private Boolean IN_ARRAY = false;
    public static final String ROOT_ELEMENT_SEPARATOR = "\n";

    private final JsonGenerator jsonGenerator;
    private final StringWriter writer = new StringWriter();
    private JsonGenerator arrayReservoir;
    private final StringWriter resWriter = new StringWriter();

    public ESBulkAPIEncoder() {
        try {
            jsonGenerator = new JsonFactory().createGenerator(writer);
            jsonGenerator.setRootValueSeparator(null);

            arrayReservoir = new JsonFactory().createGenerator(resWriter);
            arrayReservoir.setRootValueSeparator(null);

            arrayReservoir.writeRaw(",");

        } catch (final IOException e) {
            throw new MetafactureException(e);
        }
    }

    public void setPrettyPrinting(final boolean prettyPrinting) {
        if (prettyPrinting) {
            jsonGenerator.useDefaultPrettyPrinter();
        } else {
            jsonGenerator.setPrettyPrinter(null);
        }
    }

    public boolean getPrettyPrinting() {
        return jsonGenerator.getPrettyPrinter() != null;
    }

    /**
     * By default JSON output does only have escaping where it is strictly
     * necessary. This is recommended in the most cases. Nevertheless it can
     * be sometimes useful to have some more escaping. With this method it is
     * possible to use {@link StringEscapeUtils#escapeJavaScript(String)}.
     *
     * @param escapeCharacters an array which defines which characters should be
     *                         escaped and how it will be done. See
     *                         {@link CharacterEscapes}. In most cases this should
     *                         be null. Use like this:
     *                         <pre>{@code int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
     * 	                       // and force escaping of a few others:
     * 	                       esc['\''] = CharacterEscapes.ESCAPE_STANDARD;
     *                         JsonEncoder.useEscapeJavaScript(esc);
     *                         }</pre>
     */
    public void setJavaScriptEscapeChars(final int[] escapeCharacters) {

        final CharacterEscapes ce = new CharacterEscapes() {
            private static final long serialVersionUID = 1L;

            @Override
            public int[] getEscapeCodesForAscii() {
                if (escapeCharacters == null) {
                    return CharacterEscapes.standardAsciiEscapesForJSON();
                }

                return escapeCharacters;
            }

            @Override
            public SerializableString getEscapeSequence(final int ch) {
                final String chString = Character.toString((char) ch);
                final String jsEscaped = StringEscapeUtils.escapeJavaScript(chString);
                return new SerializedString(jsEscaped);
            }

        };

        jsonGenerator.setCharacterEscapes(ce);
    }

    @Override
    public void startRecord(final String id) {
        final StringBuffer buffer = writer.getBuffer();
        buffer.delete(0, buffer.length());
        startGroup(id);
    }

    @Override
    public void endRecord() {
        endGroup();
        try {
            jsonGenerator.flush();
        } catch (final IOException e) {
            throw new MetafactureException(e);
        }
        getReceiver().process(writer.toString());
    }

    @Override
    public void startEntity(final String name) {
        startGroup(name);
    }

    @Override
    public void endEntity() {
        endGroup();
    }

    @Override
    public void literal(final String name, final String value) {
        try {

            JsonStreamContext ctx = getJsonGenerator().getOutputContext();

            if (IN_ARRAY) {
                if (ctx.inObject()) {
                    getJsonGenerator().writeFieldName(name);
                }
            } else {
                if (ctx.inObject()) {
                    getJsonGenerator().writeFieldName(name);
                }
            }

            if (value == null) {
                getJsonGenerator().writeNull();
            } else {
                getJsonGenerator().writeString(value);
            }

        } catch (final JsonGenerationException e) {
            throw new MetafactureException(e);
        }
        catch (final IOException e) {
            throw new MetafactureException(e);
        }
    }

    /**
     * Called whenever a new entity starts
     * @param name Name of entity
     */
    private void startGroup(final String name) {
        try {

            JsonStreamContext ctx = getJsonGenerator().getOutputContext();

            if (IN_ARRAY) {

                if (!name.endsWith(ARRAY_MARKER)
                        && !name.endsWith(BNODE_MARKER)) {
                    getJsonGenerator().writeFieldName(name);
                }

                getJsonGenerator().writeStartObject();

            } else {

                if (ctx.inRoot() && !name.equals("")) {
                    getJsonGenerator().writeStartObject();
                }

                ctx = getJsonGenerator().getOutputContext();

                // If a new explicit array begins, remove array marker and open array bracket
                if (name.endsWith(ARRAY_MARKER)) {
                    IN_ARRAY = true;
                    ctx = getJsonGenerator().getOutputContext();
                    if(!ctx.inArray()) {
                        getJsonGenerator().writeFieldName(name.substring(0, name.length() - ARRAY_MARKER.length()));
                        getJsonGenerator().writeRaw(":");
                        getJsonGenerator().writeStartArray();
                    }
                } else {
                    if (ctx.inObject() || (ctx.inRoot() && !name.equals(""))) {
                        getJsonGenerator().writeFieldName(name);
                    }
                    getJsonGenerator().writeStartObject();
                }

            }

        } catch (final JsonGenerationException e) {
            throw new MetafactureException(e);
        }
        catch (final IOException e) {
            throw new MetafactureException(e);
        }
    }

    private void endGroup() {
        try {
            JsonStreamContext ctx = getJsonGenerator().getOutputContext();
            if (ctx.inObject()) {
                if (!IN_ARRAY && ctx.getParent().getParent().inRoot()) {
                    closeRootObject();
                } else {
                    getJsonGenerator().writeEndObject();
                }
            } else if (ctx.inArray()) {
                //
                if (ctx.getParent().inRoot() && IN_ARRAY) {
                    IN_ARRAY = false;
                } else {
                    getJsonGenerator().writeEndArray();
                    if (ctx.getParent().getParent().inRoot()) {
                        closeRootObject();
                    }
                }

            }
        } catch (final JsonGenerationException e) {
            throw new MetafactureException(e);
        }
        catch (final IOException e) {
            throw new MetafactureException(e);
        }
    }


    /**
     * Check if root element can be closed
     * @throws IOException
     */
    protected void closeRootObject() throws IOException {
        if (arrayReservoir.getOutputContext().inArray()) {
            arrayReservoir.writeEndArray();
            arrayReservoir.flush();
            jsonGenerator.writeRaw(resWriter.toString());
            resWriter.getBuffer().setLength(0);
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.writeEndObject();
        jsonGenerator.writeRaw(ROOT_ELEMENT_SEPARATOR);
    }


    /**
     * Get JsonGenerator (depends on switch IN_ARRAY)
     * @return Current JsonGenerator
     */
    protected JsonGenerator getJsonGenerator() {
        if(IN_ARRAY) {
            return arrayReservoir;
        } else {
            return jsonGenerator;
        }
    }

}