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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;

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
    public static final String NO_KEY_OBJECT_MARKER = "{}";
    private Boolean IN_ARRAY = false;
    private Boolean FIRST_LINE = true;
    public static final String ROOT_ELEMENT_SEPARATOR = "\n";

    // protected ArrayList<JsonGenerator> jsonObjectThreads;
    // protected Byte subThread = 0;

    private final JsonGenerator jsonGenerator;
    private final StringWriter writer = new StringWriter();
    private final JsonGenerator arrayReservoir;
    private final StringWriter resWriter = new StringWriter();

    public ESBulkAPIEncoder() {
        try {
            jsonGenerator = new JsonFactory().createGenerator(writer);
            // jsonGenerator.setRootValueSeparator(new SerializedString("\n"));
            jsonGenerator.setRootValueSeparator(null);

            arrayReservoir = new JsonFactory().createGenerator(resWriter);
            arrayReservoir.setRootValueSeparator(null);

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
            JsonStreamContext ctx = jsonGenerator.getOutputContext();

            // Check if an explicitly created array has ended. If so, close array bracket
            if (!name.endsWith(ARRAY_MARKER) && IN_ARRAY && !ctx.inObject()) {
                IN_ARRAY = false;
                arrayReservoir.writeEndArray();
                arrayReservoir.flush();
                jsonGenerator.writeRawValue(resWriter.toString());
            }

            ctx = jsonGenerator.getOutputContext();

            // If a new explicit array begins, remove array marker and open array bracket
            if (name.endsWith(ARRAY_MARKER) && !IN_ARRAY) {
                if (ctx.inObject()) {
                    jsonGenerator.writeFieldName(name.substring(0, name.length() - ARRAY_MARKER.length()));
                }
                jsonGenerator.writeStartArray();
                IN_ARRAY = true;
            } else {
                if (ctx.inObject()) {
                    jsonGenerator.writeFieldName(name);
                }
            }

            if (value == null) {
                jsonGenerator.writeNull();
            } else {
                jsonGenerator.writeString(value);
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

            JsonStreamContext ctx = getJsonStreamContext();


            if(IN_ARRAY) {

                // Check if an explicitly created array has ended. If so, close array bracket
                if (!name.endsWith(ARRAY_MARKER)
                        && !ctx.inObject()
                        && !name.endsWith(NO_KEY_OBJECT_MARKER)) {
                    IN_ARRAY = false;
                    // arrayReservoir.writeEndArray();
                    // arrayReservoir.flush();
                    // jsonGenerator.writeRawValue(resWriter.toString());
                    if (name.equals("index")
                            || name.equals("dct:bibliographicResource")
                            || name.equals("bibo:Document")) {
                        jsonGenerator.writeEndObject();
                    }
                    checkIfNewRootObject(name);
                }

                //

                if (!name.endsWith(ARRAY_MARKER)
                        && !name.endsWith(NO_KEY_OBJECT_MARKER)) {
                    // Todo: Check corresponding new root method!
                    if (IN_ARRAY) {
                        // A key-value pair isn't allowed directly embedded in an array. However it is allowed as descendant of a
                        // blank node (i.e. a JSON-LD object without key).
                        arrayReservoir.writeFieldName(name);
                    } else {
                        jsonGenerator.writeFieldName(name);
                    }

                }



            } else {

                checkIfNewRootObject(name);

                JsonStreamContext ctx = jsonGenerator.getOutputContext();


                // If a new explicit array begins, remove array marker and open array bracket
                if (name.endsWith(ARRAY_MARKER)) {
                    if (ctx.inObject()) {
                        arrayReservoir.writeFieldName(name.substring(0, name.length() - ARRAY_MARKER.length()));
                    }
                    arrayReservoir.writeStartArray();
                    IN_ARRAY = true;
                }

            }










            // Write field name if not in explicitly created array
            if (ctx.inObject() && !IN_ARRAY) {
                jsonGenerator.writeFieldName(name);
            }

            // Open object brace
            if (!name.endsWith(ARRAY_MARKER)) {
                jsonGenerator.writeStartObject();
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
            final JsonStreamContext ctx = jsonGenerator.getOutputContext();
            if (ctx.inObject()) {
                jsonGenerator.writeEndObject();
            } else if (ctx.inArray() && !IN_ARRAY) {
                jsonGenerator.writeEndArray();
            }
        } catch (final JsonGenerationException e) {
            throw new MetafactureException(e);
        }
        catch (final IOException e) {
            throw new MetafactureException(e);
        }
    }


    /**
     * Check if object is on the root level. If so, close old root element (if one exists), go to new line and create
     * a new root element
     * @param name Name of the current element
     * @throws IOException
     */
    protected void checkIfNewRootObject(String name) throws IOException {
        //
        if (!name.equals("")) {
            JsonStreamContext ctx = getJsonStreamContext();
            if (ctx.getParent().inRoot()) {
                if (FIRST_LINE) {
                    FIRST_LINE = false;
                } else {
                    jsonGenerator.writeEndObject();
                    jsonGenerator.writeRaw(ROOT_ELEMENT_SEPARATOR);
                    jsonGenerator.writeStartObject();
                }
            }
        }
    }


    /**
     * Get JsonStreamContext of current JsonGenerator (depends on switch IN_ARRAY)
     * @return JsonStreamContext of current JsonGenerator
     */
    protected JsonStreamContext getJsonStreamContext() {
        if (IN_ARRAY) {
            return arrayReservoir.getOutputContext();
        } else {
            return jsonGenerator.getOutputContext();
        }
    }

}