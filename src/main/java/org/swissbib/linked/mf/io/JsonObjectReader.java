package org.swissbib.linked.mf.io;

import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultObjectPipe;

import java.io.IOException;
import java.io.Reader;

/**
 * <p>Reads JSON data from a {@code Reader} and splits it into individual
 * records along object boundaries.</p>
 * <p>
 * @author Sebastian Schüpbach
 */
@Description("Reads data from a JSON file and splits it into individual objects")
@In(Reader.class)
@Out(String.class)
@FluxCommand("read-json-object")
public final class JsonObjectReader extends
        DefaultObjectPipe<Reader, ObjectReceiver<String>> {

    private static final int BUFFER_SIZE = 1024 * 1024 * 16;

    private static final char START_OBJECT = '{';
    private static final char START_ARRAY = '[';
    private static final char END_OBJECT = '}';
    private static final char END_ARRAY = ']';

    private final StringBuilder builder = new StringBuilder();
    private final char[] buffer = new char[BUFFER_SIZE];


    @Override
    public void process(final Reader reader) {
        assert !isClosed();

        try {
            boolean nothingRead = true;
            int size;
            while ((size = reader.read(buffer)) != -1) {
                nothingRead = false;
                int offset = 0;
                int objectType = -1;
                int level = 0;
                boolean inString = false;
                char[] lastChars = new char[2];
                for (int i = 0; i < size; ++i) {
                    if (!inString && (buffer[i] == START_OBJECT || buffer[i] == START_ARRAY)) {
                        // Start a new object / array
                        if (level <= 0) {
                            // Start of a new root object / root array even
                            objectType = (buffer[i] == START_OBJECT) ? 1 : 0;
                        }
                        level = level + 1;
                    } else if (!inString && (buffer[i] == '"' || buffer[i] == '\'')) {
                        // Start of a literal string
                        inString = true;
                    } else if (inString && (lastChars[0] != '\\' || lastChars[1] == '\\')) {
                        // End of a literal string
                        inString = false;
                    } else if (!inString && ((objectType == 1 && buffer[i] == END_OBJECT) ||
                            (objectType == 0 && buffer[i] == END_ARRAY))) {
                        // End of a root object / root array
                        level = level - 1;
                        if (level <= 0) {
                            builder.append(buffer, offset, i - offset);
                            emitRecord();
                            offset = i + 1;
                        }
                    }
                    lastChars[1] = lastChars[0];
                    lastChars[0] = buffer[i];
                }
                builder.append(buffer, offset, size - offset);
            }
            if (!nothingRead) {
                emitRecord();
            }

        } catch (
                final IOException e)

        {
            throw new MetafactureException(e);
        }

    }

    private void emitRecord() {
        final String record = builder.toString();
        if (!record.isEmpty()) {
            getReceiver().process(record);
            builder.delete(0, builder.length());
        }
    }
}
