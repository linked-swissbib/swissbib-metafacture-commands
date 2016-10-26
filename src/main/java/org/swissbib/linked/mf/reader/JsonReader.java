package org.swissbib.linked.mf.reader;

import org.culturegraph.mf.stream.reader.ReaderBase;
import org.swissbib.linked.mf.decoder.JsonDecoder;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 26.10.16
 */
public class JsonReader extends ReaderBase<JsonDecoder> {

    public JsonReader() {
        super(new JsonDecoder());
    }
}
