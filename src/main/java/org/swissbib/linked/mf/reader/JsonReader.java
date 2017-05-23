package org.swissbib.linked.mf.reader;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
//import org.culturegraph.mf.stream.reader.ReaderBase;
import org.swissbib.linked.mf.decoder.JsonDecoder;

import java.io.IOException;
import java.io.Reader;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 26.10.16
 */
public class JsonReader extends DefaultObjectPipe<Reader, JsonDecoder> {

    //todo
    //how to implement JSonReader in conjunction with JsonDecoer as it was with version 3.x??
    //default ObjectPipe does nothing!! doesn't work this way!!

    /*

    old implementattion is no lobger possible
public class JsonReader extends ReaderBase<JsonDecoder> {

    public JsonReader() {
        super(new JsonDecoder());
    }
}


     */


}
