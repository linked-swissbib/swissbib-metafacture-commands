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
package org.swissbib.linked.mf.source;

import org.culturegraph.mf.framework.MetafactureException;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.io.HttpOpener;
//import org.culturegraph.mf.stream.source.Opener;
//import org.culturegraph.mf.io.ResourceOpener;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;


/*
todo: what was the idea of this type???
 */


/**
 * Opens a {@link URLConnection} and passes a reader to the receiver.
 *
 * @author Christoph Böhme
 * @author Jan Schnasse
 */
@Description("Opens a http resource. Supports the setting of Accept and Accept-Charset as http header fields.")
@In(String.class)
@Out(Reader.class)
public final class MultiHttpOpener extends DefaultObjectPipe<String, ObjectReceiver<Reader>>
{
    private String encoding = "UTF-8";
    private String accept = "*/*";
    private int lowerBound;
    private int upperBound;
    private int chunkSize;

    /**
     * @param accept The accept header in the form type/subtype, e.g. text/plain.
     */
    public void setAccept(final String accept) {
        this.accept = accept;
    }

    /**
     * @param encoding The encoding is used to encode the output and is passed
     *                 as Accept-Charset to the http connection.
     */
    public void setEncoding(final String encoding) {
        this.encoding = encoding;
    }

    public void setLowerBound(final String lowerBound) {
        this.lowerBound = Integer.parseInt(lowerBound);
    }

    public void setUpperBound(final String upperBound) {
        this.upperBound = Integer.parseInt(upperBound);
    }

    public void setChunkSize(final String chunkSize) {
        this.chunkSize = Integer.parseInt(chunkSize);
    }

    @Override
    public void process(String urlStr) {
        urlStr = urlStr.replace("${cs}", String.valueOf(chunkSize));
        int pager = lowerBound;
        while (pager < upperBound) {
            try {
                final URL url = new URL(urlStr.replace("${pa}", String.valueOf(pager)));
                final URLConnection con = url.openConnection();
                con.addRequestProperty("Accept", accept);
                con.addRequestProperty("Accept-Charset", encoding);
                String enc = con.getContentEncoding();
                if (enc == null) {
                    enc = encoding;
                }
                getReceiver().process(new InputStreamReader(con.getInputStream(), enc));
            } catch (IOException e) {
                throw new MetafactureException(e);
            }
            pager = pager + chunkSize;
        }
    }
}
