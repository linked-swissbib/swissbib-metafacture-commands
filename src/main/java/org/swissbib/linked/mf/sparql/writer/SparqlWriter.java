package org.swissbib.linked.mf.sparql.writer;

import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.nio.charset.Charset;

/**
 * Created by swissbib on 18.03.15.
 */
public final class SparqlWriter <T> implements ConfigurableObjectWriter<T> {


    private String header = DEFAULT_HEADER;
    private String footer = DEFAULT_FOOTER;
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;

    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    public SparqlWriter () {

    }

    @Override
    public String getEncoding() {
        return Charset.defaultCharset().toString();
    }

    @Override
    public void setEncoding(String encoding) {
        throw new UnsupportedOperationException("Cannot change encoding of Search engine");
    }

    @Override
    public FileCompression getCompression() {
        return FileCompression.NONE;
    }

    @Override
    public void setCompression(FileCompression compression) {
        throw new UnsupportedOperationException(SET_COMPRESSION_ERROR);
    }

    @Override
    public void setCompression(String compression) {
        throw new UnsupportedOperationException(SET_COMPRESSION_ERROR);
    }

    @Override
    public String getHeader() {
        return DEFAULT_HEADER;
    }

    @Override
    public void setHeader(String header) {
        this.header = header;
    }

    @Override
    public String getFooter() {
        return DEFAULT_FOOTER;
    }

    @Override
    public void setFooter(String footer) {
        this.footer = footer;
    }

    @Override
    public String getSeparator() {
        return DEFAULT_SEPARATOR;
    }

    @Override
    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public void process(T obj) {
        if (firstObject) {
            //System.out.print(getHeader());
            firstObject = false;
        } else {

        }
    }

    @Override
    public void resetStream() {
        firstObject = true;
    }

    @Override
    public void closeStream() {
        if (!firstObject) {
            System.out.print(getFooter());
        }
        closed = true;

    }


}
