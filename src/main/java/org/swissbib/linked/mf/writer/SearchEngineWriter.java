package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.nio.charset.Charset;

/**
 * Created by swissbib on 18.01.15.
 */


@Description("first step to write triples into a search engine index")
@In(Object.class)
@Out(Void.class)
public final class SearchEngineWriter<T> implements ConfigurableObjectWriter<T> {



    private String header = DEFAULT_HEADER;
    private String footer = DEFAULT_FOOTER;
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;



    private static final String SET_COMPRESSION_ERROR = "Cannot compress Search engine";

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

    @Override
    public void process(T obj) {
        if (firstObject) {
            System.out.print(getHeader());
            firstObject = false;
        } else {
            System.out.print(getSeparator());
        }
        System.out.print(obj);

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
