package org.swissbib.linked.mf.writer;

import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swissbib.linked.mf.utils.JSONLDParserCachedContext;
import java.io.*;
import java.util.*;




/**
 * Transformation Json-LD to Turtle.
 * Should be generalised so we can transform any serialisation format into any other
 *
 * @author Günter Hipler, project swissbib, Basel
 */
@Description("Serialise JSon-LD to turtle")
@In(Reader.class)
@Out(Void.class)
public class TurtleProducer<T> extends CustomWriter<T> {

    final static Logger LOGTURTLE = LoggerFactory.getLogger(TurtleProducer.class);

    private RDFParser rdfParser;

    private ArrayList<String> contexts = new ArrayList<>(Arrays.asList(
            "https://resources.swissbib.ch/document/context.jsonld",
            "https://resources.swissbib.ch/item/context.jsonld",
            "https://resources.swissbib.ch/person/context.jsonld",
            "https://resources.swissbib.ch/organisation/context.jsonld",
            "https://resources.swissbib.ch/resource/context.jsonld",
            "https://resources.swissbib.ch/work/context.jsonld"
    ));

    private Map<String, File> contextMap = new HashMap<>();

    public TurtleProducer() {

        //produce default context
        createFileMap();
        rdfParser = new JSONLDParserCachedContext(contextMap);

    }


    @Override
    public void process(T obj) {

        this.openOutFile();

        Reader r = (Reader) obj;
        TurtleWriter tw = new TurtleWriter(this.fout);
        rdfParser.setRDFHandler(tw);

        try {
            rdfParser.parse(r, "");
            this.closeOutFile();

        } catch (IOException ioex) {
            LOGTURTLE.error("error parsing Jsonld", ioex);
        }

    }


    private void createFileMap() {

        contextMap = new HashMap<>();
        contexts.forEach(context -> contextMap.put(context,new File(context)));

    }

    public void setContext(String contexts) {

        this.contexts = new ArrayList<>(Arrays.asList( contexts.split("###")));
        createFileMap();

    }

    public String getContext() {
        //return this.contexts.stream().collect(Collectors.joining("###"));
        return String.join("###", this.contexts);
    }


    @Override
    public String getEncoding() {
        throw new UnsupportedOperationException("getEncoding not supported");

    }

    @Override
    public void setEncoding(String encoding) {
        throw new UnsupportedOperationException("setEncoding not supported");

    }


    @Override
    public String getHeader() {
        throw new UnsupportedOperationException("getHeader not supported");
    }

    @Override
    public void setHeader(String header) {
        throw new UnsupportedOperationException("setHeader not supported");

    }

    @Override
    public String getFooter() {
        throw new UnsupportedOperationException("getFooter not supported");

    }

    @Override
    public void setFooter(String footer) {
        throw new UnsupportedOperationException("setFooter not supported");

    }

    @Override
    public String getSeparator() {
        throw new UnsupportedOperationException("getSeparator not supported");

    }

    @Override
    public void setSeparator(String separator) {
        throw new UnsupportedOperationException("setSeperator not supported");

    }

    @Override
    public void resetStream() {
        //nothing to do
    }

    @Override
    public void closeStream() {
        this.closeOutFile();
    }

    @Override
    void writeText(String text) {
        throw new UnsupportedOperationException("writeText not supported");

    }

    @Override
    void closeOutFile() {
        if (this.fout != null) {

            try {
                this.fout.flush();
                this.fout.close();
                this.fout = null;

            } catch (IOException ioexc) {
                LOGTURTLE.error("error closing outputfile",ioexc);
            }

        }
    }
}
