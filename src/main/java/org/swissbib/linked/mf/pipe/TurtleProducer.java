package org.swissbib.linked.mf.pipe;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultObjectPipe;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;


/**
 * Transformation Json-LD to Turtle.
 * Should be generalised so we can transform any serialisation format into any other
 *
 * @author Günter Hipler, project swissbib, Basel
 */
@Description("Serialise JSon-LD to turtle")
@In(String.class)
@Out(String.class)
public class TurtleProducer extends DefaultObjectPipe<String, ObjectReceiver<String>> {

    private RDFParser rdfParser;
    //private RDFWriter rdfWriter;
    private StringWriter sw;

    private int numberRecords = 0;

    //private StringBuilder jsonLdRecords = new StringBuilder();

    private ArrayList<String> jsonRecords = new ArrayList<>();

    public TurtleProducer() {

        sw = new StringWriter();
        rdfParser = Rio.createParser(RDFFormat.JSONLD);
        RDFWriter rdfWriter = Rio.createWriter(RDFFormat.TURTLE,sw );

        rdfParser.setRDFHandler(rdfWriter);

    }


    @Override
    public void process(String obj) {

        //todo: transform to Turtle
        this.numberRecords++;
        this.jsonRecords.add(obj);

        if (numberRecords == 3) {
            transformTurtle(makeJson());
        }


        super.process(obj);
    }

    private void transformTurtle(StringReader sr) {

        try {
            //java.net.URL documentUrl = new URL("http://example.org/example.ttl");

            //StringReader sr = new StringReader();
            rdfParser.parse(sr, "");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(sw.toString());
    }


    private StringReader makeJson() {

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int i = 1;
        for (String s : this.jsonRecords) {
            sb.append(s);
            if (i < this.jsonRecords.size()) {
                sb.append(",");
            }
            i++;

        }

        sb.append("]");

        return new StringReader(sb.toString());

    }


}
