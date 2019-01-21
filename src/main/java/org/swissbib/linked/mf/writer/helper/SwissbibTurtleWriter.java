package org.swissbib.linked.mf.writer.helper;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

public class SwissbibTurtleWriter extends TurtleWriter {

    public SwissbibTurtleWriter(Writer out) {
        super(out, null);
    }

    @Override
    public void handleStatement(Statement st)
            throws RDFHandlerException
    {
        if (!writingStarted) {
            throw new RuntimeException("Document writing has not yet been started");
        }


        if (st.getPredicate().toString().contains("Literal"))
            //throw away all lsb related triples because they often contain rubbish making problems
            //in the context of Turtle serialization
            return;

        super.handleStatement(st);

    }

}
