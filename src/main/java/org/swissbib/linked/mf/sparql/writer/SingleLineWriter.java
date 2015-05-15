package org.swissbib.linked.mf.sparql.writer;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by swissbib on 4/28/15.
 */
public class SingleLineWriter <T> implements ConfigurableObjectWriter<T> {




    private String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";
    private String footer = "</collection>\n</rdf:RDF>\n";
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;

    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    private final String documentHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";
    private final String documentFooter = "</collection>\n</rdf:RDF>";
    private Pattern bibliographicResource;
    private Pattern biboDoc;

    private PrefixMapping pm;

    private String filename = null;

    private String defaultFileName = "/tmp/rdf-one-line.xml";

    private BufferedWriter fout = null;

    private boolean firstWrite = true;


    public SingleLineWriter ()
    {

        this.bibliographicResource = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
        this.biboDoc = Pattern.compile("<bibo:Document.*?</bibo:Document>", Pattern.MULTILINE | Pattern.DOTALL);


    }




    public void setOutFile (final String filename) {
        this.filename = filename;
        this.openOutFile(filename);

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
        if (this.fout == null) {
            this.openOutFile(this.defaultFileName);
        }



        if (firstObject) {
            this.writeText(this.documentHeader);
            firstObject = false;
        } else {

            Matcher m = this.bibliographicResource.matcher((String) obj);
            if (m.find()) {
                String recordToStore =  m.group().replaceAll("[\n\r]", "").trim() + "\n" ;
                this.writeText(recordToStore);
            }

            m = this.biboDoc.matcher((String) obj);
            if (m.find()) {
                String recordToStore =  m.group().replaceAll("[\n\r]", "").trim() + "\n" ;
                this.writeText(recordToStore);
            }



        }

    }


    private void writeText (String text) {

        try {
            this.fout.write(text);
            this.fout.flush();
        } catch (IOException ioExc) {

            System.out.println(ioExc.getMessage());

        }
    }

    @Override
    public void resetStream() {
        firstObject = true;
    }

    @Override
    public void closeStream() {
        if (!firstObject) {
            this.writeText(this.documentFooter);

        }

        if (this.fout != null) {
            try {
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
        closed = true;

    }

    private void openOutFile (String pathAndfilename) {

        try {

            this.fout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pathAndfilename), "UTF-8"));
        } catch (FileNotFoundException fnfEx) {

            System.out.println("file not Found");
            //throw new Exception(fnfEx);

        } catch (UnsupportedEncodingException usEnc) {
            System.out.println("UNsupportedEnding");
        }

    }



}
