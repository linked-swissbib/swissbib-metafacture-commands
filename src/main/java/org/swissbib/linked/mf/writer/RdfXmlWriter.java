package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract class for RDF-XML writer classes.
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 10.09.15
 */
public abstract class RdfXmlWriter<T> implements ConfigurableObjectWriter<T> {

    String header =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rdf:RDF " +
                    "xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" " +
                    "xmlns:void=\"http://rdfs.org/ns/void#\" " +
                    "xmlns:dbp=\"http://dbpedia.org/ontology/\" " +
                    "xmlns:dct=\"http://purl.org/dc/terms/\" " +
                    "xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" " +
                    "xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" " +
                    "xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" " +
                    "xmlns:bibo=\"http://purl.org/ontology/bibo/\" " +
                    "xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" " +
                    "xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" " +
                    "xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n " +
                    "<collection>\n";
    String footer = "</collection>\n</rdf:RDF>\n";
    String separator = DEFAULT_SEPARATOR;

    boolean firstObject = true;
    boolean closed;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    //is set by the previous module
    String documentHeader = null;

    //private String documentFooter = "</collection>\n</rdf:RDF>";
    Pattern bibliographicResource;
    Pattern biboDoc;
    Pattern item;
    Pattern work;
    Pattern person;
    Pattern organization;

    String baseOutDir = "/tmp";
    String outFilePrefix = "rdfxml1Line";
    int fileSize = 2000;
    String rootTag = "rdf:RDF";
    int numberLinesWritten = 0;

    String encoding = "UTF-8";

    BufferedWriter fout = null;


    public RdfXmlWriter()
    {

        this.bibliographicResource = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
        this.biboDoc = Pattern.compile("<bibo:Document.*?</bibo:Document>", Pattern.MULTILINE | Pattern.DOTALL);
        this.item = Pattern.compile("<bf:HeldItem.*?</bf:HeldItem>", Pattern.MULTILINE | Pattern.DOTALL);
        this.work = Pattern.compile("<bf:Work.*?</bf:Work>", Pattern.MULTILINE | Pattern.DOTALL);
        this.person = Pattern.compile("<foaf:Person.*?</foaf:Person>", Pattern.MULTILINE | Pattern.DOTALL);
        this.organization = Pattern.compile("<foaf:Organization.*?</foaf:Organization>", Pattern.MULTILINE | Pattern.DOTALL);

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

    public void setBaseOutDir (final String outDir) {
        this.baseOutDir = outDir;
        //this.openOutFile(filename);
    }

    public void setOutFilePrefix (final String filePrefix) {
        this.outFilePrefix = filePrefix;
    }

    public void setFileSize (final int fileSize) {
        this.fileSize = fileSize;
    }

    public void setRootTag (String rootTag) {
        this.rootTag = rootTag;
    }

    @Override
    public void resetStream() {
        firstObject = true;
    }

    @Override
    public void closeStream() {
        this.closeOutFile();
        closed = true;
    }

    public void process(T obj) {

        if (firstObject) {
            this.documentHeader = ((String) obj).replaceAll("[\n\r]", "").trim() + "\n";
            this.openOutFile();
            firstObject = false;
        } else {
            Matcher m = this.bibliographicResource.matcher((String) obj);
            if (m.find()) { trimmer(m); }

            m = this.biboDoc.matcher((String) obj);
            if (m.find()) { trimmer(m); }

            m = this.work.matcher((String) obj);
            if (m.find()) { trimmer(m); }

            m = this.person.matcher((String) obj);
            if (m.find()) { trimmer(m); }

            m = this.item.matcher((String) obj);
            if (m.find()) { trimmer(m); }

            m = this.organization.matcher((String) obj);
            if (m.find()) { trimmer(m); }
        }

    }

    public void trimmer(Matcher m) {
        String recordToStore = m.group().replaceAll("[\n\r]", "").trim() + "\n";
        this.writeText(recordToStore);
    }

    abstract void openOutFile();

    void closeOutFile () {

        if (this.fout != null) {
            try {
                this.writeText("</" + this.rootTag + ">") ;
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
    }

    abstract void writeText(String text);

}
