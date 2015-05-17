package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A formatter for multiline output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 *
 */
@Description("Writes RDF/XML documents in outpufiles. Each document is written down in one line which makes it easer for further processing")
@In(Object.class)
@Out(Void.class)
public class SingleLineWriterRDFXml<T> implements ConfigurableObjectWriter<T> {




    private String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";
    private String footer = "</collection>\n</rdf:RDF>\n";
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;

    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    //private String documentHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    //                        "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";

    //is set by the previous module
    private String documentHeader = null;

    //private String documentFooter = "</collection>\n</rdf:RDF>";
    private Pattern bibliographicResource;
    private Pattern biboDoc;



    private String baseOutDir = "/tmp";
    private String outFilePrefix = "rdfxml1Line";
    private int fileSize = 2000;
    private String rootTag = "rdf:RDF";
    private int numberFilesPerDirectory = 300;
    private int currentSubDir = 1;
    private int numberOpenedFiles = 0;
    private int numberLinesWritten = 0;

    private FileCompression compression = FileCompression.AUTO;
    private String encoding = "UTF-8";



    private BufferedWriter fout = null;

    private boolean firstWrite = true;


    public SingleLineWriterRDFXml()
    {

        this.bibliographicResource = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
        this.biboDoc = Pattern.compile("<bibo:Document.*?</bibo:Document>", Pattern.MULTILINE | Pattern.DOTALL);


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

    public void setFilesPerDir(int numberFilesPerDirectory) {
        this.numberFilesPerDirectory = numberFilesPerDirectory;
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
            this.documentHeader = ((String)obj).replaceAll("[\n\r]", "").trim() + "\n";
            this.openOutFile();
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
            if (this.fout != null) {
                this.fout.write(text);
                this.numberLinesWritten++;
                if (this.numberLinesWritten >= this.fileSize) {
                    this.numberLinesWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                }
            }
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

        this.closeOutFile();
        closed = true;

    }


    private void closeOutFile () {

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

    private void openOutFile () {

        try {


            boolean subDirexists = true;
            File subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
            if (!subDir.exists()) {

                 subDirexists = subDir.mkdir();
            } else if (this.numberFilesPerDirectory <= this.numberOpenedFiles) {
                this.currentSubDir++;
                subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
                if (!subDir.exists()) {
                    subDirexists = subDir.mkdir();
                }
                this.numberOpenedFiles = 0;
            }

            Date dNow = new Date( );
            SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String path = this.baseOutDir + File.separator + this.currentSubDir + File.separator + this.outFilePrefix + "_" + ft.format(dNow) + ".xml.gz";
                final OutputStream file = new FileOutputStream(path);
                OutputStream compressor = compression.createCompressor(file, path);

                this.fout = new BufferedWriter(new OutputStreamWriter(compressor,this.encoding));
                this.writeText(this.documentHeader);

            } else {
                this.fout = null;
            }


            if (this.fout != null) {
                this.numberOpenedFiles++;
            }

            this.writeText(this.documentHeader);

            //Todo: GH: Look up Exception Handlng in Metafacture Framework
            //hint: implementation of File opener in MF
        } catch (FileNotFoundException fnfEx) {
            System.out.println("file not Found");

        } catch (UnsupportedEncodingException usEnc) {
            System.out.println("UNsupportedEnding");
        }

    }



}
