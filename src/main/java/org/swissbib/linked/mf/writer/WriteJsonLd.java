
package org.swissbib.linked.mf.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.JsonLdUtils;
import com.github.jsonldjava.utils.JsonUtils;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;
import org.openrdf.rio.*;
import org.openrdf.rio.rdfxml.RDFXMLParser;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
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
public class WriteJsonLd<T> implements ConfigurableObjectWriter<T> {




    String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";
    String footer = "</collection>\n</rdf:RDF>\n";
    String separator = DEFAULT_SEPARATOR;

    boolean firstObject = true;
    boolean closed;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    //private String documentHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    //                        "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dbp=\"http://dbpedia.org/ontology/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n<collection>\n";

    //is set by the previous module
    String documentHeader = null;

    //private String documentFooter = "</collection>\n</rdf:RDF>";
    Pattern bibliographicResource;
    Pattern biboDoc;



    String baseOutDir = "/tmp";
    String outFilePrefix = "rdfxml1Line";
    String contextFile = "";
    int fileSize = 2000;
    String rootTag = "rdf:RDF";
    int numberFilesPerDirectory = 300;
    int currentSubDir = 1;
    int numberOpenedFiles = 0;
    int numberLinesWritten = 0;

    FileCompression compression = FileCompression.AUTO;
    String encoding = "UTF-8";

    private RDFXMLParser rdfParser = new RDFXMLParser();
    // private StringBuilder collector = new StringBuilder();



    BufferedWriter fout = null;

    boolean firstWrite = true;


    public WriteJsonLd()
    {

        this.bibliographicResource = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
        this.biboDoc = Pattern.compile("<bibo:Document.*?</bibo:Document>", Pattern.MULTILINE | Pattern.DOTALL);


    }


    public void setBaseOutDir (final String outDir) {
        this.baseOutDir = outDir;
        //this.openOutFile(filename);
    }

    public void setContextFile (final String contextFile) {
        this.contextFile = contextFile;
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
                this.convertToJsonLd(m.group(), "bibres");
            }

            m = this.biboDoc.matcher((String) obj);
            if (m.find()) {
                this.convertToJsonLd(m.group(), "bibo");
            }

        }

    }

    private void convertToJsonLd(String body, String type) {
        try {
            if (this.fout != null) {
                String input = this.documentHeader + body + "\n</rdf:RDF>";
                InputStream is = new ByteArrayInputStream(input.getBytes());
                // We need a sink for the Sesame transformation for json-ld can use i
                ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                RDFWriter rdfWriter = Rio.createWriter(RDFFormat.JSONLD, outStream);
                this.rdfParser.setRDFHandler(rdfWriter);
                try {
                    rdfParser.parse(is, "http://example.com");
                }
                catch (RDFParseException e) {
                    // handle unrecoverable parse error
                }
                catch (RDFHandlerException e) {
                    // handle a problem encountered by the RDFHandler
                }
                ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
                Object jsonObject = JsonUtils.fromInputStream(inStream);
                Map context = new ObjectMapper().
                        readValue(new File(this.contextFile), Map.class);
                JsonLdOptions options = new JsonLdOptions();
                Object jsonldTransformed = null;
                LinkedHashMap jsonldTruncated = new LinkedHashMap();
                switch (type) {
                    case "bibres":
                        jsonldTransformed = JsonLdProcessor.frame(jsonObject, context, options);
                        ArrayList jsonldGraph = (ArrayList) ((LinkedHashMap) jsonldTransformed).get("@graph");
                        for (Object temp : jsonldGraph) {
                            Set entrySet = ((LinkedHashMap)temp).entrySet();
                            Iterator it = entrySet.iterator();
                            while (it.hasNext())
                            {
                                Map.Entry<?,?> entry = (Map.Entry<?, ?>) it.next();
                                jsonldTruncated.put(entry.getKey(), entry.getValue());
                            }
                        }
                        jsonldTruncated.put("@context", ((LinkedHashMap) jsonldTransformed).get("@context"));
                        String idBibres = jsonldTruncated.get("@id").toString().substring(33,42);
                        this.fout.write(genBulkHeader("testsb", idBibres, "bibliographicResource"));
                        break;
                    case "bibo":
                        jsonldTruncated = (LinkedHashMap) JsonLdProcessor.compact(jsonObject, context, options);
                        String idBibo = jsonldTruncated.get("@id").toString().substring(33,42);
                        this.fout.write(genBulkHeader("testsb", idBibo, "document"));
                        break;
                }
                JsonUtils.write(this.fout, jsonldTruncated);
                this.fout.write("\n");
                this.numberLinesWritten++;
                if (this.numberLinesWritten >= this.fileSize) {
                    this.numberLinesWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                }
            }
        } catch (IOException ioExc) {
            System.out.println(ioExc.getMessage());
        } catch (JsonLdError jsonLdError) {
            jsonLdError.printStackTrace();
        }
    }


    protected static String genBulkHeader (String index, String id, String type) {
        return "{\"index\": {\"_index\": \"" + index + "\", \"_id\": \"" + id + "\", \"_type\": \"" + type + "\"}}\n";
    }


    void writeText (String text) {

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


    void closeOutFile () {

        if (this.fout != null) {
            try {
                // this.writeText("</" + this.rootTag + ">") ;
                // this.writeText("]");
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
    }

    void openOutFile () {

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
                String path = this.baseOutDir + File.separator + this.currentSubDir + File.separator +
                        this.outFilePrefix + "_" + ft.format(dNow) + ".jsonld.gz";
                final OutputStream file = new FileOutputStream(path);
                OutputStream compressor = compression.createCompressor(file, path);

                this.fout = new BufferedWriter(new OutputStreamWriter(compressor,this.encoding));
                // this.writeText(this.documentHeader);

            } else {
                this.fout = null;
            }


            if (this.fout != null) {
                this.numberOpenedFiles++;
            }

            //Todo: GH: Look up Exception Handlng in Metafacture Framework
            //hint: implementation of File opener in MF
        } catch (FileNotFoundException fnfEx) {
            System.out.println("file not Found");

        } catch (UnsupportedEncodingException usEnc) {
            System.out.println("UNsupportedEnding");
        }

    }



}
