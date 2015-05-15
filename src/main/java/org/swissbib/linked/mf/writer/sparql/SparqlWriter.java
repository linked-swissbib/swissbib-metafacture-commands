package org.swissbib.linked.mf.writer.sparql;

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
 * Created by swissbib on 18.03.15.
 */
public final class SparqlWriter <T> implements ConfigurableObjectWriter<T> {

    public static enum Format {
        RDF_XML("RDF/XML"), RDF_XML_ABBREV("RDF/XML-ABBREV"), N_TRIPLE("N-TRIPLE"), N3(
                "N3"), TURTLE("TURTLE");

        private final String name;

        Format(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }



    private String header = DEFAULT_HEADER;
    private String footer = DEFAULT_FOOTER;
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;

    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    private final String documentHeader = "<rdf:RDF xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\">\n";
    private final String documentFooter = "</rdf:RDF>";

    private PrefixMapping pm;

    private String filename = null;

    private String defaultFileName = "/tmp/linked-swissbib.ttl";

    private BufferedWriter fout = null;

    private boolean firstWrite = true;



    public SparqlWriter () {

        Map<String, String> map = new HashMap<String, String>();

        map.put("bibo", "http://purl.org/ontology/bibo/");
        map.put("dc", "http://purl.org/dc/elements/1.1/");
        map.put("dct", "http://purl.org/dc/terms/");
        map.put("foaf", "http://xmlns.com/foaf/0.1/");
        map.put("owl", "http://www.w3.org/2004/02/skos/core#");
        map.put("rdau", "http://rdaregistry.info/Elements/u/");
        map.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        map.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        map.put("skos", "http://www.w3.org/2004/02/skos/core#");
        map.put("void", "http://rdfs.org/ns/void#");

        this.pm = PrefixMapping.Factory.create();
        this.pm.setNsPrefixes(map);



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
            //System.out.print(getHeader());
            firstObject = false;
        } else {

            //at the moment I look only for dct:BibliographicResource resources and not for bibo:Document

            try {


                Pattern p = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
                Matcher m = p.matcher((String) obj);
                if (m.find()) {


                    String recordToStore = this.documentHeader + m.group() + this.documentFooter;

                    Pattern pID = Pattern.compile("rdf:about=\"http://data\\.swissbib\\.ch/resource/(.*?)\"", Pattern.MULTILINE | Pattern.DOTALL);
                    Matcher mID = pID.matcher(recordToStore);

                    String id = "";
                    if (mID.find()) {
                        id = mID.group(1);
                    }

                    Resource subject;

                    Map<String, ArrayList<RDFNode>> predicates;

                    final Model model = ModelFactory.createDefaultModel();

                    model.setNsPrefixes(pm);
                    model.read(new StringReader(recordToStore), null, Format.RDF_XML.getName());

                    ResIterator resIter = model.listSubjects();

                    while (resIter.hasNext()) {
                        subject = resIter.next();

                        Model subjectModel = subject.getModel();
                        predicates = new HashMap<String, ArrayList<RDFNode>>();
                        StmtIterator iterator = subjectModel.listStatements();
                        while (iterator.hasNext()) {

                            Statement st = iterator.next();
                            if (predicates.containsKey(st.getPredicate().getURI())) {
                                predicates.get(st.getPredicate().getURI()).add(st.getObject());
                            } else {
                                predicates.put(st.getPredicate().getURI(), new ArrayList<RDFNode>());
                                predicates.get(st.getPredicate().getURI()).add(st.getObject());
                            }

                        }

                        StringWriter sw = new StringWriter();

                        RDFDataMgr.write(sw, model, Lang.TURTLE);

                        if (this.firstWrite) {

                            this.fout.write(sw.toString());
                            this.fout.flush();
                            this.firstWrite = false;


                        } else {


                            Matcher matcher =
                                    Pattern.compile("(^@prefix.*?\n$)", Pattern.MULTILINE | Pattern.DOTALL).matcher(sw.toString());

                            String output = matcher.replaceAll("");

                            //now remove the empty lines
                            matcher =
                                    Pattern.compile("^$", Pattern.MULTILINE | Pattern.DOTALL).matcher(output);


                            output = matcher.replaceAll("");
                            this.fout.write(output);
                            this.fout.flush();
                        }
                        //System.out.println();

                    }

                }
            } catch (Throwable throwable) {
                System.out.print(throwable.getMessage());
                System.out.println((String) obj);
            }
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
