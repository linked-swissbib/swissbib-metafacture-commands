package org.swissbib.linked.mf.writer.es;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by swissbib on 18.01.15.
 */


@Description("first step to write triples into a search engine index")
@In(Object.class)
@Out(Void.class)
public final class SearchEngineWriter<T> implements ConfigurableObjectWriter<T> {




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

    private Client client;
    private Node node;

    private PrefixMapping pm;




    private String header = DEFAULT_HEADER;
    private String footer = DEFAULT_FOOTER;
    private String separator = DEFAULT_SEPARATOR;

    private boolean firstObject = true;
    private boolean closed;

    private final String documentHeader = "<rdf:RDF xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:dbp=\"http://dbpedia.org/ontology/\">\n";
    private final String documentFooter = "</rdf:RDF>";

    private Map<String, String> keyMapping = null;

    public SearchEngineWriter() {

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
        map.put("dbp", "http://dbpedia.org/ontology/");

        this.pm = PrefixMapping.Factory.create();
        this.pm.setNsPrefixes(map);


        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "linked-swissbib").build();

        this.client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        //.addTransportAddress(new InetSocketTransportAddress("host2", 9300));

        this.init();

    }

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

                        if (predicates.size() > 0)

                            try {

                                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

                                for (String key : predicates.keySet()) {

                                    ArrayList<String> keyValues = new ArrayList<String>();
                                    for (RDFNode node : predicates.get(key)) {

                                        if (node.isURIResource()) {
                                            keyValues.add(node.asResource().getURI());
                                        } else if (node.isLiteral()){
                                            keyValues.add(node.asLiteral().toString());
                                        }
                                    }
                                    builder.field(this.keyMapping.containsKey(key) ? this.keyMapping.get(key) : key, keyValues);
                                }

                                builder.field("fullRecord", recordToStore);

                                IndexRequestBuilder requestBuilder = client.prepareIndex("swissbib", "RDF");
                                requestBuilder.setId(id);
                                requestBuilder.setCreate(false);

                                requestBuilder.setSource(builder);
                                IndexResponse response = requestBuilder.execute().actionGet();

                                //todo: check the response

                            } catch (IOException ioException) {
                                System.out.println(ioException.getMessage());

                            }

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
        closed = true;

    }

    private void init() {

        this.keyMapping = new HashMap<String, String>();
        this.keyMapping.put("http://rdaregistry.info/Elements/u/contentType","contentType");
        this.keyMapping.put("http://rdaregistry.info/Elements/u/mediaType","mediaType");
        this.keyMapping.put("http://purl.org/dc/terms/language","language");
        this.keyMapping.put("http://purl.org/dc/terms/issued","isssued");
        this.keyMapping.put("http://rdaregistry.info/Elements/u/placeOfPublication","placeOfPublication");
        this.keyMapping.put("http://purl.org/ontology/bibo/isbn13","isbn13");
        this.keyMapping.put("http://purl.org/ontology/bibo/isbn10","isbn10");
        this.keyMapping.put("http://purl.org/ontology/bibo/issn","issn");
        this.keyMapping.put("http://purl.org/dc/elements/1.1/contributor","contributor");
        this.keyMapping.put("http://purl.org/dc/terms/title","title");
        this.keyMapping.put("http://purl.org/dc/terms/alternative","alternative");
        this.keyMapping.put("http://purl.org/ontology/bibo/edition","edition");
        this.keyMapping.put("http://rdaregistry.info/Elements/u/publicationStatement","publicationStatement");
        this.keyMapping.put("http://purl.org/dc/elements/1.1/format","format");
        this.keyMapping.put("http://purl.org/dc/terms/bibliographicCitation","bibliographicCitation");
        this.keyMapping.put("http://rdaregistry.info/Elements/u/noteOnResource", "noteOnResource");
        this.keyMapping.put("http://rdaregistry.info/Elements/u/dissertationOrThesisInformation", "dissertationOrThesisInformation");
        this.keyMapping.put("http://purl.org/dc/terms/hasPart", "hasPart");
        this.keyMapping.put("http://purl.org/dc/terms/subject", "subject");
        this.keyMapping.put("http://www.w3.org/2000/01/rdf-schema#isDefinedBy", "isDefinedBy");
        this.keyMapping.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "resourceType");


    }
}
