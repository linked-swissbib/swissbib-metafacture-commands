package org.swissbib.linked;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.shared.PrefixMapping;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by swissbib on 19.01.15.
 */



public class JenaTesting {

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

    private static PrefixMapping pm;

    static {

        //is this necessary?
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

        pm = PrefixMapping.Factory.create();
        pm.setNsPrefixes(map);

    }



    public static void main (String [] args) {




        final Model model = ModelFactory.createDefaultModel();

        model.setNsPrefixes(pm);
        model.read(new StringReader(getTestRecord()), null, Format.RDF_XML.getName());

        ResIterator resIter = model.listSubjects();

        while (resIter.hasNext()) {
            Resource resource = resIter.next();
            String uriResource = resource.getURI();
            //AnonId anonId = resource.getId();
            //String labelStringOfSubject = anonId.getLabelString();
            //String toStringOfAnon = anonId.toString();

            //todo: Namespace und localName werden falsch ermittelt
            //Problem des fehlenden Modells?
            String localNameOfResource = resource.getLocalName();
            boolean isUriResource = resource.isURIResource();
            String namespaceOfResource = resource.getNameSpace();

            Model subjectModel = resource.getModel();

            StmtIterator iterator = subjectModel.listStatements();
            while (iterator.hasNext()) {

                Statement st = iterator.next();
                //Resource subjectOfStatement  = st.getSubject();
                System.out.println("uri of predicate " + st.getPredicate().getURI());
                RDFNode objectRDFNode = st.getObject();
                if (objectRDFNode.isURIResource()) {

                    System.out.println("uri of object: " + objectRDFNode.asResource().getURI());
                    //Resource obejctAsResource = objectRDFNode.asResource();
                    //String test1 = "";
                } else {
                    System.out.println( "literal value of object: " + objectRDFNode.asLiteral().toString());
                }


            }




            /*
            boolean isUriRessource = rdfNode.isURIResource();
            boolean isRessource = rdfNode.isResource();
            String literal = rdfNode.asLiteral().toString();
            StmtIterator iterator = rdfNode.getModel().listStatements();
            while (iterator.hasNext()) {

                Statement st = iterator.next();
                System.out.println(st.getPredicate().toString());

            }
            */

        }









    }


    private static String getTestRecord () {

        return "<rdf:RDF xmlns:rdau=\"http://rdaregistry.info/Elements/u/\" xmlns:void=\"http://rdfs.org/ns/void#\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:owl=\"http://www.w3.org/2004/02/skos/core#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" xmlns:bibo=\"http://purl.org/ontology/bibo/\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n" +
                "<dct:BibliographicResource rdf:about=\"http://data.swissbib.ch/resource/31602208X\">\n" +
                "<rdfs:isDefinedBy rdf:resource=\"http://data.swissbib.ch/resource/31602208X/about\" />\n" +
                "<rdau:placeOfPublication rdf:resource=\"http://sws.geonames.org/2921044/\" />\n" +
                "\t\t\t<bibo:isbn13>9783825239886</bibo:isbn13>\n" +
                "\t\t\t<bibo:isbn10>3825239888</bibo:isbn10>\n" +
                "\t\t\t<dct:alternative>Spain literature history</dct:alternative>\n" +
                "\t\t\t<dct:alternative>Historia de la literatura española</dct:alternative>\n" +
                "\t\t\t<dct:bibliographicCitation>UTB. Romanistik, Literaturwissenschaft</dct:bibliographicCitation>\n" +
                "\t\t\t<dct:bibliographicCitation>UTB ; 3988</dct:bibliographicCitation>\n" +
                "\t\t\t<dct:hasPart>Tome 1. A-D / Par Charles Laboulaye et MM. Debette, Barral</dct:hasPart>\n" +
                "\t\t\t<dct:hasPart>Tome 2. E-M / Par Charles Laboulaye et MM. Debette, Barral</dct:hasPart>\n" +
                "\t\t\t<dct:hasPart>Tome 3. N-Z / Par Charles Laboulaye et MM. Debette, Barral</dct:hasPart>\n" +
                "\t\t\t<dct:hasPart>Tome 4. Complément, avec le concours de savants, d&apos;ingénieurs et de fabricants / Par Charles Laboulaye et MM. Debette, Barral</dct:hasPart>\n" +
                "\t\t\t<dct:subject rdf:resource=\"http://d-nb.info/gnd/4077640-2\" />\n" +
                "\t\t\t<dct:subject rdf:resource=\"http://d-nb.info/gnd/4035964-5\" />\n" +
                "\t\t\t<dct:subject rdf:resource=\"http://d-nb.info/gnd/4035964-5\" />\n" +
                "\t\t\t<dc:contributor>Rivero Iglesias, Carmen</dc:contributor>\n" +
                "\t\t\t<rdau:contentType rdf:resource=\"http://rdvocab.info/termList/RDAContentType/1020\" />\n" +
                "\t\t\t<rdau:mediaType rdf:resource=\"http://rdvocab.info/termList/RDAMediaType/1007\" />\n" +
                "\t\t\t<dct:language rdf:resource=\"http://lexvo.org/id/iso639-3/deu\" />\n" +
                "\t\t\t<dct:issued>2014</dct:issued>\n" +
                "\t\t\t<rdau:placeOfPublication rdf:resource=\"http://sws.geonames.org/2661603/\" />\n" +
                "\t\t\t<dct:title>Spanische Literaturgeschichte : eine kommentierte Anthologie / hrsg. von Carmen Rivero Iglesias</dct:title>\n" +
                "\t\t\t<rdau:publicationStatement>Paderborn : Fink, 2014</rdau:publicationStatement>\n" +
                "\t\t\t<dc:format>380 S</dc:format>\n" +
                "\t\t</dct:BibliographicResource>\n" +
                "</rdf:RDF>";

    }


}
