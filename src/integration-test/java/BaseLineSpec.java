import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.metafacture.biblio.marc21.MarcXmlHandler;
import org.metafacture.io.FileOpener;
import org.metafacture.mangling.RecordIdChanger;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.xml.XmlDecoder;
import org.swissbib.linked.mf.pipe.ESBulkEncoder;

import static org.junit.Assert.assertEquals;


public class BaseLineSpec {

    private final ClassLoader classLoader = getClass().getClassLoader();

    private final FileOpener fileOpener = new FileOpener();
    private final XmlDecoder xmlDecoder = new XmlDecoder();
    private final MarcXmlHandler marcXmlHandler = new MarcXmlHandler();
    private final Filter filter = new Filter(new Metamorph(classLoader.getResourceAsStream("sources/245aFilter.xml")));
    private final Metamorph metamorph = new Metamorph(classLoader.getResourceAsStream("sources/resourceMorph.xml"));
    private final RecordIdChanger recordIdChanger = new RecordIdChanger();
    private final ESBulkEncoder esBulkEncoder = new ESBulkEncoder();
    private final MockESWriter mockEsWriter = new MockESWriter();


    @Before
    public void setup() {

        this.esBulkEncoder.setEscapeChars("true");
        this.esBulkEncoder.setHeader("true");
        this.esBulkEncoder.setIndex("lsb");
        this.esBulkEncoder.setType("bibliographicResource");

        this.mockEsWriter.setJsonCompliant(false);

        this.fileOpener
                .setReceiver(xmlDecoder)
                .setReceiver(marcXmlHandler)
                .setReceiver(filter)
                .setReceiver(metamorph)
                .setReceiver(recordIdChanger)
                .setReceiver(esBulkEncoder)
                .setReceiver(mockEsWriter);
        this.fileOpener.process(classLoader.getResource("sources/swissbibMarc.xml").getFile());
    }


    @Test
    public void testResourceCreationJsonCompliant() {
        String jsonCompliant = "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000027\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000027\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000027/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"rdau:P60339\":\"Historisches Museum Frankfurt\",\"rdau:P60333\":\"Frankfurt a.M : [s.n.], 1975-1976\",\"dc:format\":\"1 Ordner : Ill\",\"dct:hasPart\":[\"[Teil 1]. Historische Dokumentation 8.-15. Jahrhundert. Informationsblätter der Abteilungen 1-4. 1976\",\"[Teil 2]. Historische Dokumentation 16.-18. Jahrhundert. Informationsblätter der Abteilungen 5-26. 1975\",\"[Teil 3]. Historische Dokumentation 19. Jahrhundert. Informationsblätter der Abteilungen 27-43. 1975\",\"[Teil 4]. Historische Dokumentation 20. Jahrhundert. Informationsblätter der Abteilungen 50-55. 1976\"],\"dct:contributor\":\"http://data.swissbib.ch/person/63e2561e-ad2d-3d4f-8fa8-7b93a9093b14\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1975-1976\",\"dct:title\":\"Historische Dokumentation : Informationsblätter der Abteilungen\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000051\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000051\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000051/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"dbp:originalLanguage\":\"http://lexvo.org/id/iso639-3/fra\",\"dct:contributor\":[\"http://data.swissbib.ch/person/49108198-9c7e-3a95-b369-4e60a1fab9be\",\"http://data.swissbib.ch/person/4882e3fd-3715-3424-93d6-4bf0605a775c\",\"http://data.swissbib.ch/person/1d4e773d-2f54-34d2-b495-cf0544eb3add\"],\"rdau:P60339\":\"Text: Thomas Mosdi ; Zeichnungen und Kolorierung : Civiello ; [Übersetzung: Resel Rebiersch]\",\"rdau:P60333\":\"[O.O.] : Kult Editionen, 2005\",\"dc:format\":\"47 S : Ill\",\"dct:bibliographicCitation\":\"Korrigans ; Bd. 2. \",\"rdau:P60470\":\"Legenden der Welt\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"2005\",\"dct:title\":\"Die Krieger der Finsternis\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"00000006X\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/00000006X\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/00000006X/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"dct:contributor\":\"http://data.swissbib.ch/person/0ce4d976-9c46-3923-a659-f1ecb1495f2e\",\"rdau:P60339\":\"Joh. Georg Brückner\",\"rdau:P60333\":\"Gotha : Mevius, 1753-1760\",\"dc:format\":\"3 Bde ; 4°\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1753-1760\",\"dct:title\":\"Sammlung verschiedener Nachrichten zu e. Beschreibung d. Kirchen- u. Schulenstaats im Herzogth. Gotha\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000078\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000078\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000078/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"bibo:isbn13\":\"9783980562126\",\"bibo:isbn10\":\"3980562123\",\"rdau:P60339\":\"hrsg. von Angelika Mundorff ... [et al.]\",\"rdau:P60333\":\"Fürstenfeldbruck : Stadtmuseum, 1997\",\"dc:format\":\"227 S : Ill\",\"rdau:P60470\":\"Ausstellung (Fürstenfeldbruck ; Stadtmuseum ; 1997)\",\"dct:contributor\":\"http://data.swissbib.ch/person/f6a3b2ec-c93e-352b-bd39-92fbba17f734\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1997\",\"dct:title\":\"Kaiser Ludwig der Bayer 1282-1347 : Katalog zur Ausstellung im Stadtmuseum Fürstenfeldbruck, 25. Juli bis 12. Oktober 1997\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000094\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000094\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000094/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"bibo:isbn13\":\"9783129270295\",\"bibo:isbn10\":\"3129270299\",\"dct:contributor\":[\"http://data.swissbib.ch/person/28b361c1-b582-35d7-8e0e-e970900614c8\",\"http://data.swissbib.ch/person/d07ce56f-cca7-3537-b9df-c0ca5c2c2afe\",\"http://data.swissbib.ch/person/93de5a8b-2e8d-3547-a0ae-1e5f1f466828\"],\"rdau:P60339\":\"Hans Bergmann, Uwe Bergmann, Horst Steibl\",\"rdau:P60333\":\"Stuttgart : Klett, 2004\",\"dc:format\":\"223 S : Ill\",\"work\":\"000000094\",\"bf:instanceOf\":\"http://data.swissbib.ch/work/000000094\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"2004\",\"dct:title\":\"Training Mathematik. 8. Schuljahr : Termumformungen, Lineare Funktionen und Gleichungen, Dreiecke, Flächeninhalte\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000108\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000108\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000108/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"dct:contributor\":\"http://data.swissbib.ch/person/1c6d3da1-9a69-3335-8d89-16601aca1ce5\",\"rdau:P60339\":\"F. Brenner\",\"rdau:P60333\":\"Bamberg : Goebhardt, 1815-1818\",\"dc:format\":\"3 Bde\",\"work\":\"000000108\",\"bf:instanceOf\":\"http://data.swissbib.ch/work/000000108\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1815-1818\",\"dct:title\":\"Freye Darstellung der Theologie in der Idee des Himmelreichs: oder Neueste katholische Dogmatik nach den Bedürfnissen unserer Zeit\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000116\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000116\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000116/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"dct:contributor\":\"http://data.swissbib.ch/person/1dd029e5-f8e4-3343-b65f-26197929ddd6\",\"rdau:P60339\":\"F. Brenner\",\"rdau:P60333\":\"Bamberg : Geobhardt, 1816\",\"dc:format\":\"Bd. 2\",\"work\":\"000000108\",\"bf:instanceOf\":\"http://data.swissbib.ch/work/000000108\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1816\",\"dct:title\":\"Freye Darstellung der Theologie in der Idee des Himmelreichs, oder: neueste katholische Dogmatik nach den Bedürfnissen unserer Zeiten\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000132\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000132\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000132/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/eng\",\"rdau:P60163\":\"http://sws.geonames.org/2635167/\",\"bibo:isbn13\":\"9780194577809\",\"bibo:isbn10\":\"0194577805\",\"dct:contributor\":[\"http://data.swissbib.ch/person/18e061e6-e526-3306-9aa5-8d4dfbe43e7e\",\"http://data.swissbib.ch/person/2b142e3d-e90e-37f9-9e6b-22a6008968c5\"],\"rdau:P60339\":\"David Grant, Robert McLarty\",\"rdau:P60333\":\"Oxford : Oxford University Press, 2006\",\"dc:format\":\"175 p : Ill + + 1 Beil. (1 CD-ROM)\",\"rdau:P60470\":\"International edition\",\"work\":\"000000132\",\"bf:instanceOf\":\"http://data.swissbib.ch/work/000000132\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"2006\",\"dct:title\":\"Business basics : updated for the international marketplace. [Workbook]\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000140\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000140\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000140/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2658434/\",\"dct:contributor\":\"http://data.swissbib.ch/person/8046d763-7dec-3e40-b43d-242c143aa389\",\"rdau:P60339\":\"Hans Brugger\",\"rdau:P60333\":\"Weinfelden : s.n, 1971\",\"dc:format\":\"S 65-100 : Tab., Diagr\",\"rdau:P60470\":\"Sonderdruck aus: Festgabe zum hundertjährigen Bestehen der Thurgauischen Kantonalbank 1871-1971\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1971\",\"dct:title\":\"Landwirtschaft des Kontons Thurgau\"}\n" +
        "{\"index\":{\"_type\":\"bibliographicResource\",\"_index\":\"lsb\",\"_id\":\"000000159\"}}\n" +
        "{\"@type\":\"http://purl.org/dc/terms/BibliographicResource\",\"@context\":\"https://resources.swissbib.ch/bibliographicResource/context.jsonld\",\"@id\":\"http://data.swissbib.ch/bibliographicResource/000000159\",\"rdfs:isDefinedBy\":\"http://data.swissbib.ch/bibliographicResource/000000159/about\",\"dct:language\":\"http://lexvo.org/id/iso639-3/deu\",\"rdau:P60163\":\"http://sws.geonames.org/2921044/\",\"rdau:P60339\":\"Kabinett der Gegenwart Kaisertrutz\",\"dct:alternative\":\"Ausstellung technischer Kulturdenkmale\",\"rdau:P60333\":\"[Görlitz] : [s.n.], 1952\",\"dc:format\":\"112 S : Ill\",\"dct:bibliographicCitation\":[\"Schriftenreihe der Städtischen Kunstsammlungen Görlitz. Neue Folge ; 2. \",\"Technische Kulturdenkmale Sachsens ; Heft 1. \"],\"rdau:P60470\":\"Ausstellung (Görlitz ; Kulturhistorisches Museum Kaisertrutz ; 1952)\",\"dct:contributor\":\"http://data.swissbib.ch/organisation/12d09c4e-1cda-335b-91a2-95cbbe22f839\",\"rdau:P60049\":\"http://rdvocab.info/termList/RDAContentType/1020\",\"rdau:P60050\":\"http://rdvocab.info/termList/RDAMediaType/1007\",\"rdf:type\":\"http://purl.org/ontology/bibo/Book\",\"dct:issued\":\"1952\",\"dct:title\":\"Zeichnungen aus dem Planarchiv des Landesamtes für Volkskunde und Denkmalpflege und Modelle aus sächsischen Museen\"}\n";
        assertEquals(jsonCompliant, mockEsWriter.getBuffer());
    }


    @After
    public void cleanup() {
    }

}
