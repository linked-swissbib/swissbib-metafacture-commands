package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A formatter for multiline output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 */
@Description("Writes RDF/XML documents in outpufiles. Each document is written down in one line which makes it easer for further processing")
@In(Object.class)
@Out(Void.class)
public class SingleLineWriterRDFXml<T> extends CustomWriter<T> {

    protected boolean useContributor = false;
    String rootTag = "rdf:RDF";
    String footer = "</collection>\n</rdf:RDF>\n";


    public SingleLineWriterRDFXml() {
        this.bibliographicResource = Pattern.compile("<dct:BibliographicResource.*?</dct:BibliographicResource>", Pattern.MULTILINE | Pattern.DOTALL);
        this.biboDoc = Pattern.compile("<bibo:Document.*?</bibo:Document>", Pattern.MULTILINE | Pattern.DOTALL);
        this.item = Pattern.compile("<bf:HeldItem.*?</bf:HeldItem>", Pattern.MULTILINE | Pattern.DOTALL);
        this.work = Pattern.compile("<bf:Work.*?</bf:Work>", Pattern.MULTILINE | Pattern.DOTALL);
        this.person = Pattern.compile("<foaf:Person.*?</foaf:Person>", Pattern.MULTILINE | Pattern.DOTALL);
        this.organization = Pattern.compile("<foaf:Organization.*?</foaf:Organization>", Pattern.MULTILINE | Pattern.DOTALL);
        this.containsContributor = Pattern.compile("<dct:contributor.*?</dct:contributor>", Pattern.MULTILINE | Pattern.DOTALL);
    }

    public void setUsecontributor(final String contributor) {
        this.useContributor = Boolean.parseBoolean(contributor);
    }

    public void setRootTag(final String rootTag) {
        this.rootTag = rootTag;
    }


    public void trimmer(Matcher m) {
        String recordToStore = m.group().replaceAll("[\n\r]", "").trim() + "\n";
        this.writeText(recordToStore);
    }

    @Override
    void writeText(String text) {

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
    public void process(T obj) {

        if (firstObject) {
            this.documentHeader = ((String) obj).replaceAll("[\n\r]", "").trim() + "\n";
            this.openOutFile();
            firstObject = false;
        } else {
            Matcher m = this.bibliographicResource.matcher((String) obj);
            if (m.find()) {
                trimmer(m);
            }

            m = this.biboDoc.matcher((String) obj);
            if (m.find()) {
                trimmer(m);
            }

            m = this.work.matcher((String) obj);
            if (m.find()) {
                trimmer(m);
            }

            if (this.useContributor) {
                m = this.person.matcher((String) obj);
                if (m.find()) {
                    trimmer(m);
                }
            } else {
                m = this.person.matcher((String) obj);
                Matcher m1 = this.containsContributor.matcher((String) obj);
                if (m.find() && !m1.find()) {
                    trimmer(m);
                }
            }

            m = this.item.matcher((String) obj);
            if (m.find()) {
                trimmer(m);
            }

            m = this.organization.matcher((String) obj);
            if (m.find()) {
                trimmer(m);
            }
        }

    }

    @Override
    void closeOutFile() {

        if (this.fout != null) {
            try {
                this.writeText("</" + this.rootTag + ">");
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
    }

}
