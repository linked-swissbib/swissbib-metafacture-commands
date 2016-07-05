package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Elasticsearch Bulk API compliant output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class NeoWriter<T> implements ConfigurableObjectWriter<T> {

    private final static Logger LOG = LoggerFactory.getLogger(NeoWriter.class);
    private CsvWriterRegister register;
    private String nodeLabel;
    private boolean firstRecord = true;
    private String csvDir;
    private int csvFileLength = 1000000;
    private int batchWriteSize = 1500;


    public void setCsvDir(String csvDir) {
        this.csvDir = csvDir;
    }

    public void setCsvFileLength(String csvFileLength) {
        this.csvFileLength = Integer.parseInt(csvFileLength);
    }

    public void setBatchWriteSize(String batchWriteSize) {
        this.batchWriteSize = Integer.parseInt(batchWriteSize);
    }

    public void process(T obj) {
        if (firstRecord) {
            register = new CsvWriterRegister(csvDir, csvFileLength, batchWriteSize);
            firstRecord = false;
        }
        String rawNodeLabel = ((String) obj).split("#")[0];
        this.nodeLabel = rawNodeLabel.substring(1, rawNodeLabel.length() - 1);
        deserializer((String) obj);
    }

    private void deserializer(String s) {
        int counter = 0;
        for (String csvLines : s.split("\\|\\|")) {
            // Set limit of returned strings to 2 (we don't know if there are another # in text)
            String[] csvTokens = csvLines.split("#", 2);
            String filename;
            String content;
            // If counter == 0, it's a node, if not, it's a relation. Relations have a filename of pattern node1-node2,
            // nodes of pattern node
            if (counter == 0) {
                filename = nodeLabel;
                content = csvTokens[1] + "," + csvTokens[0] + "\n";
            } else {
                filename = nodeLabel + "-" + csvTokens[0];
                content = csvTokens[1] + "\n";
            }
            writeText(filename, content);
            counter++;
        }
    }


    private void writeText(String name, String content) {
        BufferedCsvWriter writer = register.ask(name);
        writer.append(content);
    }


    @Override
    public String getEncoding() {
        return null;
    }

    @Override
    public void setEncoding(String s) {

    }

    @Override
    public FileCompression getCompression() {
        return null;
    }

    @Override
    public void setCompression(FileCompression fileCompression) {

    }

    @Override
    public void setCompression(String s) {

    }

    @Override
    public String getHeader() {
        return null;
    }

    @Override
    public void setHeader(String s) {

    }

    @Override
    public String getFooter() {
        return null;
    }

    @Override
    public void setFooter(String s) {

    }

    @Override
    public String getSeparator() {
        return null;
    }

    @Override
    public void setSeparator(String s) {

    }

    @Override
    public void resetStream() {
        register.close();

    }

    @Override
    public void closeStream() {
        register.close();
    }
}
