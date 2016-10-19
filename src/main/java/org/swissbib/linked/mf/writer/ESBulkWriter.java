package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import java.io.IOException;

/**
 * An Elasticsearch Bulk API compliant output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class ESBulkWriter<T> extends CustomWriter<T> {

    private Boolean jsonCompliant = false;
    private int numberRecordsWritten = 0;

    public void setJsonCompliant(final String jsonCompliant) {
        this.jsonCompliant = Boolean.parseBoolean(jsonCompliant);
        LOG.debug("Settings - Is output valid JSON? {}", this.jsonCompliant);
    }


    @Override
    public void process(T obj) {

        if (firstObject) {
            this.documentHeader = (String) obj;
            this.openOutFile();
            firstObject = false;
        } else {
            this.writeText((String) obj);
        }

    }

    @Override
    void writeText(String text) {

        try {
            if (this.fout != null) {
                this.numberRecordsWritten++;
                LOG.debug("Writing record {} in file", numberRecordsWritten);
                if (jsonCompliant) {
                    if (numberRecordsWritten > 1) fout.write((",\n"));
                    text = text.substring(0, text.length() - 1);
                }
                if (!text.equals("{}\n")) this.fout.write(text);
                if (this.numberRecordsWritten >= this.fileSize) {
                    this.numberRecordsWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                }
            }
        } catch (IOException ioExc) {
            LOG.error(ioExc.getMessage());
        }
    }


    @Override
    void openOutFile() {
        super.openOutFile();
        try {
            if (jsonCompliant) this.fout.write("[\n");
            this.writeText(this.documentHeader);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    void closeOutFile() {
        if (this.fout != null) {
            try {
                if (jsonCompliant) this.fout.write("\n]");
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                LOG.error("IO exception while output file should be closed: {}", ioEx);
            }
        }
    }

}
