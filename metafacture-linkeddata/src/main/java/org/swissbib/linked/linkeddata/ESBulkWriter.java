package org.swissbib.linked.linkeddata;

import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.swissbib.linked.commons.CustomWriter;

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
            this.openOutFile();
            firstObject = false;
        }
        this.writeText((String) obj);

    }

    @Override
    public void writeText(String text) {

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
    public void openOutFile() {
        super.openOutFile();
        try {
            if (jsonCompliant) this.fout.write("[\n");
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }

    @Override
    public void closeOutFile() {
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
