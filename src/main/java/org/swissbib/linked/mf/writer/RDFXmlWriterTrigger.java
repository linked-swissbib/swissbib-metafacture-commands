package org.swissbib.linked.mf.writer;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 10.09.15
 */
public class RDFXmlWriterTrigger<T> extends RdfXmlWriter<T> {

    String commandPath;


    public RDFXmlWriterTrigger() {
        super();
    }

    public void setCommandPath(String commandPath) {
        this.commandPath = commandPath;
    }

    @Override
    void openOutFile() {
        Date dNow = new Date( );
        SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");
        String path = this.baseOutDir + File.separator + this.outFilePrefix + "_" + ft.format(dNow) + ".xml";
        try {
            final OutputStream file = new FileOutputStream(path);
            this.fout = new BufferedWriter(new OutputStreamWriter(file, this.encoding));
            this.writeText(this.documentHeader);
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
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
                    this.triggerCommand();
                    this.openOutFile();
                }
            }
        } catch (IOException ioExc) {
            System.out.println(ioExc.getMessage());
        }
    }

    public void triggerCommand() {
        try {
            Runtime.getRuntime().exec(this.commandPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
