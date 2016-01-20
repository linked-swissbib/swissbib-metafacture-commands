package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A formatter for multiline output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 *
 */
@Description("Writes RDF/XML documents in outpufiles. Each document is written down in one line which makes it easer for further processing")
@In(Object.class)
@Out(Void.class)
public class SingleLineWriterRDFXml<T> extends RdfXmlWriter<T> {

    int numberFilesPerDirectory = 300;
    int currentSubDir = 1;
    int numberOpenedFiles = 0;

    FileCompression compression = FileCompression.AUTO;


    public SingleLineWriterRDFXml() {
        super();
    }

    public void setFilesPerDir(int numberFilesPerDirectory) {
        this.numberFilesPerDirectory = numberFilesPerDirectory;
    }

    @Override
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
    void openOutFile () {

        try {

            boolean subDirexists = true;
            File subDir = this.useSubdir ? new File(this.baseOutDir + File.separator + this.currentSubDir) :
                    new File(this.baseOutDir);
            if (!subDir.exists()) {

                 subDirexists = subDir.mkdir();
            } else if (this.numberFilesPerDirectory <= this.numberOpenedFiles) {
                this.currentSubDir++;
                subDir = this.useSubdir ? new File(this.baseOutDir + File.separator + this.currentSubDir) :
                        new File(this.baseOutDir);
                if (!subDir.exists()) {
                    subDirexists = subDir.mkdir();
                }
                this.numberOpenedFiles = 0;
            }

            Date dNow = new Date( );
            SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String typeName = this.type != null ? this.type : "noType";
                String filename = this.outFilePrefix + "_" +  ft.format(dNow) +  "_" + typeName + ".xml.gz";
                String path = this.useSubdir ? this.baseOutDir + File.separator + this.currentSubDir + File.separator +
                        filename : this.baseOutDir + File.separator + filename;
                final OutputStream file = new FileOutputStream(path);
                OutputStream compressor = compression.createCompressor(file, path);

                this.fout = new BufferedWriter(new OutputStreamWriter(compressor,this.encoding));
                this.writeText(this.documentHeader);

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
