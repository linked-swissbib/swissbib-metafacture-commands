package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * An Elasticsearch Bulk API compliant output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author Sebastian Schüpbach, project swissbib, Basel
 *
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class ESBulkUpload<T> implements ConfigurableObjectWriter<T> {

    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;

    boolean firstObject = true;
    boolean closed;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    String tmpDir = "/tmp";
    String outputFormat;
    int recordsPerUpload = 2000;
    int numberFilesPerDirectory = 300;
    int numberRecordsWritten = 0;

    URLConnection conn;

    String encoding = "UTF-8";

    BufferedWriter fout = null;

    public ESBulkUpload() {

        try {
            conn = new URL("http://sb-s2.swissbib.unibas.ch:8080/_bulk").openConnection();
            conn.setDoOutput(true);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void setTmpDir(final String outDir) {
        this.tmpDir = outDir;
    }


    public void setRecordsPerUpload(final int recordsPerUpload) {
        this.recordsPerUpload = recordsPerUpload;
    }


/*    public void setContextFile (final String contextFile) throws IOException {
        String line = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(contextFile))){
            while ((line=reader.readLine()) != null) this.contextFile += line;
        }
    }*/


    public void setFilesPerDir(int numberFilesPerDirectory) {
        this.numberFilesPerDirectory = numberFilesPerDirectory;
    }

    public void setOutputFormat (final String outputFormat) {
        this.outputFormat = outputFormat;
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

        if (firstObject) {
            this.openOutFile();
            firstObject = false;
        } else {
            this.writeText((String) obj);

        }
    }


    void writeText (String text) {

        try {
            if (this.fout != null) {
                this.fout.write(text);
                this.numberRecordsWritten++;
                if (this.numberRecordsWritten >= this.recordsPerUpload) {
                    this.numberRecordsWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                }
            }
        } catch (IOException ioExc) {
            System.out.println(ioExc.getMessage());
        }
    }


    @Override
    public void resetStream() {
        firstObject = true;
    }


    @Override
    public void closeStream() {

        this.closeOutFile();
        closed = true;

    }


    void closeOutFile () {

        if (this.fout != null) {
            try {
                this.fout.flush();
                this.fout.close();
                // Todo: Implement Bulk API upload command here

            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
    }


    void openOutFile () {

        try {

            boolean subDirexists = true;
            File subDir = new File(this.tmpDir);
            if (!subDir.exists()) {
                subDirexists = subDir.mkdir();
            }

            Date dNow = new Date( );
            SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String path = this.tmpDir + File.separator + ft.format(dNow) + ".jsonld";
                final OutputStream file = new FileOutputStream(path);
                this.fout = new BufferedWriter(new OutputStreamWriter(file,this.encoding));
            } else {
                this.fout = null;
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
