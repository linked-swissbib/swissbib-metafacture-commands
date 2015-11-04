package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


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
public class ESBulkWriter<T> implements ConfigurableObjectWriter<T> {

    private final static Logger LOG = LoggerFactory.getLogger(ESBulkWriter.class);

    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;

    boolean firstObject = true;
    boolean closed;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    String baseOutDir = "/tmp";
    String outFilePrefix = "esbulk";
    Boolean jsonCompliant = false;
    int fileSize = 2000;
    int numberFilesPerDirectory = 300;
    int currentSubDir = 1;
    int numberOpenedFiles = 0;
    int numberRecordsWritten = 0;

    FileCompression compression = FileCompression.AUTO;
    String encoding = "UTF-8";

    BufferedWriter fout = null;

    Random rng = new Random();


    public void setBaseOutDir (final String outDir) {
        this.baseOutDir = outDir;
        LOG.debug("Settings - Set output directory: {}", outDir);
    }


    public void setOutFilePrefix (final String filePrefix) {
        this.outFilePrefix = filePrefix;
        LOG.debug("Settings - Set output file prefix: {}", filePrefix);
    }


    public void setFileSize (final int fileSize) {
        this.fileSize = fileSize;
        LOG.debug("Settings - Set number of records in one file: {}", fileSize);
    }


    public void setFilesPerDir(final int numberFilesPerDirectory) {
        this.numberFilesPerDirectory = numberFilesPerDirectory;
        LOG.debug("Settings - Set number of files in one subdirectory: {}", numberFilesPerDirectory);
    }

    public void setJsonCompliant(final String jsonCompliant) {
        this.jsonCompliant = Boolean.parseBoolean(jsonCompliant);
        LOG.debug("Settings - Is output valid JSON? {}", this.jsonCompliant);
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
        }
        this.writeText((String) obj);

    }


    void writeText (String text) {

        try {
            if (this.fout != null) {
                LOG.debug("Writing record {} in file", numberRecordsWritten);
                if (jsonCompliant) text = text.substring(0, text.length() - 1);
                if (!text.equals("{}\n")) this.fout.write(text);
                this.numberRecordsWritten++;
                if (this.numberRecordsWritten >= this.fileSize) {
                    this.numberRecordsWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                } else {
                    if (jsonCompliant) this.fout.write(",\n");
                }
            }
        } catch (IOException ioExc) {
            LOG.error(ioExc.getMessage());
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
                if (jsonCompliant) this.fout.write("\n]");
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                LOG.error("IO exception while output file should be closed: {}", ioEx);
            }
        }
    }


    void openOutFile () {

        try {

            boolean subDirexists = true;
            File subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
            if (!subDir.exists()) {
                LOG.debug("Creating new subdirectory {}", subDir);
                subDirexists = subDir.mkdir();
            } else if (this.numberFilesPerDirectory <= this.numberOpenedFiles) {
                this.currentSubDir++;
                subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
                if (!subDir.exists()) {
                    subDirexists = subDir.mkdir();
                }
                this.numberOpenedFiles = 0;
            }

            Date dNow = new Date( );
            SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String path = this.baseOutDir + File.separator + this.currentSubDir + File.separator +
                        this.outFilePrefix + "_" + ft.format(dNow) + "_" + generateRandomString() + ".jsonld.gz";
                LOG.debug("Creating new file {}", path);
                final OutputStream file = new FileOutputStream(path);
                OutputStream compressor = compression.createCompressor(file, path);

                this.fout = new BufferedWriter(new OutputStreamWriter(compressor,this.encoding));
                if (jsonCompliant) this.fout.write("[\n");

            } else {
                this.fout = null;
            }


            if (this.fout != null) {
                this.numberOpenedFiles++;
            }

            //Todo: GH: Look up Exception Handlng in Metafacture Framework
            //hint: implementation of File opener in MF
        } catch (FileNotFoundException fnfEx) {
            LOG.error("File not found: {}", fnfEx.getMessage());

        } catch (UnsupportedEncodingException usEnc) {
            LOG.error("Unsupported encoding: {}", usEnc.getMessage());
        } catch (IOException e) {
            LOG.error("IO exception: {}", e.getMessage());
        }

    }

    String generateRandomString() {
        char[] text = new char[2];
        String characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        for (int i = 0; i < 2; i++) text[i] = characters.charAt(rng.nextInt(characters.length()));
        return new String(text);
    }

}
