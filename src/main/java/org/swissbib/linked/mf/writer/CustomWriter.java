package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Abstract class for custom writer classes.
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 10.09.15
 */
public abstract class CustomWriter<T> implements ConfigurableObjectWriter<T> {

    protected final static Logger LOG = LoggerFactory.getLogger(ESBulkWriter.class);
    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";
    protected String type = null;
    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;
    boolean firstObject = true;
    boolean closed;
    //is set by the previous module
    String documentHeader = null;
    //private String documentFooter = "</collection>\n</rdf:RDF>";
    Pattern bibliographicResource;
    Pattern biboDoc;
    Pattern item;
    Pattern work;
    Pattern person;
    Pattern organization;
    Pattern containsContributor;
    String baseOutDir = "/tmp";
    String outFilePrefix = "rdfdump";
    int fileSize = 2000;
    Boolean compress = true;
    String extension = "";
    int numberLinesWritten = 0;
    int subdirSize = 0;
    int currentSubDir = 1;
    int numberOpenedFiles = 0;
    String encoding = "UTF-8";
    BufferedWriter fout = null;
    FileCompression compression = FileCompression.AUTO;

    Random rng = new Random();

    private static String rmTrailingSlash(String t) {
        if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
        return t;
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
    public void setCompression(String compression) {
        throw new UnsupportedOperationException(SET_COMPRESSION_ERROR);
    }

    @Override
    public void setCompression(FileCompression compression) {
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

    public void setExtension(String extension) {
        if (extension.startsWith(".")) {
            this.extension = extension;
        } else {
            this.extension = "." + extension;
        }
    }

    public void setCompress(String compress) {
        this.compress = Boolean.parseBoolean(compress);
    }

    public void setBaseOutDir (String outDir) {
        outDir = rmTrailingSlash(outDir);
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

    public void setSubdirSize(String subdirSize) {
        this.subdirSize = Integer.parseInt(subdirSize);
        LOG.debug("Settings - Set number of files in one subdirectory: {}", subdirSize);
    }

    public void setType(String type) {
        this.type = type;
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

    void openOutFile() {

        try {

            boolean subDirexists = true;
            File subDir = this.subdirSize > 0 ? new File(this.baseOutDir + File.separator + this.currentSubDir) :
                    new File(this.baseOutDir);
            if (!subDir.exists()) {

                subDirexists = subDir.mkdir();
            } else if (this.subdirSize <= this.numberOpenedFiles) {
                this.currentSubDir++;
                subDir = this.subdirSize > 0 ? new File(this.baseOutDir + File.separator + this.currentSubDir) :
                        new File(this.baseOutDir);
                if (!subDir.exists()) {
                    subDirexists = subDir.mkdir();
                }
                this.numberOpenedFiles = 0;
            }

            Date dNow = new Date();
            SimpleDateFormat ft = new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String typeName = this.type != null ? this.type : "noType";
                String filename = this.outFilePrefix + "_" + ft.format(dNow) + "_"
                        + generateRandomString() + "_" + typeName + extension;
                if (compress) filename += ".gz";
                String path = this.subdirSize > 0 ? this.baseOutDir + File.separator + this.currentSubDir + File.separator +
                        filename : this.baseOutDir + File.separator + filename;
                final OutputStream file = new FileOutputStream(path);

                if (compress) {
                    OutputStream compressor = compression.createCompressor(file, path);
                    this.fout = new BufferedWriter(new OutputStreamWriter(compressor, this.encoding));
                } else {
                    this.fout = new BufferedWriter(new OutputStreamWriter(file, this.encoding));
                }

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

    abstract void writeText(String text);

    abstract void closeOutFile();

    String generateRandomString() {
        char[] text = new char[2];
        String characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        for (int i = 0; i < 2; i++) text[i] = characters.charAt(rng.nextInt(characters.length()));
        return new String(text);
    }


}
