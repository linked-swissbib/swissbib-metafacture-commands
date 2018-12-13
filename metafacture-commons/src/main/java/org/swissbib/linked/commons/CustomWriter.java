package org.swissbib.linked.commons;

import org.metafacture.io.ConfigurableObjectWriter;
import org.metafacture.io.FileCompression;
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

    protected final static Logger LOG = LoggerFactory.getLogger(CustomWriter.class);
    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";
    private String type = null;
    private String header = DEFAULT_HEADER;
    private String footer = DEFAULT_FOOTER;
    private String separator = DEFAULT_SEPARATOR;
    protected boolean firstObject = true;
    private boolean closed;
    //is set by the previous module
    protected String documentHeader = null;
    //private String documentFooter = "</collection>\n</rdf:RDF>";
    protected Pattern bibliographicResource;
    protected Pattern biboDoc;
    protected Pattern item;
    protected Pattern work;
    protected Pattern person;
    protected Pattern organization;
    protected Pattern containsContributor;
    private String baseOutDir = "/tmp";
    private String outFilePrefix = "rdfdump";
    protected int fileSize = 2000;
    private Boolean compress = true;
    private String extension = "";
    protected int numberLinesWritten = 0;
    private int subdirSize = 0;
    private int currentSubDir = 1;
    private int numberOpenedFiles = 0;
    private final String encoding = "UTF-8";
    protected BufferedWriter fout = null;
    private final FileCompression compression = FileCompression.AUTO;

    private final Random rng = new Random();

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

    public void openOutFile() {

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

    public abstract void writeText(String text);

    public abstract void closeOutFile();

    private String generateRandomString() {
        char[] text = new char[2];
        String characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        for (int i = 0; i < 2; i++) text[i] = characters.charAt(rng.nextInt(characters.length()));
        return new String(text);
    }


}
