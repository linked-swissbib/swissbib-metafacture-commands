package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.MetafactureException;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.io.ConfigurableObjectWriter;
import org.culturegraph.mf.io.FileCompression;

import java.io.*;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 01.11.16
 */
@Description("Serialises data as CSV file with optional header")
@In(String.class)
@Out(Void.class)
public class ContinuousCsvWriter<T> implements ConfigurableObjectWriter<T> {

    private String footer = "";
    private String header;
    private String path = System.getProperty("user.dir");
    private String filenamePrefix = "";
    private String filenamePostfix = "";
    private String filetype = "csv";
    private Boolean continuousFile = true;
    private int linesPerFile = 100000;
    private FileCompression compression = FileCompression.AUTO;
    private String encoding = "UTF-8";
    private String separator = System.lineSeparator();

    private int lineCounter = 0;
    private OutputStream file;
    private OutputStream compressor;
    private OutputStreamWriter out;
    private int fileNumber = 0;
    private String fileName;

    /**
     * Returns the encoding used by the underlying writer.
     *
     * @return current encoding
     */
    @Override
    public String getEncoding() {
        return encoding;
    }

    /**
     * Sets the encoding used by the underlying writer.
     *
     * @param encoding name of the encoding
     */
    @Override
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Returns the compression mode.
     *
     * @return current compression mode
     */
    @Override
    public FileCompression getCompression() {
        return compression;
    }

    /**
     * Sets the compression mode.
     *
     * @param compression Compression mode as FileCompression instance
     */
    @Override
    public void setCompression(String compression) {
        setCompression(FileCompression.valueOf(compression.toUpperCase()));
    }

    /**
     * Sets the compression mode.
     *
     * @param compression Compression mode as String
     */
    @Override
    public void setCompression(FileCompression compression) {
        this.compression = compression;
    }

    /**
     * Returns the header which is output before the first object.
     *
     * @return header string
     */
    @Override
    public String getHeader() {
        return header;
    }

    /**
     * Sets the header which is output before the first object.
     *
     * @param header new header string
     */
    @Override
    public void setHeader(String header) {
        this.header = header;
    }

    /**
     * Returns the footer which is output after the last object.
     *
     * @return footer string
     */
    @Override
    public String getFooter() {
        return footer;
    }

    /**
     * Sets the footer which is output after the last object.
     *
     * @param footer new footer string
     */
    @Override
    public void setFooter(String footer) {
        this.footer = footer;
    }

    /**
     * Returns the separator which is output between objects.
     *
     * @return separator string
     */
    @Override
    public String getSeparator() {
        return separator;
    }

    /**
     * Sets the separator which is output between objects.
     *
     * @param separator new separator string
     */
    @Override
    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getFilenamePrefix() {
        return filenamePrefix;
    }

    public void setFilenamePrefix(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    public String getFilenamePostfix() {
        return filenamePostfix;
    }

    public void setFilenamePostfix(String filenamePostfix) {
        this.filenamePostfix = filenamePostfix;
    }

    public String getFiletype() {
        return filetype;
    }

    public void setFiletype(String filetype) {
        this.filetype = filetype;
    }

    public Boolean getContinuousFile() {
        return continuousFile;
    }

    public void setContinuousFile(String continuousFile) {
        this.continuousFile = Boolean.parseBoolean(continuousFile);
    }

    public int getLinesPerFile() {
        return linesPerFile;
    }

    public void setLinesPerFile(String linesPerFile) {
        this.linesPerFile = Integer.parseInt(linesPerFile);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    /**
     * This method is called by upstream modules to trigger the
     * processing of {@code obj}.
     *
     * @param obj the object to be processed
     */
    @Override
    public void process(Object obj) {
        writeRecord(obj);
    }

    /**
     * Resets the module to its initial state. All unsaved data is discarded. This
     * method may throw {@link UnsupportedOperationException} if the model cannot
     * be reset. This method may be called any time during processing.
     */
    @Override
    public void resetStream() {
        resetWriter();
    }

    /**
     * Notifies the module that processing is completed. Resources such as files or
     * search indexes should be closed. The module cannot be used anymore after
     * closeStream() has been called. The module may be reset, however, so
     * it can be used again. This is not guaranteed to work though.
     * This method may be called any time during processing.
     */
    @Override
    public void closeStream() {
        try {
            closeFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void resetWriter() {
        try {
            closeFile();
            openNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openNewFile() throws IOException {
        String filenumberAsString = ("00000" + fileNumber).substring(String.valueOf(fileNumber).length());
        fileName = path + File.separator + filenamePrefix + filenumberAsString + filenamePostfix + "." + filetype;
        try {
            file = new FileOutputStream(fileName);
            try {
                compressor = compression.createCompressor(file, encoding);
                try {
                    out = new OutputStreamWriter(compressor, encoding);
                    lineCounter = 0;
                    if ((!continuousFile || fileNumber == 0) && header != null) {
                        writeRecord(header);
                    }
                    fileNumber++;
                } catch (IOException e) {
                    compressor.close();
                    throw e;
                }
            } catch (IOException e) {
                file.close();
                throw e;
            }
        } catch (IOException e) {
            throw new MetafactureException("Error creating file '" + fileName + "'.", e);
        }
    }

    private void closeFile() throws IOException {
        out.close();
        compressor.close();
        file.close();
    }

    private void writeRecord(Object obj) {
        try {
            if (out == null) {
                openNewFile();
            }
            if (lineCounter >= linesPerFile) {
                resetWriter();
            }
            out.write(obj.toString());
            out.write(separator);
            lineCounter++;
        } catch (IOException e) {
            throw new MetafactureException("Error writing to file '" + fileName + "'.", e);
        }
    }
}
