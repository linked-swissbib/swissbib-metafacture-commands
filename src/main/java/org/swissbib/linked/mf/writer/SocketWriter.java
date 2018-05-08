package org.swissbib.linked.mf.writer;

import org.metafacture.io.ConfigurableObjectWriter;
import org.metafacture.io.FileCompression;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 03.10.16
 */
public class SocketWriter<T> implements ConfigurableObjectWriter<T> {

    private String enconding;
    private FileCompression compression;
    private String header;
    private String separator;
    private int port;
    private ServerSocket serverSocket;
    private PrintWriter out;


    /**
     * Returns the encoding used by the underlying writer.
     *
     * @return current encoding
     */
    @Override
    public String getEncoding() {
        return enconding;
    }

    /**
     * Sets the encoding used by the underlying writer.
     *
     * @param encoding name of the encoding
     */
    @Override
    public void setEncoding(String encoding) {
        this.enconding = encoding;
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
    public void setCompression(FileCompression compression) {
        this.compression = compression;
    }

    /**
     * Sets the compression mode.
     *
     * @param compression Compression mode as String
     */
    @Override
    public void setCompression(String compression) {
        this.compression = FileCompression.valueOf(compression);
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
        return null;
    }

    /**
     * Sets the footer which is output after the last object.
     *
     * @param footer new footer string
     */
    @Override
    public void setFooter(String footer) {
        String footer1 = footer;
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

    public void setPort(String port) {
        this.port = Integer.parseInt(port);
    }

    /**
     * This method is called by upstream modules to trigger the
     * processing of {@code obj}.
     *
     * @param obj the object to be processed
     */
    @Override
    public void process(T obj) {
        if (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(port);
                serverSocket.setSoTimeout(60000);
                Socket client = serverSocket.accept();
                out = new PrintWriter(
                        new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8.name()), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        out.println(obj.toString());
    }

    /**
     * Resets the module to its initial state. All unsaved data is discarded. This
     * method may throw {@link UnsupportedOperationException} if the model cannot
     * be reset. This method may be called any time during processing.
     */
    @Override
    public void resetStream() {

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
            serverSocket.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
