package org.swissbib.linked.mf.writer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.PrintWriter;
import java.util.Properties;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 03.10.16
 */
public class KafkaWriter<T> implements ConfigurableObjectWriter<T> {

    private String enconding;
    private FileCompression compression;
    private String header;
    private String footer;
    private String separator;
    private String host;
    private Integer port;
    private String kafkaTopic;
    private PrintWriter out;
    private Boolean firstRecord = true;
    private Properties props = new Properties();
    private Producer<String, String> producer;


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
     * @param compression Compression mode as String compression
     */
    @Override
    public void setCompression(String compression) {
        this.compression = FileCompression.valueOf(compression);
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

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = Integer.parseInt(port);
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * This method is called by upstream modules to trigger the
     * processing of {@code obj}.
     *
     * @param obj the object to be processed
     */
    @Override
    public void process(T obj) {
        if (firstRecord) {
            startProducer();
            firstRecord = false;
        }
        producer.send(new ProducerRecord<String, String>(kafkaTopic, obj.toString()));
    }

    /**
     * Resets the module to its initial state. All unsaved data is discarded. This
     * method may throw {@link UnsupportedOperationException} if the model cannot
     * be reset. This method may be called any time during processing.
     */
    @Override
    public void resetStream() {
        producer.close();
        startProducer();
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
        producer.close();
    }

    private void startProducer() {
        props.put("bootstrap.servers", host + ":" + port.toString());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }
}
