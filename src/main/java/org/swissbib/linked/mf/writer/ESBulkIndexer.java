package org.swissbib.linked.mf.writer;

import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.io.ConfigurableObjectWriter;
import org.metafacture.io.FileCompression;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swissbib.linked.mf.utils.TransportClientSingleton;

import java.nio.charset.Charset;


/**
 * Writes Elasticsearch Bulk API compliant strings to Elasticsearch Index
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class ESBulkIndexer<T> implements ConfigurableObjectWriter<T> {

    private static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";
    private final static Logger LOG = LoggerFactory.getLogger(ESBulkIndexer.class);
    private String[] esNodes = {"localhost:9300"};
    private String esClustername = "linked-swissbib";
    private int recordsPerUpload = 2000;

    private TransportClient esClient;
    private BulkProcessor bulkProcessor;


    public void setEsClustername(final String esClustername) {
        this.esClustername = esClustername;
        LOG.debug("Settings - Set cluster name for Elasticsearch: {}", esClustername);
    }


    public void setRecordsPerUpload(final int recordsPerUpload) {
        this.recordsPerUpload = recordsPerUpload;
        LOG.debug("Settings - Set number of records per bulk upload: {}", recordsPerUpload);
    }


    public void setEsNodes(final String esNode) {
        this.esNodes = esNode.split("#");
        LOG.debug("Settings - Set addresses of Elasticsearch nodes: {} (# is a delimiter)", esNode);
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

    /**
     * Sets the header which is output before the first object.
     *
     * @param header new header string
     */
    @Override
    public void setHeader(String header) {

    }


    @Override
    public String getFooter() {
        return DEFAULT_FOOTER;
    }

    /**
     * Sets the footer which is output after the last object.
     *
     * @param footer new footer string
     */
    @Override
    public void setFooter(String footer) {

    }


    @Override
    public String getSeparator() {
        return DEFAULT_SEPARATOR;
    }

    /**
     * Sets the separator which is output between objects.
     *
     * @param separator new separator string
     */
    @Override
    public void setSeparator(String separator) {

    }


    public void process(T obj) {
        LOG.trace("Adding record to bulk processor");
        esClient = TransportClientSingleton.getEsClient(esNodes, this.esClustername);
        if (bulkProcessor == null) createTransportClient();

        if (!obj.equals("{}\n")) {
            BytesArray ba = new BytesArray((String) obj);
            try {
                this.bulkProcessor.add(ba, null, null);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private void createTransportClient() {

        this.bulkProcessor = BulkProcessor.builder(this.esClient, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                LOG.debug("Bulk requests to be processed: {}", bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                LOG.debug("Indexing took {} ms", bulkResponse.getTookInMillis());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                LOG.error("Some errors were reported: {}", throwable.getMessage());
            }
        })
                // Header and body line
                .setBulkActions(this.recordsPerUpload)
                .setConcurrentRequests(1)
                .build();
    }

    @Override
    public void resetStream() {
        this.bulkProcessor.flush();
    }


    @Override
    public void closeStream() {
        LOG.info("Shutting down Elasticsearch bulk processor.");
        this.bulkProcessor.flush();
    }

}
