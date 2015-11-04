package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Writes Elasticsearch Bulk API compliant strings to Elasticsearch Index
 *
 * @author Sebastian Schüpbach, project swissbib, Basel
 *
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class ESBulkIndexer<T> implements ConfigurableObjectWriter<T> {

    private final static Logger LOG = LoggerFactory.getLogger(ESBulkIndexer.class);

    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    String[] esNodes = {"localhost:9300"};
    String esClustername = "linked-swissbib";
    int recordsPerUpload = 2000;

    Boolean connEstablished = false;

    TransportClient esClient;
    BulkProcessor bulkProcessor;


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


    protected void establishConn() {
        LOG.info("Connecting to Elasticsearch nodes");
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", this.esClustername)
                .build();

        this.esClient = TransportClient.builder().settings(settings).build();
        for (String elem: this.esNodes) {
            String[] node = elem.split(":");
            try {
                this.esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node[0]), Integer.parseInt(node[1])));
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage());
            }
        }

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


    public void process(T obj) {

        LOG.trace("Preparing record for bulk uploading");
        if (!this.connEstablished) {
            this.establishConn();
            this.connEstablished = true;
        }

        if (!obj.equals("{}\n")) {
            BytesArray ba = new BytesArray((String) obj);
            try {
                this.bulkProcessor.add(ba, null, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void resetStream() {
        this.bulkProcessor.flush();
    }


    @Override
    public void closeStream() {
        LOG.info("Closing connection to Elasticsearch nodes");
        this.bulkProcessor.flush();
        this.bulkProcessor.close();
        this.esClient.close();
    }

}
