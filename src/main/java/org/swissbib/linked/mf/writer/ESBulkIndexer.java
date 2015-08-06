package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.nio.charset.Charset;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


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

    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    // Todo: FLUX parameters are not recognised. If we do not define default values for these arguments, the instantiation of the class fails. Why?
    String esNode = "localhost";
    int esPort = 9300;
    String esClustername = "linked-swissbib";
    int recordsPerUpload = 2000;

    Client esClient;
    BulkProcessor bulkProcessor;


    public ESBulkIndexer() {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", this.esClustername)
                .build();

        // Todo: We should find a way to add more nodes by means of FLUX parameters
        this.esClient = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(this.esNode, this.esPort));

        this.bulkProcessor = BulkProcessor.builder(this.esClient, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                System.out.println("Bulk requests to be processed: " + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("Indexing took " + bulkResponse.getTookInMillis() + " ms");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                System.out.println("Some errors were reported: " + throwable.getMessage());
            }
        })
                .setBulkActions(this.recordsPerUpload)
                .setConcurrentRequests(1)
                .build();
    }


    public void setEsClustername(final String esClustername) {
        this.esClustername = esClustername;
    }


    public void setRecordsPerUpload(final int recordsPerUpload) {
        this.recordsPerUpload = recordsPerUpload;
    }


    public void setEsNode(final String esNode) {
        this.esNode = esNode;
    }


    public void setEsPort(final int esPort) {
        this.esPort = esPort;
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

        BytesArray ba = new BytesArray((String) obj);
        try {
            this.bulkProcessor.add(ba, false, "testsb", "bibliographicResource");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void resetStream() {
        this.bulkProcessor.flush();
    }


    @Override
    public void closeStream() {
        this.bulkProcessor.flush();
        this.bulkProcessor.close();
        this.esClient.close();
    }

}
