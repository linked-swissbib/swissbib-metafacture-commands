package org.swissbib.linked.mf.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Creates a transport client for Elasticsearch. Because we don't need several transport clients connecting to the
 * cluster, we have to make sure that only one instance will be created in the workflow.
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 13.01.16
 */
public class TransportClientSingleton {

    private final static Logger LOG = LoggerFactory.getLogger(TransportClientSingleton.class);
    private static TransportClient esClient;

    // Prevents directly instantiating class.
    private TransportClientSingleton() {}

    /**
     * Checks if an Elasticsearch transport client is already instantiated, and returns either the already or the newly
     * instantiated client.
     * @param nodes Nodes of the Elasticsearch cluster
     * @param clustername Name of the Elasticsearch cluster
     * @return The instantiated Elasticsearch transport client
     */
    public static synchronized TransportClient getEsClient(String[] nodes, String clustername) {
        if (esClient == null) {
            LOG.info("Connecting to Elasticsearch cluster {}", clustername);
            Settings settings = Settings.builder()
                    .put("cluster.name", clustername)
                    .build();
            esClient = new PreBuiltTransportClient(settings);
            for (String elem: nodes) {
                String[] node = elem.split(":");
                try {
                    esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node[0]), Integer.parseInt(node[1])));
                } catch (UnknownHostException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
        return esClient;
    }

    /**
     * Calling of inherited clone() method throws an error
     * @return An error
     * @throws CloneNotSupportedException
     */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /**
     * Shuts down Elasticsearch transport client
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        LOG.info("Shutting down Elasticsearch transport client");
        esClient.close();
    }
}
