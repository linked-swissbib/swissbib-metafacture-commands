package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.helpers.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swissbib.linked.mf.utils.TransportClientSingleton;
import org.swissbib.linked.mf.writer.ESBulkIndexer;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 24.03.16
 */
@Description("Filters out records which already exists in Elasticsearch index.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public class ESLookup extends DefaultStreamPipe<StreamReceiver> {

    private final static Logger LOG = LoggerFactory.getLogger(ESBulkIndexer.class);

    private TransportClient esClient;
    private String esClustername;
    private String[] esNodes;
    private String esIndex;
    private String esType;
    private boolean nodeExists;


    public void setEsClustername(final String esClustername) {
        this.esClustername = esClustername;
        LOG.debug("Settings - Set cluster name for Elasticsearch: {}", esClustername);
    }


    public void setEsNodes(final String esNode) {
        this.esNodes = esNode.split("#");
        LOG.debug("Settings - Set addresses of Elasticsearch nodes: {} (# is a delimiter)", esNode);
    }


    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }


    public void setEsType(String esType) {
        this.esType = esType;
    }


    @Override
    public void startRecord(String identifier) {
        nodeExists = lookupId(identifier);
        LOG.debug("Document {} {} in {}/{}", identifier, (nodeExists) ? "already exists" : "doesn't exist yet", esIndex, esType);
        if (!nodeExists) getReceiver().startRecord(identifier);
    }


    @Override
    public void endRecord() {
        if (!nodeExists) getReceiver().endRecord();
    }


    @Override
    public void startEntity(String name) {
        if (!nodeExists) getReceiver().startEntity(name);
    }


    @Override
    public void endEntity() {
        if (!nodeExists) getReceiver().endEntity();
    }


    @Override
    public void literal(String name, String value) {
        if (!nodeExists) getReceiver().literal(name, value);
    }


    /**
     * Checks if a document already exists in Elasticsearch index by issuing a get request on the identifier
     *
     * @param id Identifier of the document / record
     * @return true if document exists
     */
    private boolean lookupId(String id) {
        if (esClient == null)
            esClient = TransportClientSingleton.getEsClient(esNodes, esClustername);
        return esClient.prepareGet(esIndex, esType, id).get().isExists();
    }
}
