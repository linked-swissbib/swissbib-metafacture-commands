package org.swissbib.linked.mf.pipe;

import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
public class ESItemEraser extends DefaultStreamPipe<StreamReceiver> {

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
        eraseItems("http://data.swissbib.ch/resource/" + identifier);
        getReceiver().startRecord(identifier);
    }


    @Override
    public void endRecord() {
        getReceiver().endRecord();
    }


    @Override
    public void startEntity(String name) {
        getReceiver().startEntity(name);
    }


    @Override
    public void endEntity() {
        getReceiver().endEntity();
    }


    @Override
    public void literal(String name, String value) {
        getReceiver().literal(name, value);
    }


    @Override
    protected void onCloseStream() {
        esClient.close();
    }


    private void eraseItems(String resId) {
        if (esClient == null)
            esClient = TransportClientSingleton.getEsClient(esNodes, esClustername);
        SearchHits sh = esClient.prepareSearch(esIndex)
                .setTypes(esType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("bf:holdingFor", resId))
                .execute()
                .actionGet()
                .getHits();

        for (SearchHit h : sh) {
            esClient.prepareDelete(esIndex, esType, h.getId());
            LOG.debug("Document {}/{} deleted.", esType, h.getId());
        }
    }
}
