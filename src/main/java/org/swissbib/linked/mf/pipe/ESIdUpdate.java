package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swissbib.linked.mf.utils.TransportClientSingleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 06.01.16
 */
@Description("Checks if a identical identifier exists and, if not, performs a match on existing documents.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public class ESIdUpdate extends DefaultStreamPipe<StreamReceiver> {

    private final static Logger LOG = LoggerFactory.getLogger(ESIdUpdate.class);

    String[] esNodes = {"localhost:9300"};
    String esClustername = "linked-swissbib";
    List<String> mFields = new ArrayList<>();
    Float sThreshold;
    String uriPrefix;
    String refPath;

    String index;
    String type;

    Boolean skipAll = false;
    String identifier;
    Boolean noId = false;
    BoolQueryBuilder matchQuery;

    TransportClient esClient;


    public void setEsClustername(final String esClustername) {
        this.esClustername = esClustername;
        LOG.debug("Settings - Set cluster name for Elasticsearch: {}", esClustername);
    }

    public void setEsNodes(final String esNode) {
        this.esNodes = esNode.split("#");
        LOG.debug("Settings - Set addresses of Elasticsearch nodes: {} (# is a delimiter)", esNode);
    }

    public void setMatchingFields(final String matchingFields) {
        mFields = Arrays.asList(matchingFields.split("#"));
        LOG.debug("Settings - Set matching fields: {} (# is a delimiter)", mFields);
    }

    public void setsThreshold(String scoreThreshold) {
        this.sThreshold = Float.valueOf(scoreThreshold);
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setRefPath(String refPath) {
        this.refPath = refPath;
    }

    public void setUriPrefix(String uriPrefix) {
        this.uriPrefix = uriPrefix;
    }


    @Override
    public void startRecord(String identifier) {
        this.identifier = identifier;
        if (!skipAll) {
            GetResponse response = esClient
                    .prepareGet(index, type, identifier)
                    .get();

            if (!response.isExists()) {
                noId = true;
                matchQuery = new BoolQueryBuilder();
                LOG.debug("No document with same id {} could be found in index.", identifier);
            } else {
                LOG.debug("Document with same id {} could be found in index. Skipping further matching procedures.",
                        identifier);
            }
        }
        this.getReceiver().startRecord(identifier);
    }

    @Override
    public void endRecord() {
        if (noId && !skipAll) {
            SearchResponse matchResponse = esClient
                    .prepareSearch(index)
                    .setTypes(type)
                    .setQuery(matchQuery)
                    .execute()
                    .actionGet();
            LOG.trace("{} matching documents found:", matchResponse.getHits().getTotalHits());
            if (matchResponse.getHits().getMaxScore() >= sThreshold) {

                String oId = matchResponse.getHits().getAt(0).getId();
                LOG.debug("Replacing document {} (Score: {}).", oId, matchResponse.getHits().getAt(0).getScore());
                this.moveDocument(oId, identifier);
                this.updateReferences(oId, identifier);
            } else {
                LOG.debug("No matching document above score threshold ({}) could be found.", sThreshold);
            }
            this.noId = false;
        }
        this.getReceiver().endRecord();
    }

    @Override
    public void startEntity(String name) {
        this.getReceiver().startEntity(name);
    }

    @Override
    public void endEntity() {
        this.getReceiver().endEntity();
    }

    @Override
    public void literal(String name, String value) {
        if (noId && mFields.contains(name) && !skipAll) {
            matchQuery.must(QueryBuilders.matchQuery(name, value).fuzziness("AUTO"));
        }
        this.getReceiver().literal(name, value);
    }

    /**
     * "Moves" a document by indexing a new document with the same body and deleting the old document
     *
     * @param oId ID of the old document
     * @param tId ID of the new document
     */
    void moveDocument(String oId, String tId) {
        LOG.trace("Getting document with id {}", oId);
        GetResponse gr = esClient
                .prepareGet(index, type, oId)
                .get();
        LOG.trace("Indexing document with id {}", tId);
        IndexResponse ir = esClient
                .prepareIndex(index, type, tId)
                .setSource(gr.getSource())
                .get();
        LOG.trace("Copying document {} -> {}", gr.getId(), ir.getId());
        DeleteResponse dr = esClient
                .prepareDelete(index, type, oId)
                .get();
        LOG.trace("Deleting document {}", dr.getId());
    }

    /**
     * Looks for a specified person URI in documents in bibliographicResource and replaces it with a new one
     *
     * @param oId URI to be replaced
     * @param tId URI reference
     */
    void updateReferences(String oId, String tId) {
        SearchResponse sr = esClient
                .prepareSearch(index)
                .setTypes("bibliographicResource")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery(refPath, oId))
                .execute()
                .actionGet();
        for (SearchHit hit : sr.getHits()) {
            Map<String, Object> docSource = hit.getSource();
            traverseMap(docSource, uriPrefix + oId, uriPrefix + tId);
            IndexResponse ir = esClient
                    .prepareIndex(index, type, tId)
                    .setSource(docSource)
                    .get();
            LOG.trace("Update dc:contributor reference {} to {} in document {}", oId, tId, ir.getId());
        }
    }

    /**
     * Traverses nested maps searching and replacing a specified string
     *
     * @param map  Map to traverse
     * @param oVal Value to be replaced
     * @param rVal Replacing value
     */
    static public void traverseMap(Map<String, Object> map, String oVal, String rVal) {
        for (String k : map.keySet()) {
            Object value = map.get(k);
            if (Map.class.isAssignableFrom(value.getClass())) {
                traverseMap((Map<String, Object>) value, oVal, rVal);
            } else {
                if (value.getClass().getName().equals("java.lang.String") && value.equals(oVal)) {
                    map.put(k, rVal);
                }
            }
        }
    }

    @Override
    protected void onSetReceiver() {
        LOG.debug("Setting receiver");
        esClient = TransportClientSingleton.getEsClient(esNodes, this.esClustername);
        String[] indices = esClient
                .admin()
                .indices()
                .getIndex(new GetIndexRequest())
                .actionGet()
                .indices();
        if (!Arrays.asList(indices).contains(index)) {
            LOG.info("No index {} exists in cluster. Skipping further queries.", index);
            skipAll = true;
        }
        super.onSetReceiver();
    }
}
