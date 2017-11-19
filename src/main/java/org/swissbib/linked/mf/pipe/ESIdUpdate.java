package org.swissbib.linked.mf.pipe;

import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;
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
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swissbib.linked.mf.utils.TransportClientSingleton;

import java.io.File;
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

    private String[] esNodes = {"localhost:9300"};
    private String esClustername = "linked-swissbib";
    private List<String> mFields = new ArrayList<>();
    private Float sThreshold;
    private String uriPrefix;
    private String refPath;
    private File graphDbDir;

    private String index;
    private String type;

    private Boolean skipAll = false;
    private String identifier;
    private Boolean noId = false;
    private BoolQueryBuilder matchQuery;
    private String resId;

    private TransportClient esClient;
    private GraphDatabaseService graphDb;

    /**
     * Traverses nested maps searching and replacing a specified string
     *
     * @param map  Map to traverse
     * @param oVal Value to be replaced
     * @param rVal Replacing value
     */
    private static void traverseMap(Map<String, Object> map, String oVal, String rVal) {
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

    public void setGraphDbDir(String graphDbDir) {
        this.graphDbDir = new File(graphDbDir);
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
        /**
         * There are three possibilities here:
         * 1) The contributor already exists (noId == false): Just link the contributor to the resource in Neo4j
         * 2) The contributor doesn't already exist, but there is a match in elasticsearch: Link the identified contributor-node
         * to the resource
         * 3) The contributor doesn't already exist, and no match has been found: Create a new contributor-node and
         * link it to the resource
         */
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
                try (Transaction tx = graphDb.beginTx()) {
                    Node resNode = graphDb.findNode(lsbLabels.BIBLIOGRAPHICRESOURCE, "name", resId);
                    Node contribNode = graphDb.createNode(lsbLabels.ORGANISATION);
                    contribNode.setProperty("name", identifier);
                    resNode.createRelationshipTo(contribNode, lsbRelations.CONTRIBUTOR);
                    tx.success();
                } catch (ConstraintViolationException e) {
                    // FIXME: Write a better / more adequate log message...
                    LOG.debug("Relationship with label CONTRIBUTOR between node {} and node {} already exists. So don't create a new one.", resId, identifier);
                }
            }


            this.noId = false;
        } else {
            resourceContribEdge(identifier);
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
        // TODO: Which and how many elements must match in order to be identified as same entity
        if (noId && mFields.contains(name) && !skipAll) {
            matchQuery.must(QueryBuilders.matchQuery(name, value).fuzziness("AUTO"));
        }
        if (name.equals("resid")) {
            resId = value;
        }
        this.getReceiver().literal(name, value);
    }

    /**
     * "Moves" a document by indexing a new document with the same body and deleting the old document
     *
     * @param oId ID of the old document
     * @param tId ID of the new document
     */
    private void moveDocument(String oId, String tId) {
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
    private void updateReferences(String oId, String tId) {
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
     *
     */
    private void resourceContribEdge(String contribId) {
        try (Transaction tx = graphDb.beginTx()) {
            Node resNode = graphDb.findNode(lsbLabels.BIBLIOGRAPHICRESOURCE, "name", resId);
            Node contribNode = graphDb.findNode(lsbLabels.ORGANISATION, "name", contribId);
            resNode.createRelationshipTo(contribNode, lsbRelations.CONTRIBUTOR);
            tx.success();
        } catch (ConstraintViolationException e) {
            LOG.debug("Relationship with label CONTRIBUTOR between node {} and node {} already exists. So don't create a new one.", resId, identifier);
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
        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(graphDbDir)
                // TODO: Check possible further tweakings
                .setConfig(GraphDatabaseSettings.pagecache_memory, "24g")
                .newGraphDatabase();

        super.onSetReceiver();
    }

    private enum lsbLabels implements Label {
        BIBLIOGRAPHICRESOURCE, PERSON, ORGANISATION
    }


    private enum lsbRelations implements RelationshipType {
        CONTRIBUTOR
    }
}
