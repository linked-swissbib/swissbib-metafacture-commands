package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 03.03.16
 */
@Description("Transforms documents to a Neo4j graph.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public class NeoIndexer extends DefaultStreamPipe<StreamReceiver> {

    private final static Logger LOG = LoggerFactory.getLogger(NeoIndexer.class);
    private GraphDatabaseService graphDb;
    private File dbDir;
    private Node mainNode;
    private Transaction tx;
    private int batchSize;
    private int counter = 0;
    private boolean firstRecord = true;


    public void setBatchSize(String batchSize) {
        this.batchSize = Integer.parseInt(batchSize);
    }

    public void setDbDir(String dbDir) {
        this.dbDir = new File(dbDir);
    }

    @Override
    public void startRecord(String identifier) {

        if (firstRecord) {
            graphDb = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder(dbDir)
                    // TODO: Check possible further tweakings
                    .setConfig(GraphDatabaseSettings.pagecache_memory, "24g")
                    .newGraphDatabase();
            tx = graphDb.beginTx();
            if (!graphDb.schema().getIndexes(lsbLabels.PERSON).iterator().hasNext())
                graphDb.schema().constraintFor(lsbLabels.PERSON).assertPropertyIsUnique("name").create();
            if (!graphDb.schema().getIndexes(lsbLabels.ORGANISATION).iterator().hasNext())
                graphDb.schema().constraintFor(lsbLabels.ORGANISATION).assertPropertyIsUnique("name").create();
            if (!graphDb.schema().getIndexes(lsbLabels.BIBLIOGRAPHICRESOURCE).iterator().hasNext())
                graphDb.schema().constraintFor(lsbLabels.BIBLIOGRAPHICRESOURCE).assertPropertyIsUnique("name").create();
            if (!graphDb.schema().getIndexes(lsbLabels.LOCALSIGNATURE).iterator().hasNext())
                graphDb.schema().constraintFor(lsbLabels.LOCALSIGNATURE).assertPropertyIsUnique("name").create();
            if (!graphDb.schema().getIndexes(lsbLabels.WORK).iterator().hasNext())
                graphDb.schema().constraintFor(lsbLabels.WORK).assertPropertyIsUnique("name").create();
            if (!graphDb.schema().getIndexes(lsbLabels.ACTIVE).iterator().hasNext())
                graphDb.schema().indexFor(lsbLabels.ACTIVE).on("name").create();
            tx.success();
            tx.close();
            tx = graphDb.beginTx();
            firstRecord = false;
        }

        counter += 1;
        LOG.debug("Working on record {}", identifier);
        mainNode = createNode(lsbLabels.BIBLIOGRAPHICRESOURCE, identifier, true);
        getReceiver().startRecord(identifier);

    }

    @Override
    public void endRecord() {
        tx.success();
        if (counter % batchSize == 0) {
            LOG.info("Commit batch upload ({} records processed so far)", counter);
            tx.close();
            tx = graphDb.beginTx();
        }
        super.endRecord();
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
        Node node;

        switch (name) {
            case "dct:contributor":
                node = (value.contains("person")) ?
                        createNode(lsbLabels.PERSON, value, true) :
                        createNode(lsbLabels.ORGANISATION, value, true);
                mainNode.createRelationshipTo(node, lsbRelations.CONTRIBUTOR);
                getReceiver().literal(name, value);
                break;
            case "bf:local":
                node = createNode(lsbLabels.LOCALSIGNATURE, value, true);
                node.createRelationshipTo(mainNode, lsbRelations.SIGNATUREOF);
                // Do not send this field further in the process, it doesn't belong to this concept
                break;
            case "work":
                node = createNode(lsbLabels.WORK, value, true);
                node.createRelationshipTo(mainNode, lsbRelations.WORKOF);
                getReceiver().literal(name, value);
                break;
            default:
                getReceiver().literal(name, value);
                break;
        }
    }

    @Override
    protected void onCloseStream() {
        LOG.info("Cleaning up (altogether {} records processed)", counter);
        tx.close();
        getReceiver().closeStream();
    }

    /**
     * Creates a new node. If node already exists, creation is skipped.
     *
     * @param l Label of new node
     * @param v Value of property name
     * @param a Add :Active label
     * @return New node or node which matched query
     */
    private Node createNode(Label l, String v, boolean a) {
        Node n = graphDb.createNode(l);
        try {
            n.setProperty("name", v);
            if (a)
                n.addLabel(lsbLabels.ACTIVE);
        } catch (ConstraintViolationException e) {
            LOG.debug("Node with label {} and property name={} already exists. Ignoring node...", l.toString(), v);
            n.delete();
            n = graphDb.findNode(l, "name", v);
        }
        return n;
    }

    private enum lsbLabels implements Label {
        BIBLIOGRAPHICRESOURCE, PERSON, ORGANISATION, LOCALSIGNATURE, WORK, ACTIVE
    }

    private enum lsbRelations implements RelationshipType {
        CONTRIBUTOR, SIGNATUREOF, WORKOF
    }
}
