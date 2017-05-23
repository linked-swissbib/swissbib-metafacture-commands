package org.swissbib.linked.mf.pipe;

import org.culturegraph.mf.framework.helpers.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts entities of a record on a certain level (default 0) and forwards them as individual records. Set level with
 * entityBoundary.
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.10.15
 */
@Description("Extracts entities of a record on a certain level (default 0) and forwards them as individual records.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public class EntitySplitter extends DefaultStreamPipe<StreamReceiver> {

    private final static Logger LOG = LoggerFactory.getLogger(EntitySplitter.class);

    private int nodeLevel = 0;
    private int entityBoundary = 0;


    public void setEntityBoundary(String entityBoundary) {
        this.entityBoundary = Integer.parseInt(entityBoundary);
        LOG.debug("Settings - Set node level for entity splitting: {}", entityBoundary);
    }

    @Override
    public void startRecord(String identifier) {
        LOG.debug("Processing record {}", identifier);
        assert !this.isClosed();
    }

    @Override
    public void endRecord() {
        assert !this.isClosed();
    }

    @Override
    public void startEntity(String name) {
        assert !this.isClosed();
        if (nodeLevel == entityBoundary) {
            LOG.trace("Splitting record on node level {}", nodeLevel);
            getReceiver().startRecord("");
        } else {
            getReceiver().startEntity(name);
        }
        nodeLevel++;
    }

    @Override
    public void endEntity() {
        assert !this.isClosed();
        nodeLevel--;
        if (nodeLevel == entityBoundary) {
            getReceiver().endRecord();
        } else {
            getReceiver().endEntity();
        }
    }

    @Override
    public void literal(String name, String value) {
        assert !this.isClosed();
        getReceiver().literal(name, value);
    }
}
