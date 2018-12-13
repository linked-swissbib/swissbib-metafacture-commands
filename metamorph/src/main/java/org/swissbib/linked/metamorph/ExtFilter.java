/*
 *  Copyright 2013, 2014 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.swissbib.linked.metamorph;


import org.metafacture.flowcontrol.StreamBuffer;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.metafacture.javaintegration.SingleValue;
import org.metafacture.metamorph.Metamorph;

/**
 * @author Markus Michael Geipel
 * @author Sebastian Schüpbach, project Swissbib
 */
@Description("Filters a stream based on a morph definition. A record is accepted if the morph returns at least one non empty value. Set filterNot to \"true\" to accept only records where there is no match.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public final class ExtFilter extends DefaultStreamPipe<StreamReceiver> {

    private final StreamBuffer buffer = new StreamBuffer();
    private final SingleValue singleValue = new SingleValue();
    private final Metamorph metamorph;
    private Boolean filterNot = false;

    public ExtFilter(final String morphDef) {
        super();
        metamorph = new Metamorph(morphDef);
        metamorph.setReceiver(singleValue);
    }

    public ExtFilter(final Metamorph metamorph) {
        super();
        this.metamorph = metamorph;
        metamorph.setReceiver(singleValue);
    }

    public void setFilterNot(String filterNot) {
        this.filterNot = Boolean.parseBoolean(filterNot);
    }

    @Override
    protected void onSetReceiver() {
        buffer.setReceiver(getReceiver());
    }


    private void dispatch() {
        final String key = singleValue.getValue();
        // Send record down the pipe if either there is a match or we want just the records where there mustn't be a match.
        if (key.isEmpty() == filterNot) {
            buffer.replay();
        }
        buffer.clear();
    }

    @Override
    public void startRecord(final String identifier) {
        buffer.startRecord(identifier);
        metamorph.startRecord(identifier);
    }

    @Override
    public void endRecord() {
        buffer.endRecord();
        metamorph.endRecord();
        dispatch();
    }

    @Override
    public void startEntity(final String name) {
        buffer.startEntity(name);
        metamorph.startEntity(name);
    }

    @Override
    public void endEntity() {
        buffer.endEntity();
        metamorph.endEntity();
    }

    @Override
    public void literal(final String name, final String value) {
        buffer.literal(name, value);
        metamorph.literal(name, value);
    }

    @Override
    protected void onResetStream() {
        buffer.clear();
        metamorph.resetStream();
    }

    @Override
    protected void onCloseStream() {
        buffer.clear();
        metamorph.closeStream();
    }
}
