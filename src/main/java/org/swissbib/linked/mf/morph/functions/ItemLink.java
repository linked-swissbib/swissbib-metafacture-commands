package org.swissbib.linked.mf.morph.functions;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;





public final class ItemLink extends AbstractSimpleStatelessFunction {


    @Override
    protected String process(String value) {
        return "create link with: " + value;
    }
}
