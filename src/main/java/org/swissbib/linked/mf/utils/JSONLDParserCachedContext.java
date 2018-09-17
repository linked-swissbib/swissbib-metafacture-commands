package org.swissbib.linked.mf.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.jsonld.JSONLDParser;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class JSONLDParserCachedContext extends JSONLDParser {

    private Map<String, File> contextMap;

    public JSONLDParserCachedContext(Map<String, File> contextMap) {
        super();
        this.contextMap = contextMap;
    }

    @Override
    public void parse(Reader reader, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
        clear();

        try {
            final JSONLDInternalTripleCallback callback = new JSONLDInternalTripleCallback(getRDFHandler(),
                    valueFactory, getParserConfig(), getParseErrorListener(), nodeID -> createNode(nodeID),
                    () -> createNode());

            final JsonLdOptions options = new JsonLdOptions(baseURI);
            options.useNamespaces = true;
            options.setDocumentLoader(new FileDocumentLoader(contextMap));

            JsonLdProcessor.toRDF(JsonUtils.fromReader(reader), callback, options);
        }
        catch (final JsonLdError e) {
            throw new RDFParseException("Could not parse JSONLD", e);
        }
        catch (final JsonParseException e) {
            throw new RDFParseException("Could not parse JSONLD", e);
        }
        catch (final RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof RDFParseException) {
                throw (RDFParseException)e.getCause();
            }
            throw e;
        }
        finally {
            clear();
        }




    }
}
