package org.swissbib.linked.mf.utils;

import com.github.jsonldjava.core.DocumentLoader;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.RemoteDocument;
import com.github.jsonldjava.utils.JsonUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class FileDocumentLoader extends DocumentLoader {

    private static Map<String, RemoteDocument> cache = null;
    private Map<String, File> map = null;


    public FileDocumentLoader(Map<String, File> map) {
        super();
        this.map = map;
        if(cache==null){
            cache = new HashMap<>();
        }
    }


    @Override
    public RemoteDocument loadDocument(String url) throws JsonLdError {

        if(cache.containsKey(url)){
            return cache.get(url);
        }

        RemoteDocument ctx = null;

        //try {
        ctx = super.loadDocument(url);
        //} catch (JsonLdError ioe) {
        //    exception = true;
        //}
        //if (ctx == null || exception) {
        //    File file = map.get(url);
        //    ctx = JsonUtils.fromInputStream(new FileInputStream(file), "UTF-8");
        //    if (file == null) {
        //        throw new JsonLdError("Unable to obtain a context.");
        //    }
        //}

        //cache context
        if(!cache.containsKey(url)){
            cache.put(url, ctx);
        }
        return ctx;

    }
}
