package org.swissbib.linked.mf.morph.functions;


import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;
import org.culturegraph.mf.util.ResourceUtil;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;


class AlephStructure
{

    public String server;
    public String docLibrary;

}

class VirtuaStructure
{
    public String server;
}

class BacklinksTemplate
{
    public String urlTemplate;
}


public final class ItemLink extends AbstractSimpleStatelessFunction {


    protected HashMap< String, AlephStructure> alephNetworks;
    protected HashMap< String, VirtuaStructure> virtuaNetworks;
    protected HashMap< String, BacklinksTemplate> urlTemplates;


    public ItemLink() {


        Properties config = ResourceUtil.loadProperties("itemLinks/AlephNetworks.properties");
        Enumeration<Object> keys = config.keys();
        this.alephNetworks = new HashMap<String, AlephStructure>();
        while (keys.hasMoreElements()) {
            String alephKey =    (String) keys.nextElement();
            String[] keyValues = config.getProperty(alephKey).split(",");
            if (!(keyValues.length == 2)) continue;
            AlephStructure as = new AlephStructure();
            as.docLibrary = keyValues[1];
            as.server = keyValues[0];
            this.alephNetworks.put(alephKey, as);
        }

        config = ResourceUtil.loadProperties("itemLinks/VirtuaNetworks.properties");
        keys = config.keys();
        this.virtuaNetworks = new HashMap<String, VirtuaStructure>();
        while (keys.hasMoreElements()) {
            String key =    (String) keys.nextElement();
            String[] keyValues = config.getProperty(key).split(",");
            if (!(keyValues.length == 2)) continue;
            VirtuaStructure vs = new VirtuaStructure();
            vs.server = keyValues[0];
            this.virtuaNetworks.put(key, vs);
        }

        config = ResourceUtil.loadProperties("itemLinks/Backlinks.properties");
        keys = config.keys();
        this.urlTemplates = new HashMap<String, BacklinksTemplate>();
        while (keys.hasMoreElements()) {
            String key =    (String) keys.nextElement();
            BacklinksTemplate bT = new BacklinksTemplate();
            bT.urlTemplate = config.getProperty(key);
            this.urlTemplates.put(key, bT);
        }

    }

    @Override
    protected String process(String value) {

        String[] valueParts =  value.split("##");
        if (!this.checkValidity(valueParts)) return "";

        String itemURL = "";
        if (this.alephNetworks.containsKey(valueParts[0])) {
            itemURL = this.createAlephLink(valueParts);
        } else if (this.virtuaNetworks.containsKey(valueParts[0])) {
            itemURL = this.createVirtuaLink(valueParts);
        } else if (this.urlTemplates.containsKey(valueParts[0])) {
            itemURL = this.createBacklinkFromTemplate(valueParts);
        }

        return itemURL;

    }


    private boolean checkValidity (String [] valueParts)
    {
        return  (valueParts.length == 5);
    }


    private String createAlephLink (String [] valueParts) {


        return "";

    }

    private String createVirtuaLink (String [] valueParts) {

        return "";


    }

    private String createBacklinkFromTemplate (String [] valueParts) {

        return "";


    }
}
