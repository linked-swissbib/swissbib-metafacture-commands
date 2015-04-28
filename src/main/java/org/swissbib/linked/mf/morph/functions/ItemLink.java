package org.swissbib.linked.mf.morph.functions;


import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;
import org.culturegraph.mf.util.ResourceUtil;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
    protected HashMap<String, Pattern> systemNumberPattern;


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


        //(RERO)R004689410!!(SGBN)000187319!!(NEBIS)000262918!!(IDSBB)000217483
        //we need (!!|$) to catch a pattern (IDSBB)000217483 at the end of the string (without !! as delimiter)
        Pattern p = Pattern.compile("\\(IDSBB\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern = new HashMap<String, Pattern>();
        this.systemNumberPattern.put("IDSBB", p);
        p = Pattern.compile("\\(RERO\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("RERO", p);

        p = Pattern.compile("\\(SGBN\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("SGBN", p);

        p = Pattern.compile("\\(NEBIS\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("NEBIS", p);

        p = Pattern.compile("\\(IDSSG\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("IDSSG", p);

        p = Pattern.compile("\\(IDSSG2\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("IDSSG2", p);

        p = Pattern.compile("\\(IDSLU\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("IDSLU", p);

        p = Pattern.compile("\\(BGR\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("BGR", p);

        p = Pattern.compile("\\(ABN\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("ABN", p);

        p = Pattern.compile("\\(SBT\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("SBT", p);

        p = Pattern.compile("\\(ABN\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("ABN", p);

        p = Pattern.compile("\\(SNL\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("SNL", p);

        p = Pattern.compile("\\(ALEX\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("ALEX", p);

        p = Pattern.compile("\\(CCSA\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("CCSA", p);

        p = Pattern.compile("\\(CHARCH\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("CHARCH", p);
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
        return  (valueParts.length == 6);
    }


    private String createAlephLink (String [] valueParts) {

        //#ALEPH  "{server}/F?func=item-global&doc_library={bib-library-code}&doc_number={bib-system-number}&sub_library={aleph-sublibrary-code}"

        if (!this.systemNumberPattern.containsKey(valueParts[0])) return "";

        Pattern p = this.systemNumberPattern.get(valueParts[0]);
        Matcher m = p.matcher(valueParts[5]);
        String url = "";
        String subLibraryCode = valueParts[2].length() > 0 ? valueParts[2] : valueParts[1].length() > 0 ? valueParts[1] : null;
        if (subLibraryCode != null &&  m.find()) {

            url = String.format(this.urlTemplates.get("ALEPH").urlTemplate,
                    this.alephNetworks.get(valueParts[0]).server,
                    this.alephNetworks.get(valueParts[0]).docLibrary,
                    m.group(1),
                    subLibraryCode);

        }

        return url;

    }

    private String createVirtuaLink (String [] valueParts) {

        return "";


    }

    private String createBacklinkFromTemplate (String [] valueParts) {

        return "";


    }


}
