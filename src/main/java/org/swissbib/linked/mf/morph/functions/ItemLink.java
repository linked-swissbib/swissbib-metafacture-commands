package org.swissbib.linked.mf.morph.functions;


import org.metafacture.commons.ResourceUtil;
import org.metafacture.metamorph.api.helpers.AbstractSimpleStatelessFunction;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class AlephStructure
{

    String server;
    String docLibrary;

}

class VirtuaStructure
{
    String server;
}

class BacklinksTemplate
{
    String urlTemplate;
}


final class ItemLink extends AbstractSimpleStatelessFunction {


    private HashMap< String, AlephStructure> alephNetworks;
    private HashMap< String, VirtuaStructure> virtuaNetworks;
    private HashMap< String, BacklinksTemplate> urlTemplates;
    private HashMap<String, Pattern> systemNumberPattern;
    private final Pattern pNebisReplacePattern = Pattern.compile("\\{bib-system-number}");


    public ItemLink() throws IOException {


        Properties config = ResourceUtil.loadProperties("itemLinks/AlephNetworks.properties");
        Enumeration<Object> keys = config.keys();
        this.alephNetworks = new HashMap<>();
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
        this.virtuaNetworks = new HashMap<>();
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
        this.urlTemplates = new HashMap<>();
        while (keys.hasMoreElements()) {
            String key =    (String) keys.nextElement();
            BacklinksTemplate bT = new BacklinksTemplate();
            bT.urlTemplate = config.getProperty(key);
            this.urlTemplates.put(key, bT);
        }


        //(RERO)R004689410!!(SGBN)000187319!!(NEBIS)000262918!!(IDSBB)000217483
        //we need (!!|$) to catch a pattern (IDSBB)000217483 at the end of the string (without !! as delimiter)
        Pattern p = Pattern.compile("\\(IDSBB\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern = new HashMap<>();
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

        p = Pattern.compile("\\(LIBIB\\)(.*?)(!!|$)",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
        this.systemNumberPattern.put("LIBIB", p);

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

        if (!this.systemNumberPattern.containsKey(valueParts[0])) return "";
        if (!this.urlTemplates.containsKey(valueParts[0])) return "";


        Pattern p = this.systemNumberPattern.get(valueParts[0]);
        Matcher m = p.matcher(valueParts[5]);
        if (!m.find()) return "";
        String url = "";
        String subLibraryCode = valueParts[2].length() > 0 ? valueParts[2] : valueParts[1].length() > 0 ? valueParts[1] : null;
        String bibSysNumber;
        switch (valueParts[0]) {
            case "IDSBB":
                //IDSBB  "http://baselbern.swissbib.ch/Record/{id}?expandlib={sub-library-code}#holding-institution-{network}-{sub-library-code}"
                if (subLibraryCode != null) {
                    url = String.format(this.urlTemplates.get(valueParts[0]).urlTemplate,valueParts[4],subLibraryCode,valueParts[0],subLibraryCode);
                }
                break;
            case "NEBIS":
                //http://recherche.nebis.ch/primo_library/libweb/action/display.do?tabs=locationsTab&ct=display&fn=search&doc=ebi01_prod{bib-system-number}&indx=1&recIds=ebi01_prod{bib-system-number}&recIdxs=0&elementId=0&renderMode=poppedOut&displayMode=full&frbrVersion=&dscnt=0&scp.scps=scope%3A%28ebi01_prod%29&frbg=&tab=default_tab&vl%28585331958UI1%29=all_items&srt=rank&mode=Basic&dum=true&tb=t&vid=NEBIS
                bibSysNumber = m.group(1);
                Matcher nebisMatcher = this.pNebisReplacePattern.matcher(this.urlTemplates.get(valueParts[0]).urlTemplate);
                url = nebisMatcher.replaceAll(bibSysNumber);
                break;
            case "IDSSG":
                //"http://aleph.unisg.ch/php/bib_holdings.php?docnr={bib-system-number}"
                bibSysNumber = m.group(1);
                url = String.format(this.urlTemplates.get(valueParts[0]).urlTemplate, bibSysNumber);
                break;
            case "CHARCH":
                break;
            case "CCSA":
                break;
            case "RERO":
                if (subLibraryCode != null) {
                    int RERO_network_code = Integer.valueOf(subLibraryCode.substring(2, 4));
                    bibSysNumber = m.group(1);
                    String sub_library_code_rero  = subLibraryCode.replaceAll("[\\D]","");
                    url = String.format(this.urlTemplates.get(valueParts[0]).urlTemplate,"en",RERO_network_code,bibSysNumber,sub_library_code_rero);
                }
                break;
            case "SNL":
                bibSysNumber = m.group(1);
                String bibSysNumber_snl = bibSysNumber.replaceAll("^vtls0*","");
                url = String.format(this.urlTemplates.get(valueParts[0]).urlTemplate,bibSysNumber_snl);
                break;
            default:
                break;

        }

        return url;

    }


}
