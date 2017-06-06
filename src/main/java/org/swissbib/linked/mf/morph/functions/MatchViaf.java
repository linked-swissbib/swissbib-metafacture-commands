package org.swissbib.linked.mf.morph.functions;

/**
 *
 * @author Guenter Hipler, project swissbib, UB Basel
 * @version 0.1
 *
 */


import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.culturegraph.mf.metamorph.api.helpers.AbstractSimpleStatelessFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;


/**
 * used to match author strings against VIAF index
 * aim: integrate VIAF Id as anchor for additional relations
*/
public class MatchViaf extends AbstractSimpleStatelessFunction {


    protected final static String fixVIAFResorce;
    protected static String indexUrl;
    protected static HttpSolrClient solrClient;

    static {

        indexUrl = System.getProperty("indexURL");
        solrClient = new HttpSolrClient(indexUrl);
        fixVIAFResorce = "http://viaf.org/viaf/";
        //s. auch
        //curl -H "Accept: application/json" http://data.linkeddatafragments.org/viaf?subject=http://viaf.org/viaf/95218067

    }


    @Override
    protected String process(String value) {

        //could be used to deduplicate IDs - later
        HashSet<String> uniqueSet = new HashSet<>();

        //uniqueSet.addAll(Arrays.asList(splittedValues));

        StringBuilder viafIds = new StringBuilder();
        SolrQuery parameters = new SolrQuery();
        ArrayList<String> queryStrings = this.createQueryStrings(value);

        if (queryStrings.size() > 0) {
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < queryStrings.size(); i++) {
                b.append("matchstring:").append(queryStrings.get(i));
                if (i < queryStrings.size() -1 ) {
                    b.append(" OR ");
                }
            }

            parameters.set("q",b.toString() );
            //more than 100 rows doesn't make sense
            parameters.set("rows",100);
            QueryResponse response = null;
            try {
                response = solrClient.query(parameters);
                 //long numFound = response.getResults().getNumFound();
            } catch (SolrServerException | IOException serverException) {
                serverException.printStackTrace();
            } finally {
                if (null != response) {
                    for (SolrDocument doc : response.getResults()) {
                        viafIds.append(fixVIAFResorce).append(doc.getFieldValue("id").toString().substring(4)).append(",");
                    }
                }
            }
        }

        return viafIds.length() > 0 ? viafIds.toString().substring(0,viafIds.length() - 1) : "";
    }

    protected ArrayList<String> createQueryStrings(String values) {
        ArrayList<String> queryStrings = new ArrayList<>();
        //expected values
        //code D     ##   code a  ##  code b ##  code t ##   code d ##   code q  ## fix
        //{firstname}##${lastname}##${number}##${title}##${lifedata}##${fullname}##personname"

        //String normalized = "";
        String[] tokenized = values.split("##");

        if (tokenized.length == 7 && tokenized[6].equalsIgnoreCase("personname")  ) {
            if (tokenized[0].length() > 0) {
                //code D is available as forename
                queryStrings.add(this.normalizeValue(tokenized[1]) +
                        this.normalizeValue(tokenized[0]) +
                        this.normalizeValue(tokenized[2]) + //number
                        this.normalizeValue(tokenized[3]) + //title
                        this.normalizeDates(this.normalizeValue(tokenized[4]))); //lifedate

            }

            //did we get a fullname as well
            if (tokenized[5].length() > 0) {
                queryStrings.add(this.normalizeValue(tokenized[5]) +
                        this.normalizeValue(tokenized[2]) + //number
                        this.normalizeValue(tokenized[3]) + //title
                        this.normalizeDates(this.normalizeValue(tokenized[4]))); //lifedate

            }
        }

        return queryStrings;
    }


    protected String normalizeValue(String value) {
        return value.replaceAll("[^\\p{L}\\p{M}*\\p{N}|]", "");
    }

    protected String normalizeDates(String value) {
        return value.replaceAll("[^0-9]", "");
    }

}
