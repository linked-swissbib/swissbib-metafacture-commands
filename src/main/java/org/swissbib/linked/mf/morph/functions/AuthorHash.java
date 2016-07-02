package org.swissbib.linked.mf.morph.functions;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  A Hash function to create global unique identifiers for authors.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author bensmafx, Gesis, Köln
 *
 * @version 0.1
 *
 */
public class AuthorHash extends HashGenerator {


    private final String URI_PREFIX = "http://data.swissbib.ch/agent/";


    private static final Logger authorLogger ;

    static {
        authorLogger = LoggerFactory.getLogger(AuthorHash.class);
    }

    /**
     *
     * @param value
     * we expect for value a string which contains single tokens separated by ## delimiters
     * The single tokens are
     * - normalized
     * - concatenated
     * - as concatenated String used to generate a hash value
     * @return String
     * the generated hash value
     */

    @Override
    protected String process(String value) {

        String[] valueParts =  value.split("##");
        String globalIdentifier = NO_HASH;

        String authorType = checkTypeAndLength(valueParts);
        if (! authorType.equalsIgnoreCase(NO_HASH)  && checkCorrectNumberOfArguments(valueParts).equalsIgnoreCase(VALUES_OK)) {
            HashMap<String, String> mappedValues = mapValues(valueParts);

            StringBuilder normalizedValueParts = new StringBuilder();
            for (String valuePart : valueParts) {
                normalizedValueParts.append(charsToReplace.matcher(valuePart).replaceAll(""));
            }
            try {
                globalIdentifier = this.generateId(normalizedValueParts.toString());
            } catch (URISyntaxException syntaxException) {
                //Todo: better logging
                syntaxException.printStackTrace();

            }
        }
        return globalIdentifier;
    }

    private String checkTypeAndLength(String[] valueParts) {
        String check = null;
        if (!(valueParts != null && valueParts.length > 0)) {
            check = NO_HASH;
        } else {
            String authorType = valueParts[0];
            check = Arrays.asList("1000","1001","7000","7001").contains(authorType) ? authorType : NO_HASH;
        }

        return check;
    }

    private String checkCorrectNumberOfArguments(String[] values) {

        String check = NO_HASH;
        switch (values[0]) {
            case "1001":
            case "7001":
                check = values.length == 10 ? VALUES_OK : NO_HASH;
                break;
            case "1000":
            case "7000":
                check = values.length == 9 ? VALUES_OK : NO_HASH;
                break;
        }

        return check;
    }

    private HashMap<String,String> mapValues(String [] values) {

        HashMap<String, String> mappedValues = new HashMap<>();

        String authorType = values[0];
        switch (authorType) {
            case "1001":
            case "7001":
                mappedValues.put("typ",values [0]);
                mappedValues.put("swissbib_id",values [1]);
                mappedValues.put("number",values [2]);
                mappedValues.put("lastname",values [3]);
                mappedValues.put("firstname",values [4]);
                mappedValues.put("title",values [5]);
                mappedValues.put("lifedata",values [6]);
                mappedValues.put("fullname",values [7]);
                mappedValues.put("publishYear",values [8]);
                mappedValues.put("title245a",values [9]);

                break;
            case "1000":
            case "7000":
                mappedValues.put("typ",values [0]);
                mappedValues.put("swissbib_id",values [1]);
                mappedValues.put("number",values [2]);
                mappedValues.put("name",values [3]);
                mappedValues.put("title",values [4]);
                mappedValues.put("lifedata",values [5]);
                mappedValues.put("fullname",values [6]);
                mappedValues.put("publishYear",values [7]);
                mappedValues.put("title245a",values [8]);
                break;
        }


        return mappedValues;

    }

    private String pickUpValuesForHashKey(HashMap<String, String> mappedValues) {
        String stringForHashId = "";
        return stringForHashId;
    }



}
