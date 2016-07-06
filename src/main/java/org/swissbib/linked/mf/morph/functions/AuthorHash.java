package org.swissbib.linked.mf.morph.functions;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.*;
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

    private final static List<String> author1001x7001x;
    private final static List<String> author1000x7000x;


    private static final Logger authorLogger ;

    static {
        authorLogger = LoggerFactory.getLogger(AuthorHash.class);
        author1001x7001x =  Arrays.asList("1001", "7001");
        author1000x7000x =  Arrays.asList("1001", "7001");

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

        //bisher nur 100 und 700, brauche ich nicht mehr?
        String authorType = checkTypeAndLength(valueParts);
        if (! authorType.equalsIgnoreCase(NO_HASH)  && checkCorrectNumberOfArguments(valueParts).equalsIgnoreCase(VALUES_OK)) {
            HashMap<String, String> mappedValues = mapValues(valueParts);
            try {

                globalIdentifier = this.hashKeyByAuthorityID(mappedValues);
                //we couldn't create a hash ID with authority number
                if (globalIdentifier.equals(NO_HASH)) {
                    globalIdentifier = this.hashKeyByNameAndLifeDates(mappedValues);

                } if (globalIdentifier.equals(NO_HASH)) {
                    globalIdentifier = this.hashKeyByNameAndTitle(mappedValues);
                }


                //globalIdentifier = this.generateId(normalizedValueParts.toString());
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
                mappedValues.put("type",values [0]);
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
                mappedValues.put("type",values [0]);
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

    private String hashKeyByAuthorityID(HashMap<String, String> mappedValues)
                   throws URISyntaxException {
        String stringForHashId = NO_HASH;
        if (this.checkForValidValue(mappedValues, "number")) {

            //todo: check in GND for identified person
            //079$b P -> identified person
            //079$b n  -> simple named person (not identified)
            //identified persons are more reliable. But even this should be analyzed in more details
            stringForHashId =  this.generateId( mappedValues.get("number"));
        }

        return stringForHashId;
    }


    private String hashKeyByNameAndLifeDates(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;

        if (author1001x7001x.contains(mappedValues.get("type"))) {
            if ((this.checkForValidValue(mappedValues, "lastname") ||
                    this.checkForValidValue(mappedValues, "firstname")) &&
                    this.checkForValidValue(mappedValues,"lifedata")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                        mappedValues.get("lastname"),
                        mappedValues.get("firstname"),
                        mappedValues.get("title"),
                        mappedValues.get("lifedata")
                        )
                ));

            }
        } else if (author1000x7000x.contains(mappedValues.get("type"))) {
            if (this.checkForValidValue(mappedValues, "fullname") &&
                    this.checkForValidValue(mappedValues, "lifedata")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                        mappedValues.get("fullname"),
                        mappedValues.get("title"),
                        mappedValues.get("lifedata")
                        )
                ));

            }
        }

        return stringForHashId;
    }


    private String hashKeyByNameAndTitle(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;

        if (author1001x7001x.contains(mappedValues.get("type"))) {
            if (this.checkForValidValue(mappedValues, "lastname") ||
                    this.checkForValidValue(mappedValues, "firstname")){

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                        mappedValues.get("lastname"),
                        mappedValues.get("firstname"),
                        mappedValues.get("title"),
                        mappedValues.get("title245a")
                        )
                ));

            }
        } else if (author1000x7000x.contains(mappedValues.get("type"))) {
            if (this.checkForValidValue(mappedValues, "fullname")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                        mappedValues.get("fullname"),
                        mappedValues.get("title"),
                        mappedValues.get("title245a")
                        )
                ));

            }
        }

        return stringForHashId;
    }


    private boolean checkForValidValue(HashMap<String, String> mappedValues, String key) {
        return (mappedValues.containsKey(key) && mappedValues.get(key) != null ) &&
            !mappedValues.get(key).isEmpty();

    }



}
