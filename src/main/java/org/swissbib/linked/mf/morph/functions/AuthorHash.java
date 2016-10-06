package org.swissbib.linked.mf.morph.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * A Hash function to create global unique identifiers for authors.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author bensmafx, Gesis, Köln
 * @version 0.1
 */
public class AuthorHash extends HashGenerator {


    private final static List<String> author1001x7001x;
    private final static List<String> author1000x7000x;
    private final static List<String> organisationList;
    private static final Logger hash1000x7000;
    private static final Logger hash1001x7001;
    private static final Logger hash710;
    private static final Logger hash711;
    private static final Logger hashById;

    static {
        hash1000x7000 = LoggerFactory.getLogger("hash1000x7000");
        hash1001x7001 = LoggerFactory.getLogger("hash1001x7001");
        hash710 = LoggerFactory.getLogger("hash710");
        hash711 = LoggerFactory.getLogger("hash711");
        hashById = LoggerFactory.getLogger("hashById");
        author1001x7001x = Arrays.asList("1001", "7001");
        author1000x7000x = Arrays.asList("1000", "7000");
        organisationList = Arrays.asList("710__", "711__");

    }

    private final String URI_PREFIX = "http://data.swissbib.ch/agent/";

    /**
     * @param value we expect for value a string which contains single tokens separated by ## delimiters
     *              The single tokens are
     *              - normalized
     *              - concatenated
     *              - as concatenated String used to generate a hash value
     * @return String
     * the generated hash value
     */

    @Override
    protected String process(String value) {

        // There are a very small number of cases where field 245 .a does not exist
        // (the last token in value. In these cases we append a whitespace to avoid a failed
        // check in method checkCorrectNumberOfArguments
        String[] valueParts = (value.endsWith("###") ? value + " " : value).split("###");
        String globalIdentifier = NO_HASH;

        //bisher nur 100 und 700, brauche ich nicht mehr?
        String authorType = checkTypeAndLength(valueParts);
        if (!authorType.equalsIgnoreCase(NO_HASH) && checkCorrectNumberOfArguments(valueParts).equalsIgnoreCase(VALUES_OK)) {
            HashMap<String, String> mappedValues = mapValues(valueParts);
            try {
                globalIdentifier = this.hashKeyByAuthorityID(mappedValues);
                //we couldn't create a hash ID with authority number
                if (globalIdentifier.equals(NO_HASH)) {
                    if (isOrganisation(mappedValues)) {
                        //by now treatment for organisations and others are differnt
                        //to be proved!
                        globalIdentifier = this.hashKeyForOrganisations(mappedValues);

                    } else {
                        globalIdentifier = this.hashKeyByNameAndLifeDates(mappedValues);

                        if (globalIdentifier.equals(NO_HASH)) {
                            globalIdentifier = this.hashKeyByNameAndTitle(mappedValues);
                        }
                    }
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
        String check;
        if (!(valueParts != null && valueParts.length > 0)) {
            check = NO_HASH;
        } else {
            String authorType = valueParts[0];
            check = Arrays.asList("1000", "1001", "7000", "7001", "710__", "711__").contains(authorType) ? authorType : NO_HASH;
        }

        return check;
    }

    private String checkCorrectNumberOfArguments(String[] values) {

        String check = NO_HASH;
        switch (values[0]) {
            case "1001":
            case "7001":
                check = values.length == 11 ? VALUES_OK : NO_HASH;
                break;
            case "1000":
            case "7000":
            case "710__":
            case "711__":
                check = values.length == 10 ? VALUES_OK : NO_HASH;
                break;
        }

        return check;
    }

    private HashMap<String, String> mapValues(String[] values) {

        HashMap<String, String> mappedValues = new HashMap<>();

        String authorType = values[0];
        switch (authorType) {
            case "1001":
            case "7001":
                mappedValues.put("type", values[0]);
                mappedValues.put("swissbib_id", values[1]);
                mappedValues.put("number", values[2]);
                mappedValues.put("lastname", values[3]);
                mappedValues.put("firstname", values[4]);
                mappedValues.put("title", values[5]);
                mappedValues.put("lifedata", values[6]);
                mappedValues.put("fullname", values[7]);
                mappedValues.put("gnd", values[8]);
                mappedValues.put("publishYear", values[9]);
                mappedValues.put("title245a", values[10]);

                break;
            case "1000":
            case "7000":
                mappedValues.put("type", values[0]);
                mappedValues.put("swissbib_id", values[1]);
                mappedValues.put("number", values[2]);
                mappedValues.put("name", values[3]);
                mappedValues.put("title", values[4]);
                mappedValues.put("lifedata", values[5]);
                mappedValues.put("fullname", values[6]);
                mappedValues.put("gnd", values[7]);
                mappedValues.put("publishYear", values[8]);
                mappedValues.put("title245a", values[9]);
                break;
            case "710__":
            case "711__":
                mappedValues.put("type", values[0]);
                mappedValues.put("swissbib_id", values[1]);
                mappedValues.put("number", values[2]);
                mappedValues.put("name", values[3]);
                mappedValues.put("subunit", values[4]);
                mappedValues.put("date", values[5]);
                mappedValues.put("location", values[6]);
                mappedValues.put("gnd", values[7]);
                mappedValues.put("publishYear", values[8]);
                mappedValues.put("title245a", values[9]);
        }


        return mappedValues;

    }

    private String hashKeyByAuthorityID(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;
        if (this.checkForValidValue(mappedValues, "gnd")) {

            //todo: check in GND for identified person
            //079$b P -> identified person
            //079$b n  -> simple named person (not identified)
            //identified persons are more reliable. But even this should be analyzed in more details
            stringForHashId = this.generateId(mappedValues.get("gnd"));
            hashById.debug(String.format("hashKeyById (id): %s / (gnd): %s", mappedValues.get("swissbib_id"),
                    mappedValues.get("gnd")));
        }

        return stringForHashId;
    }


    private String hashKeyByNameAndLifeDates(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;

        if (author1001x7001x.contains(mappedValues.get("type"))) {
            if ((this.checkForValidValue(mappedValues, "lastname") ||
                    this.checkForValidValue(mappedValues, "firstname")) &&
                    this.checkForValidValue(mappedValues, "lifedata")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                                mappedValues.get("lastname"),
                                mappedValues.get("firstname"),
                                mappedValues.get("title"),
                                mappedValues.get("lifedata")
                                )
                        ));
                hash1001x7001.debug(String.format("hashKeyByNameAndLifeDates: (id) - %s ", mappedValues.get("swissbib_id")));

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
                hash1000x7000.debug(String.format("hashKeyByNameAndLifeDates: (id) - %s ", mappedValues.get("swissbib_id")));

            }
        }

        return stringForHashId;
    }


    private String hashKeyByNameAndTitle(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;

        if (author1001x7001x.contains(mappedValues.get("type"))) {
            if (this.checkForValidValue(mappedValues, "lastname") ||
                    this.checkForValidValue(mappedValues, "firstname")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                                mappedValues.get("lastname"),
                                mappedValues.get("firstname"),
                                mappedValues.get("title"),
                                mappedValues.get("title245a")
                                )
                        ));
                hash1001x7001.debug(String.format("hashKeyByNameAndTitle: (id) - %s ", mappedValues.get("swissbib_id")));

            }
        } else if (author1000x7000x.contains(mappedValues.get("type"))) {
            if (this.checkForValidValue(mappedValues, "name")) {

                stringForHashId = this.generateId(
                        this.concatenateAndNormalizeValueParts(Arrays.asList(
                                mappedValues.get("name"),
                                mappedValues.get("title"),
                                mappedValues.get("title245a")
                                )
                        ));
                hash1000x7000.debug(String.format("hashKeyByNameAndTitle: (id) - %s ", mappedValues.get("swissbib_id")));

            }
        }

        return stringForHashId;
    }


    private String hashKeyForOrganisations(HashMap<String, String> mappedValues)
            throws URISyntaxException {
        String stringForHashId = NO_HASH;

        stringForHashId = this.generateId(
                this.concatenateAndNormalizeValueParts(Arrays.asList(
                        mappedValues.get("name"),
                        mappedValues.get("subunit"),
                        mappedValues.get("date"),
                        mappedValues.get("location")
                        )
                ));

        if (mappedValues.get("type").equals("710__")) {
            hash710.debug(String.format("hashKeyOrganisation 710: (id) - %s ", mappedValues.get("swissbib_id")));
        } else {
            hash711.debug(String.format("hashKeyOrganisation 711: (id) - %s", mappedValues.get("swissbib_id")));
        }


        return stringForHashId;
    }


    private boolean checkForValidValue(HashMap<String, String> mappedValues, String key) {
        return (mappedValues.containsKey(key) && mappedValues.get(key) != null) &&
                !mappedValues.get(key).isEmpty();

    }

    private boolean isOrganisation(HashMap<String, String> mappedValues) {
        return mappedValues != null && mappedValues.size() > 0 &&
                organisationList.contains(mappedValues.get("type"));
    }


}
