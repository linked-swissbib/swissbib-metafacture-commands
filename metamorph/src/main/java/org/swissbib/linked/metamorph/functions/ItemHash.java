package org.swissbib.linked.metamorph.functions;

import java.net.URISyntaxException;

/**
 * Created by swissbib on 6/30/16.
 */
public class ItemHash extends HashGenerator {


    /**
     * @param value we expect for value a string which contains single tokens separated by ## delimiters
     *              we don't validate the tokens if they are in a defined sequence. The single tokens are
     *              - normalized
     *              - concatenated
     *              - as concatenated String used to generate a hash value
     * @return String
     * the generated hash value
     */

    protected String process(String value) {


        String globalIdentifier = NO_HASH;
        String[] valueParts = value.split("##");
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
        return globalIdentifier;
    }


}
