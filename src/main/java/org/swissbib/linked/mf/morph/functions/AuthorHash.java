package org.swissbib.linked.mf.morph.functions;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;

import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * A Hash function to create global unique identifiers for authors.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author bensmafx, Gesis, Köln
 * @version 0.1
 */
public class AuthorHash extends AbstractSimpleStatelessFunction {

    private Pattern charsToReplace = Pattern.compile(",| *", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);


    /**
     * @param value we expect for value a string which contains single tokens separated by ## delimiters
     *              we don't validate the tokens if they are in a defined sequence. The single tokens are
     *              - normalized
     *              - concatenated
     *              - as concatenated String used to generate a hash value
     * @return String
     * the generated hash value
     */

    @Override
    protected String process(String value) {

        String globalIdentifier = "";
        String[] valueParts = value.split("##");
        StringBuilder normalizedValueParts = new StringBuilder();
        for (String valuePart : valueParts) {
            normalizedValueParts.append(this.charsToReplace.matcher(valuePart).replaceAll(""));
        }
        try {
            globalIdentifier = this.generateAuthorId(normalizedValueParts.toString());
        } catch (URISyntaxException syntaxException) {
            //Todo: better logging
            syntaxException.printStackTrace();

        }
        return globalIdentifier;
    }


    private String generateAuthorId(String name) throws URISyntaxException {
        String normalizedName;
        //decompose unicode characters eg. é -> e´
        if (!Normalizer.isNormalized(name, Normalizer.Form.NFD)) {
            normalizedName = Normalizer.normalize(name, Normalizer.Form.NFD);
        } else {
            normalizedName = name;
        }
        //remove diacritical marks
        normalizedName = normalizedName.replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+", "");
        //transform to lower case characters
        normalizedName = normalizedName.toLowerCase();
        //URL generation
        UUID uuid = UUID.nameUUIDFromBytes(normalizedName.getBytes(Charset.forName("UTF-8")));
        //return new URI(this.URI_PREFIX + uuid.toString());
        return uuid.toString();
    }

}
