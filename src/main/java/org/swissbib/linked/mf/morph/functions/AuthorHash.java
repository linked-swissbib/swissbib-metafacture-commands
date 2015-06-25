package org.swissbib.linked.mf.morph.functions;

import org.culturegraph.mf.morph.functions.AbstractSimpleStatelessFunction;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.Normalizer;
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
public class AuthorHash extends AbstractSimpleStatelessFunction {


    private final String URI_PREFIX = "http://data.swissbib.ch/agent/";

    private Pattern charsToReplace = Pattern.compile(",| *",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);


    @Override
    protected String process(String value) {

        String globalIdentifier = "";
        String[] valueParts =  value.split("##");
        try {
            switch (valueParts.length) {
                case 1:
                    Matcher cleanerName = this.charsToReplace.matcher(valueParts[0]);
                    String name = cleanerName.replaceAll("");
                    globalIdentifier = this.generateAuthorId(name).toString();
                    break;
                case 2:
                    Matcher cleanerFirstName = this.charsToReplace.matcher(valueParts[0]);
                    String firstName = cleanerFirstName.replaceAll("");
                    Matcher cleanerLastName = this.charsToReplace.matcher(valueParts[1]);
                    String lastName = cleanerLastName.replaceAll("");
                    globalIdentifier = this.generateAuthorId(firstName + lastName).toString();
                    break;

            }
        } catch (URISyntaxException syntaxException) {
            //Todo: better logging
            syntaxException.printStackTrace();
        }


        return globalIdentifier;
    }


    private URI generateAuthorId(String name) throws URISyntaxException {
        String normalizedName = null;
        //decompose unicode characters eg. é -> e´
        if (!Normalizer.isNormalized(name, Normalizer.Form.NFD)) {
            normalizedName = Normalizer.normalize(name, Normalizer.Form.NFD);
        } else {
            normalizedName = name;
        }
        //remove diacritical marks
        normalizedName = normalizedName.replaceAll( "[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+", "" );
        //transform to lower case characters
        normalizedName = normalizedName.toLowerCase();
        //URL generation
        UUID uuid = UUID.nameUUIDFromBytes(normalizedName.getBytes(Charset.forName("UTF-8")));
        return new URI(this.URI_PREFIX + uuid.toString());
    }

}
