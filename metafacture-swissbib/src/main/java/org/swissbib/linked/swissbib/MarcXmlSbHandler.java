/*
 *  Copyright 2013, 2014 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.swissbib.linked.swissbib;

import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.XmlReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultXmlPipe;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;


/**
 * A marc xml reader.
 *
 * @author Markus Michael Geipel
 */
@Description("A marc xml reader")
@In(XmlReceiver.class)
@Out(String.class)
public final class MarcXmlSbHandler extends DefaultXmlPipe<ObjectReceiver<String>> {

    private static final String SUBFIELD = "subfield";
    private static final String DATAFIELD = "datafield";
    private static final String CONTROLFIELD = "controlfield";
    private static final String LEADER = "leader";
    private static final String RECORD = "record";
    private String currentControlfieldTag = "";
    private String currentDatafieldTag = "";
    private String currentDatafieldInd1 = "";
    private String currentDatafieldInd2 = "";
    private String currentSubfieldTag = "";
    private String currentId = "";
    private String currentLeader = "";
    private StringBuilder builder = new StringBuilder();

    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes attributes)
            throws SAXException {
        if (SUBFIELD.equals(localName)) {
            builder = new StringBuilder();
            currentSubfieldTag = attributes.getValue("code");
        } else if (DATAFIELD.equals(localName)) {
            currentDatafieldTag = attributes.getValue("tag");
            currentDatafieldInd1 = attributes.getValue("ind1");
            currentDatafieldInd2 = attributes.getValue("ind2");
        } else if (CONTROLFIELD.equals(localName)) {
            builder = new StringBuilder();
            currentControlfieldTag = attributes.getValue("tag");
        } else if (LEADER.equals(localName)) {
            builder = new StringBuilder();
        }
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) throws SAXException {
        String currentValue = builder
                .toString()
                .trim()
                .replaceAll("\"", "'")
                .replaceAll("\\p{C}", "?"); // Replace all non-printable characters
        if (SUBFIELD.equals(localName)) {
            getReceiver().process(
                    "\"" + currentId + "\"," +
                            "\"" + currentDatafieldTag + "\"," +
                            "\"" + currentDatafieldInd1 + "\"," +
                            "\"" + currentDatafieldInd2 + "\"," +
                            "\"" + currentSubfieldTag + "\"," +
                            "\"" + currentValue + "\"");
        } else if (CONTROLFIELD.equals(localName)) {
            if (currentControlfieldTag.equals("001")) {
                currentId = currentValue;
            }
            getReceiver().process(
                    "\"" + currentId + "\"," +
                            "\"" + currentControlfieldTag + "\"," +
                            "\"\",\"\",\"\"," +
                            "\"" + currentValue + "\"\"");
        } else if (LEADER.equals(localName)) {
            currentLeader = currentValue;
        } else if (RECORD.equals(localName)) {
            getReceiver().process(
                    "\"" + currentId + "\"," +
                            "\"leader\",\"\",\"\",\"\"," +
                            "\"" + currentLeader + "\"\"");

        }
    }

    @Override
    public void characters(final char[] chars, final int start, final int length) throws SAXException {
        builder.append(chars, start, length);
    }

}
