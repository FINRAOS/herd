/*
 * Copyright 2015 herd contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.herd.model.api.adapters;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

public class RegistrationDateAdapter extends XmlAdapter<String, XMLGregorianCalendar>
{
    @Override
    public XMLGregorianCalendar unmarshal(String v) throws Exception
    {
        try
        {
            final DateTimeParser[] parsers =
                {
                    DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mmZ").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ").getParser()
                };

            DateTimeFormatter parser = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();

            DateTime dateTime = parser.withZoneUTC().parseDateTime(v.trim());

            return DatatypeFactory.newInstance().newXMLGregorianCalendar(dateTime.toGregorianCalendar());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Failed to parse 'startRegistrationDate' and/or 'endRegistrationDate'.");
        }
    }

    @Override
    public String marshal(XMLGregorianCalendar v) throws Exception
    {
        try
        {
            return v.toXMLFormat();
        }
        catch (Exception e)
        {
            throw new Exception("Error marshaling response.", e);
        }
    }
}
