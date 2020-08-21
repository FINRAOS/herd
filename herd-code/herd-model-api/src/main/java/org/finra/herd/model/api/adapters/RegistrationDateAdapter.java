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

            DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();

            DateTime dateTime = dateTimeFormatter.withZoneUTC().parseDateTime(v.trim());

            return DatatypeFactory.newInstance().newXMLGregorianCalendar(dateTime.toGregorianCalendar());
        }
        catch (IllegalArgumentException e)
        {
            // Since the original exception carries an error message which gives away 'too much information', we are having it
            // swallowed and instead returning a generic error message.
            throw new IllegalArgumentException("Valid date or date and time format must be used when specifying values for start/end registration dates.");
        }
    }

    @Override
    public String marshal(XMLGregorianCalendar v)
    {
        // ISO datetime format
        return v.toGregorianCalendar().toInstant().toString();
    }
}