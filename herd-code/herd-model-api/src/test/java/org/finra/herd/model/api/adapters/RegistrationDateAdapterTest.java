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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Test;

public class RegistrationDateAdapterTest
{
    @Test
    public void unmarshalAllAllowedFormatsTest() throws Exception
    {
        RegistrationDateAdapter adapter = new RegistrationDateAdapter();

        // test for pattern: "yyyy-MM-dd"
        XMLGregorianCalendar calendar = adapter.unmarshal("2020-02-02");
        compareCalendarWithExpectedValues(calendar, 2020, 2, 2, 0, 0, 0);

        // test for pattern: "yyyy-MM-dd'T'HH:mm"
        calendar = adapter.unmarshal("2015-12-25T00:01");
        compareCalendarWithExpectedValues(calendar, 2015, 12, 25, 0, 1, 0);

        // test for pattern: "yyyy-MM-dd'T'HH:mmZ"
        calendar = adapter.unmarshal("2015-12-25T00:01+04:00");
        compareCalendarWithExpectedValues(calendar, 2015, 12, 24, 20, 1, 0);

        // test for pattern: "yyyy-MM-dd'T'HH:mm:ss"
        calendar = adapter.unmarshal("2018-02-03T10:00:02");
        compareCalendarWithExpectedValues(calendar, 2018, 2, 3, 10, 0, 2);

        // test for pattern: "yyyy-MM-dd'T'HH:mm:ssZ"
        calendar = adapter.unmarshal("2018-02-03T10:00:02+04:00");
        compareCalendarWithExpectedValues(calendar, 2018, 2, 3, 6, 0, 2);

        // test for pattern: "yyyy-MM-dd HH:mm:ssZ"
        calendar = adapter.unmarshal("2018-02-03 10:00:02+05:30");
        compareCalendarWithExpectedValues(calendar, 2018, 2, 3, 4, 30, 2);

        // test for pattern: "yyyy-MM-dd HH:mm:ss"
        calendar = adapter.unmarshal("2018-03-03 00:20:02");
        compareCalendarWithExpectedValues(calendar, 2018, 3, 3, 0, 20, 2);
    }

    private void compareCalendarWithExpectedValues(XMLGregorianCalendar calendar, int year, int month, int day, int hour, int minute, int second)
    {
        assertEquals(calendar.getYear(), year);
        assertEquals(calendar.getMonth(), month);
        assertEquals(calendar.getDay(), day);
        assertEquals(calendar.getHour(), hour);
        assertEquals(calendar.getMinute(), minute);
        assertEquals(calendar.getSecond(), second);

        // timezone is internally set to UTC in the unmarshaller
        assertEquals(calendar.getTimezone(), 0);
    }

    @Test
    public void unmarshalIllegalFormatTest()
    {
        RegistrationDateAdapter adapter = new RegistrationDateAdapter();

        try
        {
            adapter.unmarshal("2020-02");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("Valid date or date and time format must be used when specifying values for start/end registration dates."));
        }

        try
        {
            adapter.unmarshal("2020-02-02T12:02AM");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("Valid date or date and time format must be used when specifying values for start/end registration dates."));
        }
    }

    /**
     * An already initialized {@link XMLGregorianCalendar} instance is used by the marshaller so a lot of testing is not needed.
     */
    @Test
    public void marshalTest() throws Exception
    {

        // Generate a new XMLGregorianCalendar instance initialized at current time
        XMLGregorianCalendar calendar = DatatypeFactory.newInstance().newXMLGregorianCalendar();
        calendar.setYear(2020);
        calendar.setMonth(2);
        calendar.setDay(1);
        calendar.setHour(2);
        calendar.setMinute(59);
        calendar.setSecond(0);

        // Get its string representation
        String calendarStringRepresentation = calendar.toGregorianCalendar().toInstant().toString();

        // Initialize the object under test
        RegistrationDateAdapter adapter = new RegistrationDateAdapter();

        // marshal and compare
        assertEquals(calendarStringRepresentation, adapter.marshal(calendar));
    }
}
