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
package org.finra.herd.core;

import static org.finra.herd.core.HerdDateUtils.MILLIS_PER_DAY;
import static org.finra.herd.core.HerdDateUtils.MILLIS_PER_HOUR;
import static org.finra.herd.core.HerdDateUtils.MILLIS_PER_MINUTE;
import static org.finra.herd.core.HerdDateUtils.MILLIS_PER_SECOND;
import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.util.Calendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Test;

/**
 * Test driver for the DateUtils class.
 */
public class HerdDateUtilsTest extends AbstractCoreTest
{
    @Test
    public void testGetXMLGregorianCalendarValue()
    {
        Calendar calendar = Calendar.getInstance();
        XMLGregorianCalendar gregorianCalendar = HerdDateUtils.getXMLGregorianCalendarValue(calendar.getTime());
        assertEquals(calendar.getTime(), gregorianCalendar.toGregorianCalendar().getTime());
    }

    @Test
    public void testFormatDurationZeroMilliseconds()
    {
        String result = HerdDateUtils.formatDuration(0);
        assertEquals("0 Milliseconds", result);
    }

    @Test
    public void testFormatDurationDays()
    {
        int length = 100;
        String result = HerdDateUtils.formatDuration(MILLIS_PER_DAY * length);
        assertEquals(length + " Days", result);
    }

    @Test
    public void testFormatDurationHours()
    {
        String result = HerdDateUtils.formatDuration(MILLIS_PER_HOUR);
        assertEquals("1 Hour", result);
    }

    @Test
    public void testFormatDurationMinutes()
    {
        String result = HerdDateUtils.formatDuration(MILLIS_PER_MINUTE);
        assertEquals("1 Minute", result);
    }

    @Test
    public void testFormatDurationSeconds()
    {
        String result = HerdDateUtils.formatDuration(MILLIS_PER_SECOND);
        assertEquals("1 Second", result);
    }

    @Test
    public void testFormatDurationMilliseconds()
    {
        String result = HerdDateUtils.formatDuration(1);
        assertEquals("1 Millisecond", result);

        int length = 999; // Don't go above 1 second.
        result = HerdDateUtils.formatDuration(length);
        assertEquals(length + " Milliseconds", result);
    }

    @Test
    public void testFormatDurationMultipleUnits()
    {
        long millis = 300;
        long seconds = 2;
        long minutes = 59;
        long hours = 23;
        long days = 999;
        long duration = millis + MILLIS_PER_SECOND * seconds + MILLIS_PER_MINUTE * minutes + MILLIS_PER_HOUR * hours + MILLIS_PER_DAY * days;

        String expectedResult = days + " Days, " + hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds, " + millis + " Milliseconds";
        String result = HerdDateUtils.formatDuration(duration);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAddDays()
    {
        Timestamp testTimestamp = Timestamp.valueOf("2015-12-21 10:34:11");
        int daysDiff = 99;

        assertEquals(Timestamp.valueOf("2016-03-29 10:34:11"), HerdDateUtils.addDays(testTimestamp, daysDiff));
        assertEquals(testTimestamp, HerdDateUtils.addDays(testTimestamp, 0));
        assertEquals(Timestamp.valueOf("2015-09-13 10:34:11"), HerdDateUtils.addDays(testTimestamp, -daysDiff));
    }

    @Test
    public void testAddMinutes()
    {
        Timestamp testTimestamp = Timestamp.valueOf("2015-12-21 10:34:11");
        int minutesDiff = 99;

        assertEquals(Timestamp.valueOf("2015-12-21 12:13:11"), HerdDateUtils.addMinutes(testTimestamp, minutesDiff));
        assertEquals(testTimestamp, HerdDateUtils.addMinutes(testTimestamp, 0));
        assertEquals(Timestamp.valueOf("2015-12-21 08:55:11"), HerdDateUtils.addMinutes(testTimestamp, -minutesDiff));
    }

    @Test
    public void testResetTimeToMidnight() throws DatatypeConfigurationException
    {
        XMLGregorianCalendar gregorianCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar("2015-12-21T21:32:52");
        Timestamp result = HerdDateUtils.resetTimeToMidnight(gregorianCalendar);
        assertEquals(Timestamp.valueOf("2015-12-21 00:00:00.0"), result);
    }
}
