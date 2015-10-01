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
package org.finra.dm.core;

import static org.finra.dm.core.DmDateUtils.MILLIS_IN_ONE_DAY;
import static org.finra.dm.core.DmDateUtils.MILLIS_IN_ONE_HOUR;
import static org.finra.dm.core.DmDateUtils.MILLIS_IN_ONE_MINUTE;
import static org.finra.dm.core.DmDateUtils.MILLIS_IN_ONE_SECOND;
import static org.junit.Assert.assertEquals;

import java.util.Calendar;

import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Test;

/**
 * Test driver for the DateUtils class.
 */
public class DmDateUtilsTest extends AbstractCoreTest
{
    @Test
    public void testGetXMLGregorianCalendarValue()
    {
        Calendar calendar = Calendar.getInstance();
        XMLGregorianCalendar gregorianCalendar = DmDateUtils.getXMLGregorianCalendarValue(calendar.getTime());
        assertEquals(calendar.getTime(), gregorianCalendar.toGregorianCalendar().getTime());
    }

    @Test
    public void testFormatDurationZeroSeconds()
    {
        String result = DmDateUtils.formatDuration(0, false);
        assertEquals("0 Seconds", result);
    }

    @Test
    public void testFormatDurationZeroMilliseconds()
    {
        String result = DmDateUtils.formatDuration(0, true);
        assertEquals("0 Milliseconds", result);
    }

    @Test
    public void testFormatDurationDays()
    {
        String result = DmDateUtils.formatDuration(MILLIS_IN_ONE_DAY, false);
        assertEquals("1 Day", result);

        int length = 100;
        result = DmDateUtils.formatDuration(MILLIS_IN_ONE_DAY * length, true);
        assertEquals(length + " Days", result);
    }

    @Test
    public void testFormatDurationHours()
    {
        String result = DmDateUtils.formatDuration(MILLIS_IN_ONE_HOUR, true);
        assertEquals("1 Hour", result);

        int length = 10; // Don't go above 1 day.
        result = DmDateUtils.formatDuration(MILLIS_IN_ONE_HOUR * length, false);
        assertEquals(length + " Hours", result);
    }

    @Test
    public void testFormatDurationMinutes()
    {
        String result = DmDateUtils.formatDuration(MILLIS_IN_ONE_MINUTE, true);
        assertEquals("1 Minute", result);

        int length = 59; // Don't go above 1 hour.
        result = DmDateUtils.formatDuration(MILLIS_IN_ONE_MINUTE * length, false);
        assertEquals(length + " Minutes", result);
    }

    @Test
    public void testFormatDurationSeconds()
    {
        String result = DmDateUtils.formatDuration(MILLIS_IN_ONE_SECOND, true);
        assertEquals("1 Second", result);

        int length = 33; // Don't go above 1 minute.
        result = DmDateUtils.formatDuration(MILLIS_IN_ONE_SECOND * length, false);
        assertEquals(length + " Seconds", result);
    }

    @Test
    public void testFormatDurationMilliseconds()
    {
        String result = DmDateUtils.formatDuration(1, true);
        assertEquals("1 Millisecond", result);

        int length = 999; // Don't go above 1 second.
        result = DmDateUtils.formatDuration(length, true);
        assertEquals(length + " Milliseconds", result);

        result = DmDateUtils.formatDuration(length, false);
        assertEquals("0 Seconds", result); // Rounding is not done.
    }

    @Test
    public void testFormatDurationMultipleUnits()
    {
        long millis = 300;
        long seconds = 2;
        long minutes = 59;
        long hours = 23;
        long days = 999;
        long duration = millis + MILLIS_IN_ONE_SECOND * seconds + MILLIS_IN_ONE_MINUTE * minutes + MILLIS_IN_ONE_HOUR * hours + MILLIS_IN_ONE_DAY * days;

        String expectedResult = days + " Days, " + hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds";
        String result = DmDateUtils.formatDuration(duration, false);
        assertEquals(expectedResult, result);
        result = DmDateUtils.formatDuration(duration, true);
        assertEquals(expectedResult + ", " + millis + " Milliseconds", result);
    }
}
