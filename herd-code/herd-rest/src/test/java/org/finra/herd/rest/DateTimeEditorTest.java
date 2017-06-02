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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

/*
 * Test the DateTimeEditor class.
 */
public class DateTimeEditorTest extends AbstractRestTest
{
    @InjectMocks
    DateTimeEditor dateTimeEditor;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSetAsText()
    {
        String inputDateTimeAsText = "2015-07-18T13:32:56.971-04:00";
        DateTime expectedDateTime = new DateTime(2015, 7, 18, 13, 32, 56, 971, DateTimeZone.forOffsetHours(-4));

        dateTimeEditor.setAsText(inputDateTimeAsText);
        assertEquals(Long.valueOf(expectedDateTime.getMillis()), Long.valueOf(((DateTime) dateTimeEditor.getValue()).getMillis()));

        dateTimeEditor.setAsText(addWhitespace(inputDateTimeAsText));
        assertEquals(Long.valueOf(expectedDateTime.getMillis()), Long.valueOf(((DateTime) dateTimeEditor.getValue()).getMillis()));

        dateTimeEditor.setAsText(BLANK_TEXT);
        assertNull(dateTimeEditor.getValue());

        dateTimeEditor.setAsText(null);
        assertNull(dateTimeEditor.getValue());
    }
}
