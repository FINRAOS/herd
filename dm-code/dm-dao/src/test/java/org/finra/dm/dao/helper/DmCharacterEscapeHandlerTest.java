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
package org.finra.dm.dao.helper;

import static org.junit.Assert.assertEquals;

import java.io.StringWriter;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.dao.AbstractDaoTest;

/**
 * This class tests the DM character escape handler. This class ensures all characters are escaped or not escaped properly per the Javadoc of the "escape"
 * method within the handler.
 */
public class DmCharacterEscapeHandlerTest extends AbstractDaoTest
{
    @Autowired
    protected DmCharacterEscapeHandler escapeHandler;

    private static final String TEST_BUFFER = "A<>&\"'\u0001\t";

    @Test
    public void testEscapeNoAttribute() throws Exception
    {
        // "No attribute" doesn't escape double and single quotes.
        StringWriter writer = new StringWriter();
        escapeHandler.escape(TEST_BUFFER.toCharArray(), 0, TEST_BUFFER.length(), false, writer);
        assertEquals("A&lt;&gt;&amp;\"'&#x1;\t", writer.toString());
    }

    @Test
    public void testEscapeAttribute() throws Exception
    {
        // "Attribute" escapes double and single quotes.
        StringWriter writer = new StringWriter();
        escapeHandler.escape(TEST_BUFFER.toCharArray(), 0, TEST_BUFFER.length(), true, writer);
        assertEquals("A&lt;&gt;&amp;&quot;&apos;&#x1;\t", writer.toString());
    }

    @Test
    public void testEscapeSubBuffer() throws Exception
    {
        // Test a sub-part of the buffer (i.e. positions 1 and 2).
        StringWriter writer = new StringWriter();
        escapeHandler.escape(TEST_BUFFER.toCharArray(), 1, 2, true, writer);
        assertEquals("&lt;&gt;", writer.toString());
    }
}
