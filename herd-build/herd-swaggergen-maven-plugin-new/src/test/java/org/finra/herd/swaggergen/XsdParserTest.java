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
package org.finra.herd.swaggergen;

import static org.junit.Assert.*;

import org.junit.Test;

public class XsdParserTest extends AbstractTest
{
    @Test
    public void test_1() throws Exception
    {
        XsdParser xsdParser = new XsdParser("xsd1.xsd");
        assertEquals("The object", xsdParser.getAnnotation("object"));
        assertEquals("The scalar 2", xsdParser.getAnnotation("object", "scalar2"));
    }

    @Test
    public void test_2() throws Exception
    {
        try
        {
            new XsdParser("does_not_exist");
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("No XSD found with name \"does_not_exist\"", e.getMessage());
        }
    }

    @Test
    public void test_3() throws Exception
    {
        try
        {
            new XsdParser("yaml1.yaml");
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Error parsing XSD", e.getMessage());
        }
    }

    @Test
    public void test_4() throws Exception
    {
        XsdParser xsdParser = new XsdParser("xsd1.xsd");
        assertEquals(null, xsdParser.getAnnotation("does_not_exist"));
        assertEquals(null, xsdParser.getAnnotation("object", "does_not_exist"));
        assertEquals(null, xsdParser.getAnnotation("does_not_exist", "does_not_exist"));
    }
}
