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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Test;

import org.finra.herd.swaggergen.test.exampleXmlGenerator.BasicCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.DatetimeCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.EdgeCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.MultiTypedListCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.NonParametrizedListCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.WildcardParameterizedListCase;
import org.finra.herd.swaggergen.test.exampleXmlGenerator.XmlTypeInterfaceCase;

public class ExampleXmlGeneratorTest extends AbstractTest
{
    @Test
    public void test_1() throws Exception
    {
        ExampleXmlGenerator exampleXmlGenerator = new ExampleXmlGenerator(LOG, BasicCase.class);
        String exampleXml = exampleXmlGenerator.getExampleXml();
        assertEquals(getResourceAsString("/exampleXml1"), exampleXml);
    }

    @Test
    public void test_2() throws Exception
    {
        ExampleXmlGenerator exampleXmlGenerator = new ExampleXmlGenerator(LOG, DatetimeCase.class);
        String exampleXml = exampleXmlGenerator.getExampleXml();
        Pattern pattern = Pattern.compile(
            "<xmlGregorianCalendar>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}-[0-9]{2}:[0-9]{2}</xmlGregorianCalendar>");
        Matcher matcher = pattern.matcher(exampleXml);
        assertTrue(format("exampleXml does not match pattern. exampleXml: %s, pattern: %s", exampleXml, pattern), matcher.find());
    }

    @Test
    public void test_3() throws Exception
    {
        ExampleXmlGenerator exampleXmlGenerator = new ExampleXmlGenerator(LOG, EdgeCase.class);
        String exampleXml = exampleXmlGenerator.getExampleXml();
        assertEquals(getResourceAsString("/exampleXml2"), exampleXml);
    }

    @Test
    public void test_4() throws Exception
    {
        try
        {
            new ExampleXmlGenerator(LOG, NonParametrizedListCase.class);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" is a collection that is not a ParameterizedType.", e.getMessage());
        }
    }

    @Test
    public void test_5() throws Exception
    {
        try
        {
            new ExampleXmlGenerator(LOG, WildcardParameterizedListCase.class);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" has a parameterized type which is not a class.", e.getMessage());
        }
    }

    @Test
    public void test_6() throws Exception
    {
        ExampleXmlGenerator exampleXmlGenerator = new ExampleXmlGenerator(LOG, XmlTypeInterfaceCase.class);
        String exampleXml = exampleXmlGenerator.getExampleXml();
        assertEquals("", exampleXml);
    }

    @Test
    public void test_7() throws Exception
    {
        try
        {
            new ExampleXmlGenerator(LOG, MultiTypedListCase.class);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" is a collection that has 2 parameterized types and exactly 1 is required.", e.getMessage());
        }
    }
}
