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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.dao.AbstractDaoTest;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the DmDaoHelper class.
 */
public class DmStringHelperTest extends AbstractDaoTest
{
    @Autowired
    private DmStringHelper dmStringHelper;

    @Test
    public void testGetRequiredConfigurationValue() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), STRING_VALUE);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            String resultValue = dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);
            assertEquals(STRING_VALUE, resultValue);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testGetRequiredConfigurationValueBlank() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), BLANK_TEXT);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            dmStringHelper.getRequiredConfigurationValue(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);
            fail("Suppose to throw an IllegalStateException when encrypted configuration value is blank.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(
                String.format("Missing configuration parameter value for key \"%s\".", ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey()),
                e.getMessage());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testSplitStringWithDefaultDelimiterEscapedNull()
    {
        List<String> splitString = dmStringHelper.splitStringWithDefaultDelimiterEscaped(null);

        assertTrue(splitString.size() == 0);
    }

    @Test
    public void testSplitStringWithDefaultDelimiterEscaped()
    {
        String value1 = "arg1";
        String value2 = "arg2\\|escaped";
        String value2Escaped = "arg2|escaped";

        String delimiter = configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);

        String delimitedString = value1 + delimiter + value2;

        List<String> splitString = dmStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedString);

        assertTrue(splitString.get(0).equals(value1));
        assertTrue(splitString.get(1).equals(value2Escaped));
    }

    @Test
    public void testSplitAndTrim()
    {
        Set<String> expectedResults = new HashSet<>(Arrays.asList("a", "b", "c"));
        Set<String> actualResults = dmStringHelper.splitAndTrim(", \n\t\ra \n\t\r,,b,c,", ",");
        assertEquals("split and trim result", expectedResults, actualResults);
    }

    /**
     * Happy path where 3 values are given without needing to escape.
     */
    @Test
    public void testJoin()
    {
        List<String> list = Arrays.asList("A", "B", "C");
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "A|B|C", result);
    }

    /**
     * Elements to join contain the delimiter, which should be escaped.
     */
    @Test
    public void testJoinEscape()
    {
        List<String> list = Arrays.asList("A|", "B|", "C|");
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "A\\||B\\||C\\|", result);
    }

    /**
     * Elements contain whitespace only elements, which should be ignored.
     */
    @Test
    public void testJoinIgnoreBlanks()
    {
        List<String> list = Arrays.asList(" \t\n\r", "A", " \t\n\r", "B", " \t\n\r");
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "A|B", result);
    }

    /**
     * Elements contain null elements, which should be ignored.
     */
    @Test
    public void testJoinIgnoreNulls()
    {
        List<String> list = Arrays.asList(null, "A", null, "B", null);
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "A|B", result);
    }

    /**
     * Only 1 element in the list
     */
    @Test
    public void testJoinSingleElement()
    {
        List<String> list = Arrays.asList("A");
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "A", result);
    }

    /**
     * Only blank and null elements
     */
    @Test
    public void testJoinBlanksOnly()
    {
        List<String> list = Arrays.asList(" \t\n\r", null, " \t\n\r", null);
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertEquals("result", "", result);
    }

    /**
     * List is null
     */
    @Test
    public void testJoinListNull()
    {
        List<String> list = null;
        String delimiter = "|";
        String escapeSequence = "\\";
        String result = dmStringHelper.join(list, delimiter, escapeSequence);

        assertNull("result", result);
    }
}
