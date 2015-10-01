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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.junit.Test;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;

/**
 * Unit tests for ArgumentParser class.
 */
public class ArgumentParserTest extends AbstractCoreTest
{
    @Test
    public void testGetApplicationName()
    {
        final String testApplicationName = "MY_TEST_APP";
        ArgumentParser argParser = new ArgumentParser(testApplicationName);
        assertEquals(testApplicationName, argParser.getApplicationName());
    }

    @Test
    public void testAddArgument()
    {
        Option opt1 = new Option("a", "some_flag", false, "Some flag parameter");
        Option opt2 = new Option("b", "some_parameter", true, "Some parameter with an argument");

        List<String> optionsIn = Arrays.asList(optionToString(opt1), optionToString(opt2));

        ArgumentParser argParser = new ArgumentParser("");

        argParser.addArgument(opt1, true);
        argParser.addArgument(opt2.getOpt(), opt2.getLongOpt(), opt2.hasArg(), opt2.getDescription(), false);

        Collection resultOptions = argParser.getConfiguredOptions();

        List<String> optionsOut = new ArrayList<>();

        for (Object obj : resultOptions)
        {
            assertTrue(obj instanceof Option);
            optionsOut.add(optionToString((Option) obj));
        }

        optionsOut.containsAll(optionsIn);
        optionsIn.containsAll(optionsOut);
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void testParseArgumentsUnrecognizedOptionException() throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("");
        argParser.addArgument("a", "configured_option", false, "Some flag parameter", false);
        argParser.parseArguments(new String[] {"-b", "unrecognized_option"});
    }

    @Test(expected = MissingArgumentException.class)
    public void testParseArgumentsMissingArgumentException() throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("");
        argParser.addArgument("a", "some_optional_parameter", true, "Some optional parameter with an argument", false);
        argParser.parseArguments(new String[] {"-a"});
    }

    @Test
    public void testParseArgumentsFailOnMissingArgumentFalse() throws ParseException
    {
        // Create an argument parser with a required "a" option.
        ArgumentParser argParser = new ArgumentParser("");
        argParser.addArgument("a", "some_optional_parameter", false, "Some argument", true);

        // Parse the arguments with a missing argument, but this should not thrown an exception since we are passing the flag which WILL NOT fail on missing
        // arguments.
        argParser.parseArguments(new String[] {}, false);
    }

    @Test(expected = MissingOptionException.class)
    public void testParseArgumentsFailOnMissingArgumentTrue() throws ParseException
    {
        // Create an argument parser with a required "a" option.
        ArgumentParser argParser = new ArgumentParser("");
        argParser.addArgument("a", "some_optional_parameter", false, "Some argument", true);

        // Parse the arguments with a missing argument which should thrown an exception since we are passing the flag which WILL fail on missing arguments.
        argParser.parseArguments(new String[] {}, true);
    }

    @Test(expected = MissingOptionException.class)
    public void testParseArguments() throws ParseException
    {
        // Create an argument parser with a required "a" option.
        ArgumentParser argParser = new ArgumentParser("");
        argParser.addArgument("a", "some_required_parameter", true, "Some argument", true);

        // Parse the arguments with a missing argument which should thrown an exception since we are using the method that WILL fail on missing arguments.
        argParser.parseArguments(new String[] {});
    }

    @Test
    public void testGetUsageInformation()
    {
        List<Option> optionsIn = new ArrayList<>();
        optionsIn.add(new Option("a", "some_flag", false, "Some flag parameter"));
        optionsIn.add(new Option("b", "some_parameter", true, "Some parameter with an argument"));

        ArgumentParser argParser = new ArgumentParser("TestApp");
        argParser.addArgument(optionsIn.get(0), false);
        argParser.addArgument(optionsIn.get(1), true);

        String usage = argParser.getUsageInformation();

        assertNotNull(usage);
        assertTrue(usage.contains(String.format("usage: %s", argParser.getApplicationName())));

        for (Option option : optionsIn)
        {
            assertTrue(usage.contains(String.format("-%s,", option.getOpt())));
            assertTrue(usage.contains(String.format("--%s", option.getLongOpt())));
            assertTrue(usage.contains(option.getDescription()));
            assertTrue(!option.hasArg() || usage.contains(String.format("<%s>", option.getArgName())));
        }
    }

    @Test
    public void testGetBooleanValue() throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("");
        Option boolOpt = argParser.addArgument("b", "bool", false, "Some optional configuration flag", false);
        Boolean resultValue;

        final String shortBoolOpt = String.format("-%s", boolOpt.getOpt());
        final String longBoolOpt = String.format("--%s", boolOpt.getLongOpt());

        argParser.parseArguments(new String[] {});
        resultValue = argParser.getBooleanValue(boolOpt);
        assertNotNull(resultValue);
        assertFalse(resultValue);

        argParser.parseArguments(new String[] {shortBoolOpt});
        resultValue = argParser.getBooleanValue(boolOpt);
        assertNotNull(resultValue);
        assertTrue(resultValue);

        argParser.parseArguments(new String[] {longBoolOpt});
        resultValue = argParser.getBooleanValue(boolOpt);
        assertNotNull(resultValue);
        assertTrue(resultValue);
    }

    @Test
    public void testGetStringValueAsBoolean() throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("");
        Option strOpt = argParser.addArgument("s", "str", true, "Some string input parameter to have a boolean value", false);

        final String shortStrOpt = String.format("-%s", strOpt.getOpt());
        final String longStrOpt = String.format("--%s", strOpt.getLongOpt());

        // Validate the default value - no option is specified.
        argParser.parseArguments(new String[] {});
        assertFalse(argParser.getStringValueAsBoolean(strOpt, false));
        assertTrue(argParser.getStringValueAsBoolean(strOpt, true));

        // Validate all "true" boolean values using both short an long options.
        for (String inputValue : Arrays
            .asList(CustomBooleanEditor.VALUE_TRUE, CustomBooleanEditor.VALUE_YES, CustomBooleanEditor.VALUE_ON, CustomBooleanEditor.VALUE_1))
        {
            argParser.parseArguments(new String[] {shortStrOpt, inputValue});
            assertTrue(argParser.getStringValueAsBoolean(strOpt, false));
            argParser.parseArguments(new String[] {longStrOpt, inputValue});
            assertTrue(argParser.getStringValueAsBoolean(strOpt, false));
        }

        // Validate all "false" boolean values.
        for (String inputValue : Arrays
            .asList(CustomBooleanEditor.VALUE_FALSE, CustomBooleanEditor.VALUE_NO, CustomBooleanEditor.VALUE_OFF, CustomBooleanEditor.VALUE_0))
        {
            argParser.parseArguments(new String[] {shortStrOpt, inputValue});
            assertFalse(argParser.getStringValueAsBoolean(strOpt, true));
        }

        // Try to parse an invalid boolean value.
        argParser.parseArguments(new String[] {shortStrOpt, INVALID_BOOLEAN_VALUE});
        try
        {
            argParser.getStringValueAsBoolean(strOpt, false);
            fail("Suppose to throw a ParseException when option has an invalid boolean value.");
        }
        catch (ParseException e)
        {
            assertEquals(String.format("Invalid boolean value [%s]", INVALID_BOOLEAN_VALUE), e.getMessage());
        }
    }

    @Test
    public void testGetStringValue() throws ParseException
    {
        final String testDefaultValue = "default_str_value";

        ArgumentParser argParser = new ArgumentParser("");
        Option strOpt = argParser.addArgument("s", "str", true, "Some string input parameter", false);
        String inputValue;
        String resultValue;

        final String shortStrOpt = String.format("-%s", strOpt.getOpt());
        final String longStrOpt = String.format("--%s", strOpt.getLongOpt());

        argParser.parseArguments(new String[] {});
        assertNull(argParser.getStringValue(strOpt));
        assertEquals(testDefaultValue, argParser.getStringValue(strOpt, testDefaultValue));

        inputValue = "my_string_value_1";
        argParser.parseArguments(new String[] {shortStrOpt, inputValue});
        resultValue = argParser.getStringValue(strOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = "my_string_value_2";
        argParser.parseArguments(new String[] {shortStrOpt, inputValue});
        resultValue = argParser.getStringValue(strOpt, testDefaultValue);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = "my_string_value_3";
        argParser.parseArguments(new String[] {longStrOpt, inputValue});
        resultValue = argParser.getStringValue(strOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);
    }

    @Test
    public void testGetIntegerValue() throws ParseException
    {
        final Integer testDefaultValue = 500;
        final Integer testMinValue = 0;
        final Integer testMaxValue = 1000;

        ArgumentParser argParser = new ArgumentParser("");
        Option intOpt = argParser.addArgument("i", "int", true, "Some integer parameter", false);
        Integer inputValue;
        Integer resultValue;

        final String shortIntOpt = String.format("-%s", intOpt.getOpt());
        final String longIntOpt = String.format("--%s", intOpt.getLongOpt());

        argParser.parseArguments(new String[] {});
        assertNull(argParser.getIntegerValue(intOpt));
        assertEquals(testDefaultValue, argParser.getIntegerValue(intOpt, testDefaultValue));

        inputValue = 123;
        argParser.parseArguments(new String[] {shortIntOpt, inputValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = 456;
        argParser.parseArguments(new String[] {shortIntOpt, inputValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = 789;
        argParser.parseArguments(new String[] {longIntOpt, inputValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        // The "happy path" test case for the minimum and maximum allowed values.
        inputValue = 234;
        argParser.parseArguments(new String[] {longIntOpt, inputValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        // The default value test case the minimum and maximum allowed values.
        argParser.parseArguments(new String[] {});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
        assertNotNull(resultValue);
        assertEquals(testDefaultValue, resultValue);

        // The edge test case for the minimum allowed value.
        argParser.parseArguments(new String[] {longIntOpt, testMinValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
        assertNotNull(resultValue);
        assertEquals(testMinValue, resultValue);

        // The edge test case for the maximum allowed value.
        argParser.parseArguments(new String[] {longIntOpt, testMaxValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
        assertNotNull(resultValue);
        assertEquals(testMaxValue, resultValue);

        // The edge test case for the minimum and maximum allowed values.
        argParser.parseArguments(new String[] {longIntOpt, testDefaultValue.toString()});
        resultValue = argParser.getIntegerValue(intOpt, testDefaultValue, testDefaultValue, testDefaultValue);
        assertNotNull(resultValue);
        assertEquals(testDefaultValue, resultValue);

        // Try to get an option value what is less than the minimum allowed value.
        inputValue = testMinValue - 1;
        argParser.parseArguments(new String[] {longIntOpt, inputValue.toString()});
        try
        {
            argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
            fail("Suppose to throw an IllegalArgumentException when option value is less than the minimum allowed value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The %s option value %d is less than the minimum allowed value of %d.", intOpt.getLongOpt(), inputValue, testMinValue),
                e.getMessage());
        }

        // Try to get an option value what is greater than the maximum allowed value.
        inputValue = testMaxValue + 1;
        argParser.parseArguments(new String[] {longIntOpt, inputValue.toString()});
        try
        {
            argParser.getIntegerValue(intOpt, testDefaultValue, testMinValue, testMaxValue);
            fail("Suppose to throw an IllegalArgumentException when option value is greater than the maximum allowed value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The %s option value %d is bigger than maximum allowed value of %d.", intOpt.getLongOpt(), inputValue, testMaxValue),
                e.getMessage());
        }
    }

    @Test
    public void testGetIntegerNullValue() throws ParseException
    {
        // Create an argument parser with an optional "a" option.
        ArgumentParser argParser = new ArgumentParser("");
        Option option = argParser.addArgument("a", "some_required_parameter", false, "Some required parameter with an argument", false);

        // Parse the arguments and get an integer value which wasn't specified which should return null.
        argParser.parseArguments(new String[] {});
        Integer value = argParser.getIntegerValue(option, null, 0, 100);
        assertNull(value);
    }

    @Test
    public void testGetFileValue() throws ParseException
    {
        File testDefaultValue = new File("default_file_name");

        ArgumentParser argParser = new ArgumentParser("");
        Option fileOpt = argParser.addArgument("f", "file", true, "Source file name", false);
        File inputValue;
        File resultValue;

        final String shortFileOpt = String.format("-%s", fileOpt.getOpt());
        final String longFileOpt = String.format("--%s", fileOpt.getLongOpt());

        argParser.parseArguments(new String[] {""});
        assertNull(argParser.getFileValue(fileOpt));
        assertEquals(testDefaultValue, argParser.getFileValue(fileOpt, testDefaultValue));

        inputValue = new File("folder/file_name_1");
        argParser.parseArguments(new String[] {shortFileOpt, inputValue.toString()});
        resultValue = argParser.getFileValue(fileOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = new File("folder/file_name_2");
        argParser.parseArguments(new String[] {shortFileOpt, inputValue.toString()});
        resultValue = argParser.getFileValue(fileOpt, testDefaultValue);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);

        inputValue = new File("file_name_3");
        argParser.parseArguments(new String[] {longFileOpt, inputValue.toString()});
        resultValue = argParser.getFileValue(fileOpt);
        assertNotNull(resultValue);
        assertEquals(inputValue, resultValue);
    }

    /**
     * Dump state of the Option object instance, suitable for debugging and comparing Options as Strings.
     *
     * @param option the Option object instance to dump the state of
     *
     * @return Stringified form of this Option object instance
     */
    private String optionToString(Option option)
    {
        StringBuilder buf = new StringBuilder();

        buf.append("[ option: ");
        buf.append(option.getOpt());

        if (option.getLongOpt() != null)
        {
            buf.append(" ").append(option.getLongOpt());
        }

        buf.append(" ");

        if (option.hasArg())
        {
            buf.append(" [ARG]");
        }

        buf.append(" :: ").append(option.getDescription());

        if (option.isRequired())
        {
            buf.append(" [REQUIRED]");
        }

        buf.append(" ]");

        return buf.toString();
    }
}
