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

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.util.Assert;

/**
 * This class makes it easy to write user-friendly command-line interfaces. It is basically a wrapper around Apache Commons CLI.
 */
public class ArgumentParser
{
    protected String applicationName;
    protected Options options; // All parameters where required parameters are truly marked as "required".
    protected Options optionsIgnoreRequired; // All parameters where each is marked as optional - including required ones.
    protected CommandLine commandLine;

    /**
     * @param applicationName The application name.
     */
    public ArgumentParser(String applicationName)
    {
        this.applicationName = applicationName;
        options = new Options();
        optionsIgnoreRequired = new Options();
    }

    /**
     * Returns the application name that this instance of ArgumentParser was created for.
     *
     * @return the application name.
     */
    public String getApplicationName()
    {
        return applicationName;
    }

    /**
     * Adds an option instance. It may be specified as being mandatory.
     *
     * @param option the option that is to be added
     * @param required specifies whether the option being added is mandatory
     *
     * @return the option that was added
     */
    public Option addArgument(Option option, boolean required)
    {
        optionsIgnoreRequired.addOption((Option) option.clone());

        option.setRequired(required);
        options.addOption(option);

        return option;
    }

    /**
     * Creates and adds an Option using the specified parameters.  The parameters contain a short-name and a long-name. It may be specified as requiring an
     * argument and/or being mandatory.
     *
     * @param opt Short single-character name of the option.
     * @param longOpt Long multi-character name of the option.
     * @param hasArg flag signally if an argument is required after this option
     * @param description Self-documenting description
     * @param required specifies whether the option being added is mandatory
     *
     * @return the resulting option that was added
     */
    public Option addArgument(String opt, String longOpt, boolean hasArg, String description, boolean required)
    {
        Option option = new Option(opt, longOpt, hasArg, description);

        return addArgument(option, required);
    }

    /**
     * Retrieve a read-only list of options in this set
     *
     * @return read-only Collection of {@link Option} objects in this descriptor
     */
    public Collection getConfiguredOptions()
    {
        return options.getOptions();
    }

    /**
     * Parses the arguments according to the specified options and properties.
     *
     * @param args the command line arguments passed to the program
     * @param failOnMissingRequiredOptions specifies whether to fail on any missing mandatory options
     */
    public void parseArguments(String[] args, boolean failOnMissingRequiredOptions) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        commandLine = parser.parse(failOnMissingRequiredOptions ? options : optionsIgnoreRequired, args);
    }

    /**
     * Parses the arguments according to the specified options. The parsing fails when any of the required arguments are missing.
     *
     * @param args the command line arguments passed to the program
     */
    public void parseArguments(String[] args) throws ParseException
    {
        parseArguments(args, true);
    }

    /**
     * Returns a help message, including the program usage and information about the arguments registered with the ArgumentParser.
     *
     * @return the usage information about the arguments registered with the ArgumentParser.
     */
    public String getUsageInformation()
    {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        formatter.printHelp(pw, formatter.getWidth(), applicationName, null, options, formatter.getLeftPadding(), formatter.getDescPadding(), null, false);
        pw.flush();

        return sw.toString();
    }

    /**
     * Query to see if an option has been set.
     *
     * @param option the option that we want to query for
     *
     * @return true if set, false if not
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "This is a false positive. A null check is present.")
    public Boolean getBooleanValue(Option option) throws IllegalStateException
    {
        ensureCommandLineNotNull();
        return commandLine.hasOption(option.getOpt());
    }

    /**
     * Retrieves the argument value, if any, as a String object and converts it to a boolean value.
     *
     * @param option the option that we want to query for
     * @param defaultValue the default value to return if option is not set or missing an argument value
     *
     * @return the value of the argument converted to a boolean value or default value when the option is not set or missing an argument value
     * @throws ParseException if the value of the argument is an invalid boolean value
     *
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "This is a false positive. A null check is present.")
    public Boolean getStringValueAsBoolean(Option option, Boolean defaultValue) throws ParseException
    {
        Boolean result;

        ensureCommandLineNotNull();
        String stringValue = getStringValue(option);

        if (StringUtils.isNotBlank(stringValue))
        {
            // Use custom boolean editor without allowed empty strings to convert the value of the argument to a boolean value.
            CustomBooleanEditor customBooleanEditor = new CustomBooleanEditor(false);
            try
            {
                customBooleanEditor.setAsText(stringValue);
            }
            catch (IllegalArgumentException e)
            {
                ParseException parseException = new ParseException(e.getMessage());
                parseException.initCause(e);
                throw parseException;
            }
            result = (Boolean) customBooleanEditor.getValue();
        }
        else
        {
            result = defaultValue;
        }

        return result;
    }

    /**
     * Retrieves the argument as a String object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "This is a false positive. A null check is present.")
    public String getStringValue(Option option) throws IllegalStateException
    {
        ensureCommandLineNotNull();
        return commandLine.getOptionValue(option.getOpt());
    }

    /**
     * Ensures that the command line is not null. If it is null, an exception will be thrown.
     *
     * @throws IllegalStateException if the command line is null.
     */
    private void ensureCommandLineNotNull() throws IllegalStateException
    {
        Assert.notNull(commandLine, "The command line hasn't been initialized.");
    }

    /**
     * Retrieves the argument as a String object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     * @param defaultValue is the default value to be returned if the option is not specified
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     */
    public String getStringValue(Option option, String defaultValue)
    {
        String answer = getStringValue(option);

        return (answer != null) ? answer : defaultValue;
    }

    /**
     * Retrieves the argument as an Integer object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     * @throws NumberFormatException if there are problems parsing the option value into the Integer type
     */
    public Integer getIntegerValue(Option option) throws NumberFormatException
    {
        String value = getStringValue(option);

        return (value != null) ? Integer.parseInt(value) : null;
    }

    /**
     * Retrieves the argument as an Integer object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     * @param defaultValue is the default value to be returned if the option is not specified
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     * @throws NumberFormatException if there are problems parsing the option value into the Integer type
     */
    public Integer getIntegerValue(Option option, Integer defaultValue) throws NumberFormatException
    {
        Integer answer = getIntegerValue(option);

        return (answer != null) ? answer : defaultValue;
    }

    /**
     * Retrieves the argument as an Integer object, if any, of an option and validates it against minimum and maximum allowed values.
     *
     * @param option the option that we want argument value to be returned for
     * @param defaultValue is the default value to be returned if the option is not specified
     * @param minValue the minimum allowed Integer value for the option
     * @param maxValue the maximum allowed Integer value for the option
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     * @throws IllegalArgumentException if there are problems with the option value
     */
    public Integer getIntegerValue(Option option, Integer defaultValue, Integer minValue, Integer maxValue) throws IllegalArgumentException
    {
        Integer answer = getIntegerValue(option, defaultValue);

        if (answer != null)
        {
            Assert.isTrue(answer.compareTo(minValue) >= 0,
                String.format("The %s option value %d is less than the minimum allowed value of %d.", option.getLongOpt(), answer, minValue));
            Assert.isTrue(answer.compareTo(maxValue) <= 0,
                String.format("The %s option value %d is bigger than maximum allowed value of %d.", option.getLongOpt(), answer, maxValue));
        }

        return answer;
    }

    /**
     * Retrieves the argument as a File object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     */
    public File getFileValue(Option option)
    {
        String value = getStringValue(option);

        return (value != null) ? new File(value) : null;
    }

    /**
     * Retrieves the argument as a File object, if any, of an option.
     *
     * @param option the option that we want argument value to be returned for
     * @param defaultValue is the default value to be returned if the option is not specified
     *
     * @return Value of the argument if option is set, and has an argument, otherwise defaultValue.
     */
    public File getFileValue(Option option, File defaultValue)
    {
        File answer = getFileValue(option);

        return (answer != null) ? answer : defaultValue;
    }
}
