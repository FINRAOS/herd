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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;

import org.finra.herd.core.config.CoreTestSpringModuleConfig;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.core.helper.LoggingHelper;

/**
 * Base class for all core and extending tests. We need to use a customized "loader" that stores the application context in an application context holder so
 * static bean creation methods don't fail. This is similar to what WarInitializer.java does for the WAR, but rather for the JUnits.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CoreTestSpringModuleConfig.class, loader = ContextHolderContextLoader.class)
@TransactionConfiguration
public abstract class AbstractCoreTest
{
    public static final String BLANK_TEXT = "      \t\t ";

    public static final long FILE_SIZE_0_BYTE = 0L;

    public static final long FILE_SIZE_1_KB = 1024L;

    public static final long FILE_SIZE_2_KB = 2048L;

    public static final String INVALID_BOOLEAN_VALUE = "INVALID_BOOLEAN_VALUE";

    public static final String INVALID_INTEGER_VALUE = "INVALID_INTEGER_VALUE";

    public static final String RANDOM_SUFFIX = getRandomSuffix();

    public static final String RANDOM_SUFFIX_2 = getRandomSuffix();

    public static final long ROW_COUNT_1000 = 1000L;

    protected static Path localTempPath;

    @Autowired
    protected ApplicationContext appContext;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected LoggingHelper loggingHelper;

    // The Spring environment.
    @Autowired
    protected Environment environment;

    @Autowired
    protected ResourceLoader resourceLoader;

    /**
     * Returns a random suffix.
     */
    public static String getRandomSuffix()
    {
        return String.format("%.5f", Math.random()).substring(2, 7);
    }

    @Before
    public void setup() throws Exception
    {
        // Remove the system environment property source so system environment variables don't affect unit tests.
        getMutablePropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    }

    /**
     * Same as {@link Assert#assertEquals(String, Object, Object)} but null and empty strings are considered equal. This method simply does not call assertion
     * if both expected and actual are null or empty.
     *
     * @param message - The message to display on assertion error
     * @param expected - The expected value
     * @param actual - The actual value
     *
     * @see StringUtils#isEmpty(CharSequence)
     */
    protected void assertEqualsIgnoreNullOrEmpty(String message, String expected, String actual)
    {
        if (!StringUtils.isEmpty(expected) || !StringUtils.isEmpty(actual))
        {
            assertEquals(message, expected, actual);
        }
    }

    /**
     * Asserts that the given expected collection is equal to the actual collection ignoring the order of the elements. Two collections are equal as defined by
     * {@link Set#equals(Object)}. This assertion is {@code null} safe.
     *
     * @param message - message as defined by {@link Assert#assertEquals(String, Object, Object)}
     * @param expected - expected collection
     * @param actual - actual collection
     */
    protected void assertEqualsIgnoreOrder(String message, Collection<?> expected, Collection<?> actual)
    {
        if (expected != null && actual != null)
        {
            Set<?> expectedSet = new HashSet<Object>(expected);
            Set<?> actualSet = new HashSet<Object>(actual);
            assertEquals(message, expectedSet, actualSet);
        }
        else if (expected != actual)
        {
            assertEquals(message, expected, actual);
        }
    }

    /**
     * Asserts that the given value is strictly greater than the greaterThan value. Throws an AssertionError if the assertion fails.
     *
     * @param message Optional message added to the exception message
     * @param value The value that must not be greater than "greaterThan"
     * @param greaterThan The value which "value" must not be greater than
     */
    protected void assertGreaterThan(String message, long value, long greaterThan)
    {
        if (!(value > greaterThan))
        {
            String detailMessage = String.format("Expected greater than %s. Actual %s", greaterThan, value);
            if (StringUtils.isNotBlank(message))
            {
                detailMessage = message + ". " + detailMessage;
            }
            throw new AssertionError(detailMessage);
        }
    }

    /**
     * Creates a file of the specified size relative to the base directory.
     *
     * @param baseDirectory the local parent directory path, relative to which we want our file to be created
     * @param file the file path (including file name) relative to the base directory for the file to be created
     * @param size the file size in bytes
     *
     * @return the created file
     */
    protected File createLocalFile(String baseDirectory, String file, long size) throws IOException
    {
        Path filePath = Paths.get(baseDirectory, file);
        // We don't check the "mkdirs" response because the directory may already exist which would return false.
        // But we want to create sub-directories if they don't yet exist which is why we're calling "mkdirs" in the first place.
        // If an actual directory couldn't be created, then the new file below will throw an exception anyway.
        filePath.toFile().getParentFile().mkdirs();
        RandomAccessFile randomAccessFile = new RandomAccessFile(filePath.toString(), "rw");
        randomAccessFile.setLength(size);
        randomAccessFile.close();
        return filePath.toFile();
    }

    /**
     * Gets the mutable property sources object from the environment.
     *
     * @return the mutable property sources.
     * @throws Exception if the mutable property sources couldn't be obtained.
     */
    protected MutablePropertySources getMutablePropertySources() throws Exception
    {
        // Ensure we have a configurable environment so we can remove the property source.
        if (!(environment instanceof ConfigurableEnvironment))
        {
            throw new Exception("The environment is not an instance of ConfigurableEnvironment and needs to be for this test to work.");
        }

        // Return the property sources from the configurable environment.
        ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;
        return configurableEnvironment.getPropertySources();
    }

    /**
     * Executes a command without logging. The logging will be temporarily turned off during the execution of the command and then restored once the command has
     * finished executing.
     *
     * @param loggingClass the logging class to turn off. If null is specified, the command will be executed with no logging changes
     * @param command the command to execute
     *
     * @throws Exception if any errors were encountered.
     */
    protected void executeWithoutLogging(Class<?> loggingClass, Command command) throws Exception
    {
        loggingHelper.executeWithoutLogging(loggingClass, command);
    }

    /**
     * Executes a command without logging. The logging will be temporarily turned off during the execution of the command and then restored once the command has
     * finished executing.
     *
     * @param loggingClasses the list of logging classes to turn off
     * @param command the command to execute
     *
     * @throws Exception if any errors were encountered
     */
    protected void executeWithoutLogging(List<Class<?>> loggingClasses, Command command) throws Exception
    {
        loggingHelper.executeWithoutLogging(loggingClasses, command);
    }

    /**
     * Adds a test appender.
     *
     * @param appenderName the appender name to add.
     *
     * @return the string writer associated with the writer appender.
     */
    protected StringWriter addLoggingWriterAppender(String appenderName)
    {
        return loggingHelper.addLoggingWriterAppender(appenderName);
    }

    /**
     * Removes a logging appender.
     *
     * @param appenderName the appender name.
     */
    protected void removeLoggingAppender(String appenderName)
    {
        loggingHelper.removeLoggingAppender(appenderName);
    }

    /**
     * Gets the log level for the specified logger.
     *
     * @param clazz the class for the logger.
     */
    protected LogLevel getLogLevel(Class clazz)
    {
        return loggingHelper.getLogLevel(clazz);
    }

    /**
     * Gets the log level for the specified logger.
     *
     * @param loggerName the logger name to get the level for.
     */
    protected LogLevel getLogLevel(String loggerName)
    {
        return loggingHelper.getLogLevel(loggerName);
    }

    /**
     * Sets the log level.
     *
     * @param clazz the class for the logger.
     * @param logLevel the log level to set.
     */
    protected void setLogLevel(Class clazz, LogLevel logLevel)
    {
        loggingHelper.setLogLevel(clazz, logLevel);
    }

    /**
     * Sets the log level.
     *
     * @param loggerName the logger name (e.g. Myclass.class.getName()).
     * @param logLevel the log level to set.
     */
    protected void setLogLevel(String loggerName, LogLevel logLevel)
    {
        loggingHelper.setLogLevel(loggerName, logLevel);
    }
}
