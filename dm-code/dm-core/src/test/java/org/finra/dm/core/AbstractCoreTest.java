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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

import org.finra.dm.core.config.CoreTestSpringModuleConfig;
import org.finra.dm.core.helper.ConfigurationHelper;

/**
 * Base class for all core and extending tests. We need to use a customized "loader" that stores the application context in an application context holder so
 * static bean creation methods don't fail. This is similar to what WarInitializer.java does for the WAR, but rather for the JUnits.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CoreTestSpringModuleConfig.class, loader = ContextHolderContextLoader.class)
@TransactionConfiguration
public abstract class AbstractCoreTest
{
    protected static final String RANDOM_SUFFIX = getRandomSuffix();
    protected static final String RANDOM_SUFFIX_2 = getRandomSuffix();

    public static final long FILE_SIZE_0_BYTE = 0L;
    public static final long FILE_SIZE_1_KB = 1024L;
    public static final long ROW_COUNT_1000 = 1000L;

    public static final String INVALID_BOOLEAN_VALUE = "INVALID_BOOLEAN_VALUE";

    @Autowired
    protected ApplicationContext appContext;

    @Autowired
    protected ResourceLoader resourceLoader;

    // The Spring environment.
    @Autowired
    protected Environment environment;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    /**
     * Returns a random suffix.
     */
    protected static String getRandomSuffix()
    {
        return String.format("%.5f", Math.random()).substring(2, 7);
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
     * @param loggingClass the logging class to turn off. If null is specified, the command will be executed with no logging changes.
     * @param command the command to execute.
     *
     * @throws Exception if any errors were encountered.
     */
    protected void executeWithoutLogging(Class<?> loggingClass, Command command) throws Exception
    {
        // Temporarily turn off logging.
        Logger logger = loggingClass == null ? null : Logger.getLogger(loggingClass.getName());
        Level originalLevel = logger == null ? null : logger.getEffectiveLevel();

        if (logger != null)
        {
            logger.setLevel(Level.OFF);
        }

        try
        {
            // Execute the command.
            command.execute();
        }
        finally
        {
            if (logger != null)
            {
                // Turn the original logging back on.
                logger.setLevel(originalLevel);
            }
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
}
