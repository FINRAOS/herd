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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.ReloadingStrategy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the reloadable properties source class. This JUnit uses a PropertiesConfiguration as opposed to a DatabaseConfiguration like the real application does
 * so we can more easily create test scenarios without using a real database and having the create and delete rows with separate connections (i.e. one
 * connection used by the JUnit data source and one internally created by the DatabaseConfiguration class which could have problems since a single transaction
 * that can be rolled back won't be possible.
 */
public class ReloadablePropertySourceTest extends AbstractDaoTest
{
    private static Logger logger = Logger.getLogger(ReloadablePropertySourceTest.class);

    public static final String TEST_KEY = "testKey";
    public static final String TEST_VALUE_1 = "testValue1";
    public static final String TEST_VALUE_2 = "testValue2";

    // The time to wait in seconds before the configuration will re-read the properties file.
    // We want this to be low so the JUnit doesn't take too long to run, but not too low where the file will be re-read faster than the JUnit can execute.
    // 1 second should be sufficient.
    public static final long REFRESH_INTERVAL_SECS = 1;

    // The properties to test with.
    private Properties properties;

    // The temporary properties file where the properties will be written to and subsequently read by the reloadable properties source via the specified
    // configuration.
    private File propertiesFile;

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Set the logger to debug level so JUnit coverage will hit all debug only logging.
        Logger.getLogger(ReloadablePropertySource.class).setLevel(Level.DEBUG);
        logger.info("This test driver outputs debug level logging for Clover coverage.");

        // Create the base properties used for each test.
        properties = new Properties();
        properties.put(TEST_KEY, TEST_VALUE_1);

        // Write the properties to a temporary file (i.e. our configuration store).
        updatePropertiesFile();
    }

    @After
    public void tearDown() throws Exception
    {
        // Delete the temporary properties file.
        if (propertiesFile != null)
        {
            logger.debug("Deleting file " + propertiesFile.getName());
            if (!propertiesFile.delete())
            {
                logger.warn("Unable to delete temporary file: " + propertiesFile.getName());
            }
        }
    }

    @Test
    public void testGetPropertyNoRefreshIntervalConstructor() throws Exception
    {
        // Get a reloadable property source that loads properties from the configuration every time a property is read.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(0L);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);
    }

    @Test
    public void testGetPropertyRefreshIntervalConstructor() throws Exception
    {
        // Get a reloadable property source that loads properties from the configuration after a configured interval.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(REFRESH_INTERVAL_SECS);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);
    }

    @Test
    public void testGetPropertyValueNotYetRefreshed() throws Exception
    {
        // Get a reloadable property source that loads properties from the configuration after a configured interval.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(REFRESH_INTERVAL_SECS);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);

        // Update the value from value 1 to value 2.
        updatePropertyToValue2();

        // Read the value which should be value 1 still since the refresh interval hasn't passed yet to re-read the properties file.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);
    }

    @Ignore // TODO: Test case fails at random times. Need to figure out why.
    @Test
    public void testGetPropertyValueRefreshed() throws Exception
    {
        // Get a reloadable property source that loads properties from the configuration after a configured interval.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(REFRESH_INTERVAL_SECS);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);

        // Update the value from value 1 to value 2.
        updatePropertyToValue2();

        // Sleep beyond the refresh interval which will cause our next property get to re-read the properties file.
        sleepPastRefreshInterval();

        // Read the key which should return the new value 2.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_2);
    }

    @Ignore // TODO: Test case fails at random times. Need to figure out why.
    @Test
    public void testGetPropertyOverrideRefreshInterval() throws Exception
    {
        // Update the initial properties file to specify the refresh interval override key.
        // This will cause the refresh interval to change when the properties are loaded.
        updatePropertiesFileWithRefreshIntervalOverride(String.valueOf(REFRESH_INTERVAL_SECS));

        // Get a reloadable property source that loads properties from the configuration every time a property is read.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(0L);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);

        // Sleep until the refresh interval has passed which will cause the properties to be loaded again when we get the next property.
        sleepPastRefreshInterval();

        // Re-read the key which should yield the same value since it hasn't changed in the properties file. Reading this again will actually
        // execute through a piece of code that won't try to re-update the refresh interval that exists in the file since it hasn't changed from it's
        // previous value.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);

        // Update the value from value 1 to value 2.
        updatePropertyToValue2();

        // Read the value which should still be the original value since the overridden refresh interval hasn't yet expired.
        // If the refresh interval override wasn't working, the original configured would force a re-read of the properties file on every property
        // retrieval which would have read value 2.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);
    }

    @Test
    public void testGetPropertyOverrideWithInvalidRefreshInterval() throws Exception
    {
        // Update the initial properties file to specify an invalid refresh interval override key.
        updatePropertiesFileWithRefreshIntervalOverride("Invalid Interval");

        // Get a reloadable property source that loads properties from the configuration every time a property is read.
        ReloadablePropertySource reloadablePropertySource = getNewReloadablePropertiesSource(0L);

        // Read the value which should be the same as what we placed in initially.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_1);

        // Update the value from value 1 to value 2.
        updatePropertyToValue2();

        // Read the value which should be the updated value since the configuration currently re-reads the properties file on every property retrieval.
        // If the refresh interval override changed, this might return value 1 still since the properties file might not have been re-retrieved.
        verifyPropertySourceValue(reloadablePropertySource, TEST_VALUE_2);
    }

    /**
     * Updates the properties file with the latest version of the properties member variable.
     *
     * @throws Exception if the properties file couldn't be updated.
     */
    private void updatePropertiesFile() throws Exception
    {
        // Ensure that a properties object exists.
        if (properties == null)
        {
            throw new Exception("Properties can't be written because they don't yet exist.");
        }

        // Create a temporary file that we will use to write the properties to.
        if (propertiesFile == null)
        {
            logger.debug("Creating temporary file.");
            propertiesFile = File.createTempFile(ReloadablePropertySource.class.getSimpleName(), ".properties");
        }

        // Write the properties to the temporary file.
        try (FileOutputStream fileOutputStream = new FileOutputStream(propertiesFile))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Writing properties to " + propertiesFile.getName());
                for (Object key : properties.keySet())
                {
                    logger.debug("Key [" + key + "] = " + properties.get(key));
                }
            }
            properties.store(fileOutputStream, "Properties used by " + ReloadablePropertySource.class.getName());
        }
    }

    /**
     * Gets a new reloadable properties source object based on the properties member variable.
     *
     * @param refreshIntervalSecs the optional refresh interval in seconds. If null, then the default will be used.
     *
     * @return the newly created reloadable properties source.
     * @throws ConfigurationException if the reloadable properties source couldn't be created.
     */
    private ReloadablePropertySource getNewReloadablePropertiesSource(Long refreshIntervalSecs) throws ConfigurationException
    {
        return (refreshIntervalSecs == null ?
            new ReloadablePropertySource(ReloadablePropertySource.class.getName(), cloneProperties(properties), getNewPropertiesConfiguration()) :
            new ReloadablePropertySource(ReloadablePropertySource.class.getName(), cloneProperties(properties), getNewPropertiesConfiguration(),
                refreshIntervalSecs));
    }

    /**
     * Creates a clone of the specified properties object.
     *
     * @param properties the source properties.
     *
     * @return the cloned properties.
     */
    private Properties cloneProperties(Properties properties)
    {
        Properties clonedProperties = new Properties();
        for (Enumeration propertyNames = properties.propertyNames(); propertyNames.hasMoreElements(); )
        {
            Object key = propertyNames.nextElement();
            clonedProperties.put(key, properties.get(key));
        }
        return clonedProperties;
    }

    /**
     * Gets a new properties configuration that will re-load the properties from a file every time it is called.
     *
     * @return the properties configuration.
     * @throws ConfigurationException if the properties configuration couldn't be created.
     */
    private PropertiesConfiguration getNewPropertiesConfiguration() throws ConfigurationException
    {
        // Create a new properties configuration.
        // We are using this instead of a database configuration for easier testing.
        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(propertiesFile);

        // Create a reloading strategy that will always reload when asked.
        // There were some problems using the FileChangedReloadingStrategy where it wasn't detecting changed files and causing some methods in this
        // JUnit to fail.
        propertiesConfiguration.setReloadingStrategy(new ReloadingStrategy()
        {
            @Override
            public void setConfiguration(FileConfiguration configuration)
            {
            }

            @Override
            public void init()
            {
            }

            @Override
            public boolean reloadingRequired()
            {
                // Tell the caller that the properties should always be reloaded.
                return true;
            }

            @Override
            public void reloadingPerformed()
            {
            }
        });

        return propertiesConfiguration;
    }

    /**
     * Updates the key in the persistent property store to "value 2".
     *
     * @throws Exception if the property store couldn't be updated.
     */
    private void updatePropertyToValue2() throws Exception
    {
        properties.put(TEST_KEY, TEST_VALUE_2);
        updatePropertiesFile();
    }

    /**
     * Reads the test key from the reloadable property source and verifies that it is set to the specified expected value.
     *
     * @param reloadablePropertySource the reloadable property source.
     * @param expectedValue the expected value.
     *
     * @throws IllegalArgumentException if the value isn't the same as the expected value.
     */
    private void verifyPropertySourceValue(ReloadablePropertySource reloadablePropertySource, String expectedValue)
    {
        logger.debug("Reading key " + TEST_KEY + " and expecting value " + expectedValue);
        logger.debug("Properties file value is " + properties.get(TEST_KEY) + " and reloadable property source value is " +
            reloadablePropertySource.getProperty(TEST_KEY));
        String value = (String) reloadablePropertySource.getProperty(TEST_KEY);
        assertEquals(expectedValue, value);
    }

    /**
     * Sleeps for a duration that is equal to the refresh interval which will ensure the refresh interval has passed.
     *
     * @throws Exception if we couldn't sleep.
     */
    private void sleepPastRefreshInterval() throws Exception
    {
        logger.debug("Sleeping for " + REFRESH_INTERVAL_SECS + " second(s).");
        Thread.sleep(REFRESH_INTERVAL_SECS * 1000);
    }

    /**
     * Updates the persistent properties file with a specified refresh interval override value.
     *
     * @param refreshIntervalSecs the refresh interval.
     *
     * @throws Exception if the properties file couldn't be updated.
     */
    private void updatePropertiesFileWithRefreshIntervalOverride(String refreshIntervalSecs) throws Exception
    {
        // Update the initial properties file to specify an invalid refresh interval override key.
        properties.put(ReloadablePropertySource.REFRESH_INTERVAL_SECS_OVERRIDE_KEY, refreshIntervalSecs);
        updatePropertiesFile();
    }
}
