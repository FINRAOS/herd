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
package org.finra.dm.dao;

import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Log4jConfigurer;

import org.finra.dm.dao.config.DaoEnvTestSpringModuleConfig;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.ConfigurationEntity;

/**
 * Tests the Log4jOverridableConfigurer class. Note that the tests that setup Log4J configurations in the database are using low level JDBC to ensure data is
 * committed to the database before the test runs. Otherwise, the data wouldn't be visible to the test driver since Log4jOverridableConfigurer uses it's own
 * data source and connection which only sees previously committed data.
 */
public class Log4jOverridableConfigurerTest extends AbstractDaoTest
{
    private static final String LOG4J_CONFIG_FILENAME = "classpath:log4jOverridableConfigurer-log4j.xml";
    private static final String LOG4J_CONFIG_NO_CLOB_FILENAME = "classpath:log4jOverridableConfigurerNoClob-log4j.xml";

    private static final String LOG4J_FILENAME_TOKEN = "~log4jFileLocation~";

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void testLog4JDbOverrideConfigurationClob() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        String configKey = null;
        try
        {
            // Create a random configuration key to use when inserting the Log4J configuration into the database.
            configKey = ConfigurationValue.LOG4J_OVERRIDE_CONFIGURATION.getKey() + UUID.randomUUID().toString().substring(0, 5);

            // Insert the Log4J configuration into the database using the CLOB column.
            insertDbLog4JConfigurationFromResourceLocation(LOG4J_CONFIG_FILENAME, outputPath, ConfigurationEntity.COLUMN_VALUE_CLOB, configKey);

            // Setup the Log4J overridable configurer to use the database location - using the standard CLOB column.
            Log4jOverridableConfigurer log4jConfigurer = getLog4jOverridableConfigurerForDb(configKey);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // The database override location does exist and should create a log file.
            assertTrue("Log4J output file doesn't exist, but should.", Files.exists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
            deleteDbLog4JConfiguration(configKey);
        }
    }

    @Test
    public void testLog4JDbOverrideConfigurationNoClob() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        String configKey = null;
        try
        {
            // Create a random configuration key to use when inserting the Log4J configuration into the database.
            configKey = ConfigurationValue.LOG4J_OVERRIDE_CONFIGURATION.getKey() + UUID.randomUUID().toString().substring(0, 5);

            // Insert the Log4J configuration into the database using the non-CLOB column.
            insertDbLog4JConfigurationFromResourceLocation(LOG4J_CONFIG_NO_CLOB_FILENAME, outputPath, ConfigurationEntity.COLUMN_VALUE, configKey);

            // Setup the Log4J overridable configurer to use the database location, but override the select column to use the non-CLOB column.
            Log4jOverridableConfigurer log4jConfigurer = getLog4jOverridableConfigurerForDb(configKey);
            log4jConfigurer.setSelectColumn(ConfigurationEntity.COLUMN_VALUE);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // The database override location does exist and should create a log file.
            assertTrue("Log4J output file doesn't exist, but should.", Files.exists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
            deleteDbLog4JConfiguration(configKey);
        }
    }

    @Test
    public void testLog4JDbWithRefreshInterval() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        String configKey = null;
        try
        {
            // Create a random configuration key to use when inserting the Log4J configuration into the database.
            configKey = ConfigurationValue.LOG4J_OVERRIDE_CONFIGURATION.getKey() + UUID.randomUUID().toString().substring(0, 5);

            // Insert the standard JUnit Log4J configuration that won't create an output file into the database using the CLOB column.
            insertDbLog4JConfigurationFromResourceLocation(DaoEnvTestSpringModuleConfig.TEST_LOG4J_CONFIG_RESOURCE_LOCATION, outputPath,
                ConfigurationEntity.COLUMN_VALUE_CLOB, configKey);

            // Initialize Log4J with a refresh interval of 1/2 second. This will cause Log4J to check for configuration updates every second.
            Log4jOverridableConfigurer log4jConfigurer = getLog4jOverridableConfigurerForDb(configKey);
            log4jConfigurer.setRefreshIntervalMillis(500);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // First ensure that the Log4J output file doesn't exist.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));

            // Update the Log4J configuration with one that will create a log file.
            updateDbLog4JConfigurationFromResourceLocation(LOG4J_CONFIG_FILENAME, outputPath, ConfigurationEntity.COLUMN_VALUE_CLOB, configKey);

            // Sleep one second which will give Log4J a chance to read the new configuration file which should create an output file.
            Thread.sleep(1000);

            // Ensure that the Log4J output file now exists.
            assertTrue("Log4J output file doesn't exist, but should.", Files.exists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
            deleteDbLog4JConfiguration(configKey);
        }
    }

    @Test
    public void testLog4JNonExistentOverrideLocation() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write a Log4J configuration file that will create a random output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.setDefaultResourceLocation(DaoEnvTestSpringModuleConfig.TEST_LOG4J_CONFIG_RESOURCE_LOCATION);
            log4jConfigurer.setOverrideResourceLocation("non_existent_override_location");
            log4jConfigurer.setRefreshIntervalMillis(0);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // Since an override location doesn't exist, the default location which doesn't create a log file will get used and the override log file won't get
            // created.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    @Test
    public void testLog4JExistentOverrideLocation() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write a Log4J configuration file that will create a random output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.setDefaultResourceLocation(DaoEnvTestSpringModuleConfig.TEST_LOG4J_CONFIG_RESOURCE_LOCATION);
            log4jConfigurer.setOverrideResourceLocation(configPath.toAbsolutePath().toUri().toURL().toString());
            log4jConfigurer.setRefreshIntervalMillis(1000);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // The override location does exist and should create a log file.
            assertTrue("Log4J output file doesn't exist, but should.", Files.exists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    @Test
    public void testLog4JNoOverrideLocationOrDefaultLocation() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write a Log4J configuration file that will create a random output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // There is no database, override, or default location. This will display an error, but no logging will be configured.
            // An error will display on system.err which we don't have a way to check so we'll at least ensure that a Log4J output file didn't get created.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    @Test
    public void testLog4JBlankOverrideLocationAndDefaultLocation() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write a Log4J configuration file that will create a random output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.setDefaultResourceLocation(" ");
            log4jConfigurer.setOverrideResourceLocation(" ");
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // There is a blank default and override location so no log file will get created.
            // An error will display on system.err which we don't have a way to check so we'll at least ensure that a Log4J output file didn't get created.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    @Test
    public void testLog4JNonExistentOverrideLocationAndDefaultLocation() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write a Log4J configuration file that will create a random output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.setDefaultResourceLocation("non_existent_default_location");
            log4jConfigurer.setOverrideResourceLocation("non_existent_override_location");
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // This is similar to testLog4JNoLocationOrDefaultLocation except we are specifying explicit invalid locations instead of no locations.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    @Test
    @Ignore // This works locally, but fails in Jenkins for some reason. We'll need to investigate at some point.
    public void testLog4JFileWithRefreshInterval() throws Exception
    {
        Path configPath = getRandomLog4jConfigPath();
        Path outputPath = getRandomLog4jOutputPath();

        try
        {
            // Write the standard JUnit Log4J configuration that won't create an output file.
            writeFileFromResourceLocation(DaoEnvTestSpringModuleConfig.TEST_LOG4J_CONFIG_RESOURCE_LOCATION, configPath, outputPath);

            // Initialize Log4J with a refresh interval of 1/2 second. This will cause Log4J to check for configuration updates every second.
            Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
            log4jConfigurer.setApplicationContext(applicationContext);
            log4jConfigurer.setDefaultResourceLocation(DaoEnvTestSpringModuleConfig.TEST_LOG4J_CONFIG_RESOURCE_LOCATION);
            log4jConfigurer.setOverrideResourceLocation(configPath.toAbsolutePath().toUri().toURL().toString());
            log4jConfigurer.setRefreshIntervalMillis(500);
            log4jConfigurer.postProcessBeforeInitialization(null, null);

            // First ensure that the Log4J output file doesn't exist.
            assertTrue("Log4J output file exists, but shouldn't.", Files.notExists(outputPath));

            // Replace the Log4J configuration file with the one that will create an output file.
            writeFileFromResourceLocation(LOG4J_CONFIG_FILENAME, configPath, outputPath);

            // Sleep one second which will give Log4J a chance to read the new configuration file which should create an output file.
            Thread.sleep(1000);

            // Ensure that the Log4J output file now exists.
            assertTrue("Log4J output file doesn't exist, but should.", Files.exists(outputPath));
        }
        finally
        {
            cleanup(configPath, outputPath);
        }
    }

    /**
     * Gets a random Log4J configuration path.
     *
     * @return the Log4J configuration path.
     */
    private Path getRandomLog4jConfigPath()
    {
        return Paths
            .get(System.getProperty("java.io.tmpdir"), "log4jOverridableConfigurerConfig-log4j-" + String.valueOf(UUID.randomUUID()).substring(0, 5) + ".xml");
    }

    /**
     * Gets a random Log4J output path.
     *
     * @return the Log4J output path.
     */
    private Path getRandomLog4jOutputPath()
    {
        return Paths
            .get(System.getProperty("java.io.tmpdir"), "log4jOverridableConfigurerOutput-log4j-" + String.valueOf(UUID.randomUUID()).substring(0, 5) + ".log");
    }

    /**
     * Reads the contents of the resource location, substitutes the filename token (if it exists), and writes the contents of the resource to the local file
     * system.
     *
     * @param resourceLocation the resource location of the Log4J configuration.
     * @param configPath the Log4J configuration path.
     * @param outputPath the Log4J output path.
     *
     * @throws Exception if the file couldn't be written.
     */
    private void writeFileFromResourceLocation(String resourceLocation, Path configPath, Path outputPath) throws Exception
    {
        // Get the Log4J configuration contents from the classpath file.
        String log4JFileContents = IOUtils.toString(resourceLoader.getResource(resourceLocation).getInputStream());

        // Change the tokenized output filename (if it exists) and replace it with a random filename to support multiple invocations of the JUnit.
        log4JFileContents = log4JFileContents.replace(LOG4J_FILENAME_TOKEN, outputPath.toAbsolutePath().toString().replace("\\", "/"));

        // Write the Log4J configuration to the temporary file.
        try (FileOutputStream fileOutputStream = new FileOutputStream(configPath.toAbsolutePath().toString()))
        {
            IOUtils.write(log4JFileContents, fileOutputStream);
        }
    }

    /**
     * Reads the contents of the resource location, substitutes the filename token (if it exists), and inserts the contents of the resource to the database.
     *
     * @param resourceLocation the resource location of the Log4J configuration.
     * @param outputPath the Log4J output path.
     * @param log4jConfigurationColumn the column name for the Log4J configuration column
     * @param configEntityKey the configuration entity key.
     *
     * @throws Exception if the file contents couldn't be read or the database record couldn't be inserted.
     */
    private void insertDbLog4JConfigurationFromResourceLocation(String resourceLocation, Path outputPath, String log4jConfigurationColumn,
        String configEntityKey) throws Exception
    {
        // Get the Log4J configuration contents from the classpath file.
        String log4JFileContents = IOUtils.toString(resourceLoader.getResource(resourceLocation).getInputStream());

        // Change the tokenized output filename (if it exists) and replace it with a random filename to support multiple invocations of the JUnit.
        log4JFileContents = log4JFileContents.replace(LOG4J_FILENAME_TOKEN, outputPath.toAbsolutePath().toString().replace("\\", "/"));

        // Insert the data.
        String sql =
            String.format("INSERT INTO %s (%s, %s) VALUES (?,?)", ConfigurationEntity.TABLE_NAME, ConfigurationEntity.COLUMN_KEY, log4jConfigurationColumn);
        executePreparedStatement(sql, configEntityKey, log4JFileContents);
    }

    /**
     * Reads the contents of the resource location, substitutes the filename token (if it exists), and inserts the contents of the resource to the database.
     *
     * @param resourceLocation the resource location of the Log4J configuration.
     * @param outputPath the Log4J output path.
     * @param log4jConfigurationColumn the column name for the Log4J configuration column
     * @param configEntityKey the configuration entity key.
     *
     * @throws Exception if the file contents couldn't be read or the database record couldn't be inserted.
     */
    private void updateDbLog4JConfigurationFromResourceLocation(String resourceLocation, Path outputPath, String log4jConfigurationColumn,
        String configEntityKey) throws Exception
    {
        // Get the Log4J configuration contents from the classpath file.
        String log4JFileContents = IOUtils.toString(resourceLoader.getResource(resourceLocation).getInputStream());

        // Change the tokenized output filename (if it exists) and replace it with a random filename to support multiple invocations of the JUnit.
        log4JFileContents = log4JFileContents.replace(LOG4J_FILENAME_TOKEN, outputPath.toAbsolutePath().toString().replace("\\", "/"));

        // Update the data.
        String sql = String.format("UPDATE %s SET %s=? WHERE %s=?", ConfigurationEntity.TABLE_NAME, log4jConfigurationColumn, ConfigurationEntity.COLUMN_KEY);
        executePreparedStatement(sql, log4JFileContents, configEntityKey);
    }

    /**
     * Deletes the configuration entity record from the database.
     *
     * @param configEntityKey the configuration entity key to delete.
     *
     * @throws SQLException if any SQL errors were encountered.
     */
    private void deleteDbLog4JConfiguration(String configEntityKey) throws SQLException
    {
        if (configEntityKey != null)
        {
            String sql = String.format("DELETE FROM %s WHERE %s = ?", ConfigurationEntity.TABLE_NAME, ConfigurationEntity.COLUMN_KEY);
            executePreparedStatement(sql, configEntityKey);
        }
    }

    /**
     * Executes a SQL prepared statement with the specified arguments.
     *
     * @param sql the SQL statement.
     * @param arguments the arguments.
     *
     * @throws SQLException if any SQL errors were encountered.
     */
    private void executePreparedStatement(String sql, Object... arguments) throws SQLException
    {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try
        {
            DataSource dataSource = DaoSpringModuleConfig.getDmDataSource();
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < arguments.length; i++)
            {
                preparedStatement.setObject(i + 1, arguments[i]);
            }
            preparedStatement.execute();
        }
        finally
        {
            if (preparedStatement != null)
            {
                preparedStatement.close();
            }
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    /**
     * Gets a newly created Log4J overridable configurer for a database.
     *
     * @param configKey the configuration key to use as the where value.
     *
     * @return the Log4J overridable configurer.
     */
    private Log4jOverridableConfigurer getLog4jOverridableConfigurerForDb(String configKey)
    {
        Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
        log4jConfigurer.setTableName(ConfigurationEntity.TABLE_NAME);
        log4jConfigurer.setSelectColumn(ConfigurationEntity.COLUMN_VALUE_CLOB);
        log4jConfigurer.setWhereColumn(ConfigurationEntity.COLUMN_KEY);
        log4jConfigurer.setWhereValue(configKey);
        log4jConfigurer.setDataSource(DaoSpringModuleConfig.getDmDataSource());
        log4jConfigurer.setApplicationContext(applicationContext);
        log4jConfigurer.setRefreshIntervalMillis(0);
        return log4jConfigurer;
    }

    /**
     * Cleanup the Log4J files created.
     *
     * @param configPath the configuration path.
     * @param outputPath the output path.
     *
     * @throws IOException if any problems were encountered while cleaning up the files.
     */
    private void cleanup(Path configPath, Path outputPath) throws IOException
    {
        // Shutdown the logging which will release the lock on the output file.
        Log4jConfigurer.shutdownLogging();

        // Delete the Log4J configuration we created in the setup.
        if (Files.exists(configPath))
        {
            Files.delete(configPath);
        }

        // If we created a Log4J output file (not always), then delete it.
        if (Files.exists(outputPath))
        {
            Files.delete(outputPath);
        }
    }
}
