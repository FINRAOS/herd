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

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.Resource;
import org.springframework.util.Log4jConfigurer;

/**
 * Configure Log4J based on a possible resource override location, a possible database override location, and a default resource location, in that order. An
 * optional refresh interval can also be specified if the Log4J configuration should be monitored for changes at every "refresh interval" and re-initialized if
 * changes are found.
 * <p/>
 * This class will write to System.out and System.err for output before the logging is successfully initialized. Once the logging is initialized, logging will
 * go to where the logger is configured. Note that it is important to use valid Log4J configuration files for Log4J to work properly. If an invalid
 * configuration file is used, Log4J will output the error to System.err, but will not throw an exception so this class will not know that logging hasn't been
 * configured properly. That means that application logging messages could be lost. If logging was initialized successfully and a new override file is found
 * that is invalid, the previous valid configuration will remain in effect until a new valid configuration is found.
 * <p/>
 * This class contains nested named inner-classes. The reason for this was to keep everything required for the Log4JOverridableConfigurer functionality fully
 * encapsulated within one file so it could be easily ported if needed.
 * <p/>
 * This bean is a bean post processor and is priority ordered. By being a bean post processor, this class will get called early in the bean creation lifecycle:
 * specifically after the beans are instantiated, but before they are used. By being priority ordered (as opposed to standard ordering), our bean will get
 * called before other post processors. This is important to ensure logging is initialized before other things are initialized that could have problems. If
 * other beans have problems and perform error logging (e.g. Hibernate session manager, Ehcache, JGroups), we will loose that logging if we haven't initialized
 * our logging first.
 * <p/>
 * To use a Log4J configuration specified in the database, configure these properties:
 * <pre>
 *     dataSource: the data source for the database to connect to.
 *     tableName: the table name that contains the Log4J configuration.
 *     selectColumn: the column to select that contains the Log4J configuration.
 *     whereColumn and whereValue: an optional column name and value to filter on if multiple rows would be returned where a single row is required.
 * </pre>
 * <p/>
 * To use an externally specified Log4J override configuration, configure the overrideResourceLocation (e.g. file:///tmp/myOverride.xml).
 * <p/>
 * To use the default Log4J configuration, specify an internally bundled defaultResourceLocation (e.g. classpath:dm-log4j.xml).
 */
@SuppressFBWarnings(value = "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING",
    justification = "The SQL used within this class is self-contained and only varies based on the Spring configuration so there is no risk of SQL " +
        "injection.")
public class Log4jOverridableConfigurer implements BeanPostProcessor, PriorityOrdered, ApplicationContextAware
{
    // Only use this once logging has been initialized.
    private static final Logger LOGGER = Logger.getLogger(Log4jOverridableConfigurer.class);

    // The name of the environment variable to specify the base path for logging.
    private static final String DM_LOG4J_OUTPUT_BASE_PATH_ENVIRONMENT_VARIABLE = "DM_LOG4J_OUTPUT_BASE_PATH";

    @Autowired
    private ApplicationContext applicationContext;

    // Configurable properties
    private String overrideResourceLocation;
    private long refreshIntervalMillis;
    private String defaultResourceLocation;
    private String tableName;
    private String selectColumn;
    private String whereColumn;
    private String whereValue;
    private DataSource dataSource;

    private boolean loggingInitialized;
    private String existingDbLog4JConfiguration;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException
    {
        // Perform our initialization one time even though this method will be called once for each bean being initialized.
        // The bean properties will have already been set by the time this method is called which is required for our initialization.
        if (!loggingInitialized)
        {
            initLogging();
            loggingInitialized = true;
        }

        // Perform the standard processing by returning the original bean.
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException
    {
        // Don't do anything here but return the original bean.
        return bean;
    }

    /**
     * Perform the Log4J initialization (after the properties have been set).
     */
    // Using System.out and System.err is okay here because we need a place to output information before logging is initialized.
    @SuppressWarnings("PMD.SystemPrintln")
    private void initLogging()
    {
        // TODO: Remove setting system property below once we upgrade to log4j 2.X and switch to using environment variables directly in the XML
        // configuration using ${env:<VAR_NAME>} notation. For now, we need to explicitly set the system property from the environment variable.
        // Set the relative system property to specify the base path for logging.
        String value = System.getenv(DM_LOG4J_OUTPUT_BASE_PATH_ENVIRONMENT_VARIABLE);
        if (StringUtils.isNotBlank(value))
        {
            System.setProperty(DM_LOG4J_OUTPUT_BASE_PATH_ENVIRONMENT_VARIABLE, value);
        }

        // First try the override resource location. This gives the local machine a chance to override the configuration. This could be useful
        // for local developers who want to create a local override configuration file.
        boolean initialized = initializeLog4jFromResourceLocation(overrideResourceLocation);

        // If the override resource location wasn't used, then try the database configuration. This is useful when you have a cluster of application
        // servers that all share a common configuration.
        if (!initialized)
        {
            initialized = initializeLog4jFromDatabase();
        }

        // If no override location was found, try the default resource location which is typically one bundled within the WAR.
        if (!initialized)
        {
            initialized = initializeLog4jFromResourceLocation(defaultResourceLocation);
        }

        // If we didn't find any Log4J configuration, then display an error.
        if (!initialized)
        {
            // We shouldn't typically get here if a valid Log4J configuration file was bundled with the WAR.
            System.err.println("Unable to find a valid Log4J configuration using database query \"" + getLog4JConfigurationRetrievalQuery(true) +
                "\" or the override resource location \"" + overrideResourceLocation + "\" or the default resource location \"" +
                defaultResourceLocation +
                "\".");
        }
    }

    /**
     * Initializes Log4J from a resource location.
     *
     * @param resourceLocation the resource location.
     *
     * @return true if Log4J was initialized or false if not.
     */
    @SuppressWarnings("PMD.SystemPrintln")
    private boolean initializeLog4jFromResourceLocation(String resourceLocation)
    {
        // Default the return boolean to false (i.e. we didn't initialize Log4J).
        boolean isInitSuccessful = false;

        // See if an override resource location is configured.
        if (StringUtils.isNotBlank(resourceLocation))
        {
            // Trim the resource location and get a handle to the resource.
            String resourceLocationTrimmed = resourceLocation.trim();
            Resource resource = applicationContext.getResource(resourceLocationTrimmed);

            // If the resource exists, then initialize Log4J with the resource.
            if (resource.exists())
            {
                // Initialize Log4J from the resource location.
                // Write the "good" parameters to System.out since logging hasn't been initialized yet.
                System.out
                    .println("Using Log4J configuration location \"" + resourceLocationTrimmed + "\" and refresh interval \"" + refreshIntervalMillis + "\".");

                try
                {
                    if (refreshIntervalMillis != 0)
                    {
                        // Initialize with refresh interval (i.e. with Log4J's watchdog thread checking the file in the background).
                        Log4jConfigurer.initLogging(resourceLocationTrimmed, refreshIntervalMillis);
                    }
                    else
                    {
                        // Initialize without refresh check (i.e. without Log4J's watchdog thread).
                        Log4jConfigurer.initLogging(resourceLocationTrimmed);
                    }

                    // Now that Logging has been initialized, log something so we know it's working.
                    LOGGER.info("Logging successfully initialized.");
                }
                catch (FileNotFoundException ex)
                {
                    // We shouldn't get here since we already checked if the location existed previously.
                    throw new IllegalArgumentException("Invalid location configuration: \"" + resourceLocationTrimmed + "\".", ex);
                }

                // Mark that we successfully initialized Log4J.
                isInitSuccessful = true;
            }
        }

        // Return if we successfully initialized Log4J or not.
        return isInitSuccessful;
    }

    /**
     * Initializes Log4J from the database.
     *
     * @return true if Log4J was initialized or false if not.
     */
    @SuppressWarnings("PMD.SystemPrintln")
    private boolean initializeLog4jFromDatabase()
    {
        // Default the return boolean to false (i.e. we didn't initialize Log4J).
        boolean isInitSuccessful = false;

        // Try the database override location if all the necessary DB parameters were specified.
        if (isDbConfigurationPresent())
        {
            existingDbLog4JConfiguration = getLog4JConfigurationFromDatabase();
        }

        // If a database configuration was found, try to initialize with it.
        if (StringUtils.isNotBlank(existingDbLog4JConfiguration))
        {
            // Write the "good" parameters to System.out since logging hasn't been initialized yet.
            System.out.println(
                "Using Log4J configuration from the database using query \"" + getLog4JConfigurationRetrievalQuery(true) + "\" and refresh interval \"" +
                    refreshIntervalMillis + "\".");

            // Proceed with the initial Log4J initialization based on the retrieved database configuration.
            new DOMConfigurator().doConfigure(new StringReader(existingDbLog4JConfiguration), LogManager.getLoggerRepository());

            // If we have a refresh interval, start a Log4J database watchdog which will keep monitoring the database for changes to the configuration.
            if (refreshIntervalMillis != 0)
            {
                new Log4jDbWatchdog().start();
            }

            // Now that Logging has been initialized, log something so we know it's working.
            LOGGER.info("Logging successfully initialized.");

            // Mark that we successfully initialized Log4J.
            isInitSuccessful = true;
        }

        // Return if we successfully initialized Log4J or not.
        return isInitSuccessful;
    }

    /**
     * Returns whether the database configuration is present or not (i.e. the select column and the tableName have both been specified).
     *
     * @return true if specified or false if not.
     */
    private boolean isDbConfigurationPresent()
    {
        return StringUtils.isNotBlank(selectColumn) && StringUtils.isNotBlank(tableName);
    }

    /**
     * Gets the Log4J configuration retrieval query.
     *
     * @param isForLogging If true, the statement will be returned for logging purposes (i.e. the where clause value will be present instead of a "?"
     * character.
     *
     * @return the query.
     */
    private String getLog4JConfigurationRetrievalQuery(boolean isForLogging)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ").append(selectColumn).append(" FROM ").append(tableName);
        if (StringUtils.isNotBlank(whereColumn) && StringUtils.isNotBlank(whereValue))
        {
            stringBuilder.append(" WHERE ").append(whereColumn).append(" = ");
            if (isForLogging)
            {
                stringBuilder.append('\'').append(whereValue).append('\'');
            }
            else
            {
                stringBuilder.append('?');
            }
        }
        return stringBuilder.toString();
    }

    /**
     * Gets the Log4J override configuration from the database.
     *
     * @return the Log4J configuration or null if no configuration was found.
     */
    protected String getLog4JConfigurationFromDatabase()
    {
        // Create a JDBC operation that can execute our query and return the Log4J configuration.
        JdbcOperation<String> jdbcOperation = new JdbcOperation<String>(dataSource)
        {
            @Override
            protected String performOperation() throws SQLException
            {
                try (ResultSet resultSet = openResultSet(getLog4JConfigurationRetrievalQuery(false), whereValue))
                {
                    if (resultSet.next())
                    {
                        return getConfigurationFromResultSet(resultSet, selectColumn);
                    }

                    // No results were found so return null.
                    return null;
                }
            }
        };

        // Execute the JDBC operation.
        try
        {
            return jdbcOperation.execute();
        }
        catch (SQLException ex)
        {
            // In the case of a SQL exception, just return null indicating there is no override configuration available.
            LOGGER.error("SQL Exception encountered while getting Log4J override configuration.", ex);
            return null;
        }
    }

    /**
     * Extracts the configuration from the given result set. The passed in {@code ResultSet} was created by a SELECT statement on the underlying database table.
     * The selectColumn is the column that will be retrieved from the result set. Normally the contained value is directly returned. However, if it is of type
     * {@code CLOB}, text is extracted as string. This method was mostly obtained from the DatabaseConfiguration class.
     *
     * @param resultSet the current {@code ResultSet}
     * @param selectColumn the selected column to obtain from the result set.
     *
     * @return the Log4J configuration.
     * @throws SQLException if an error occurs.
     */
    protected String getConfigurationFromResultSet(ResultSet resultSet, String selectColumn) throws SQLException
    {
        // Default to a return value of null.
        String returnString = null;

        // Get the value from the result set for the select column.
        Object resultSetValue = resultSet.getObject(selectColumn);
        if (resultSetValue != null)
        {
            if (resultSetValue instanceof Clob)
            {
                // If we have a CLOB, we need to extract the contents of the CLOB to a string.
                returnString = convertClob((Clob) resultSetValue);
            }
            else
            {
                // If we have any other value (should typically be a string), perform toString on it to ensure we have a string to return.
                returnString = resultSetValue.toString();
            }
        }

        // Return the return value as a string.
        return returnString;
    }

    /**
     * Converts a CLOB to a string. This method was mostly obtained from DatabaseConfiguration.
     *
     * @param clob the CLOB to be converted.
     *
     * @return the extracted string value.
     * @throws SQLException if an error occurs.
     */
    protected String convertClob(Clob clob) throws SQLException
    {
        int len = (int) clob.length();
        return (len > 0) ? clob.getSubString(1, len) : ""; // Note getSubString has the first character at position 1 (not 0).
    }

    @Override
    public int getOrder()
    {
        // We want our logging to come very early in the bean creation process so we are setting the priority very high.
        // (0 is the highest and MAX_INT is the lowest). We're using 100 to make it high, but not 0 to give other beans we write a chance to
        // use a higher priority if desired.
        return 100;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
    {
        this.applicationContext = applicationContext;
    }

    /**
     * Set the Log4J override configuration resource location to use. If the override location doesn't exist, the default resource location will be attempted.
     *
     * @param overrideResourceLocation the location.
     */
    public void setOverrideResourceLocation(String overrideResourceLocation)
    {
        this.overrideResourceLocation = overrideResourceLocation;
    }

    /**
     * The default Log4J configuration location to use if the override location isn't found.
     *
     * @param defaultResourceLocation the default location.
     */
    public void setDefaultResourceLocation(String defaultResourceLocation)
    {
        this.defaultResourceLocation = defaultResourceLocation;
    }

    /**
     * The Log4J refresh interview in milliseconds. This is the amount of time that will pass before the configuration is re-read to obtain any potential
     * changes made to the configuration.
     *
     * @param refreshIntervalMillis the refresh interval.
     */
    public void setRefreshIntervalMillis(long refreshIntervalMillis)
    {
        this.refreshIntervalMillis = refreshIntervalMillis;
    }

    /**
     * Sets the table name that contains the Log4J database override configuration.
     *
     * @param tableName the table name.
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Sets the column to be selected (from the specified "tableName") that contains the Log4J database override configuration.
     *
     * @param selectColumn the select column.
     */
    public void setSelectColumn(String selectColumn)
    {
        this.selectColumn = selectColumn;
    }

    /**
     * Sets the optional where column to filter on to determine which row to retrieve that contains the Log4J database override configuration. If no where
     * column or value is specified, then only one row should exist in the specified table.
     *
     * @param whereColumn the where column to filter on.
     */
    public void setWhereColumn(String whereColumn)
    {
        this.whereColumn = whereColumn;
    }

    /**
     * Sets the optional where clause value to filter on to determine which row to retrieve that contains the Log4J database override configuration. If no where
     * column or value is specified, then only one row should exist in the specified table.
     *
     * @param whereValue the where value to filter on.
     */
    public void setWhereValue(String whereValue)
    {
        this.whereValue = whereValue;
    }

    /**
     * Sets the data source to use when connecting to the database to get the Log4J override configuration.
     *
     * @param dataSource the data source.
     */
    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    /**
     * An internally used helper class for simplifying database access through plain JDBC. This class provides a simple framework for creating and executing a
     * JDBC statement. It especially takes care of proper handling of JDBC resources even in case of an error. This code was mostly taken from the Commons
     * Configuration DatabaseConfiguration class:
     * <pre>
     * https://apache.googlesource.com/commons-configuration/+/trunk/src/main/java/org/apache/commons/configuration2/DatabaseConfiguration.java
     * </pre>
     * Using a simple low level JDBC class such as this as opposed to a higher level abstraction such as JPA/Hibernate is preferred since we want logging
     * initialized before as many other technologies as possible. That way, we will be able to capture higher level technology logging in our Log4J logs and not
     * lose it.
     *
     * @param <T> the type of the results produced by a JDBC operation
     */
    protected abstract class JdbcOperation<T>
    {
        /**
         * Stores the connection.
         */
        private Connection connection;

        /**
         * Stores the statement.
         */
        private PreparedStatement preparedStatement;

        /**
         * Stores the result set.
         */
        private ResultSet resultSet;

        /**
         * Stores the data source used to connect to the database.
         */
        private DataSource dataSource;

        /**
         * Creates a new instance of {@code JdbcOperation}.
         *
         * @param dataSource the data source to the database.
         */
        protected JdbcOperation(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        /**
         * Performs the JDBC operation. This method is called by {@code execute()} after this object has been fully initialized. Here the actual JDBC logic has
         * to be placed.
         *
         * @return the result of the operation
         * @throws SQLException if an SQL error occurs
         */
        protected abstract T performOperation() throws SQLException;

        /**
         * Executes this operation. This method obtains a database connection and then delegates to {@code performOperation()}. Afterwards it performs the
         * necessary clean up.
         *
         * @return the result of the operation.
         * @throws SQLException if there was a problem executing the operation.
         */
        public T execute() throws SQLException
        {
            T result = null;
            try
            {
                connection = dataSource.getConnection();
                result = performOperation();
            }
            finally
            {
                close(connection, preparedStatement, resultSet);
            }
            return result;
        }

        /**
         * Creates a {@code PreparedStatement} for a query, initializes it and executes it. The resulting {@code ResultSet} is returned.
         *
         * @param sql the statement to be executed.
         * @param params the parameters for the statement.
         *
         * @return the {@code ResultSet} produced by the query.
         * @throws SQLException if an SQL error occurs.
         */
        protected ResultSet openResultSet(String sql, Object... params) throws SQLException
        {
            resultSet = initStatement(sql, params).executeQuery();
            return resultSet;
        }

        /**
         * Creates an initializes a {@code PreparedStatement} object for executing an SQL statement.
         *
         * @param sql the statement to be executed
         * @param parameters the parameters for the statement
         *
         * @return the initialized statement object
         * @throws SQLException if an SQL error occurs
         */
        protected PreparedStatement initStatement(String sql, Object... parameters) throws SQLException
        {
            // Create the prepared statement.
            preparedStatement = getConnection().prepareStatement(sql);

            // Loop through all the parameters and set them on the prepared statement.
            for (int i = 0; i < parameters.length; i++)
            {
                preparedStatement.setObject(i + 1, parameters[i]);
            }

            // Return the prepared statement.
            return preparedStatement;
        }

        /**
         * Returns the current connection. This method can be called while {@code execute()} is running. It returns <b>null</b> otherwise.
         *
         * @return the current connection
         */
        protected Connection getConnection()
        {
            return connection;
        }

        /**
         * Close the specified database objects. Avoid closing if null and hide any SQLExceptions that occur.
         *
         * @param connection The database connection to close
         * @param statement The statement to close
         * @param resultSet the result set to close
         */
        protected void close(Connection connection, Statement statement, ResultSet resultSet)
        {
            // Close the result set if it exists.
            try
            {
                if (resultSet != null)
                {
                    resultSet.close();
                }
            }
            catch (SQLException ex)
            {
                LOGGER.error("An error occurred on closing the result set.", ex);
            }

            // Close the statement if it exists.
            try
            {
                if (statement != null)
                {
                    statement.close();
                }
            }
            catch (SQLException ex)
            {
                LOGGER.error("An error occurred on closing the statement.", ex);
            }

            // Close the connection if it exists.
            try
            {
                if (connection != null)
                {
                    connection.close();
                }
            }
            catch (SQLException ex)
            {
                LOGGER.error("An error occurred on closing the connection.", ex);
            }
        }
    }

    /**
     * An internally used watchdog thread that keeps querying the database to retrieve the Log4J configuration each time it wakes up. Each time it queries the
     * configuration, it compares it to the previously read configuration and if any changes are present, the Log4J configuration is re-initialized. This is
     * similar to the org.apache.log4j.helpers.FileWatchdog class. Note that this thread will first sleep before it does anything so it is important that the
     * caller first initializes Log4J before this thread is started.
     */
    protected class Log4jDbWatchdog extends Thread
    {
        /**
         * The main "run" method which gets invoked when this thread gets started.
         */
        @Override
        public void run()
        {
            // Keep running until the JVM exits.
            while (true)
            {
                try
                {
                    // Sleep for the refresh interval.
                    Thread.sleep(refreshIntervalMillis);
                }
                catch (InterruptedException ex)
                {
                    // We don't really expect to get interrupted, but log a warning message so we're aware of it just in case.
                    LOGGER.warn("An attempt was made to interrupt a sleeping thread.", ex);
                }

                // Each time we wake up, read the latest configuration from the database and re-configure Log4J if a change is found from the previously
                // read configuration.
                String latestLog4JConfiguration = getLog4JConfigurationFromDatabase();
                if ((StringUtils.isNotBlank(latestLog4JConfiguration)) && (!(latestLog4JConfiguration.equals(existingDbLog4JConfiguration))))
                {
                    LOGGER.info("Log4J configuration change found in database so Log4J is being re-initialized.");
                    existingDbLog4JConfiguration = latestLog4JConfiguration;
                    new DOMConfigurator().doConfigure(new StringReader(existingDbLog4JConfiguration), LogManager.getLoggerRepository());
                }
            }
        }
    }
}