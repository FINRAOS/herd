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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.JdbcDao;
import org.finra.dm.dao.S3Dao;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.api.xml.JdbcConnection;
import org.finra.dm.model.api.xml.JdbcDatabaseType;
import org.finra.dm.model.api.xml.JdbcExecutionRequest;
import org.finra.dm.model.api.xml.JdbcExecutionResponse;
import org.finra.dm.model.api.xml.JdbcStatement;
import org.finra.dm.model.api.xml.JdbcStatementResultSet;
import org.finra.dm.model.api.xml.JdbcStatementStatus;
import org.finra.dm.model.api.xml.JdbcStatementType;
import org.finra.dm.model.api.xml.S3PropertiesLocation;
import org.finra.dm.service.JdbcService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.VelocityHelper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link JdbcService} which uses Spring's JDBC wrapper framework to handle connections and transactions.
 */
@Service
public class JdbcServiceImpl implements JdbcService
{
    public static final String DRIVER_REDSHIFT = "com.amazon.redshift.jdbc41.Driver";
    public static final String DRIVER_POSTGRES = "org.postgresql.Driver";
    public static final String DRIVER_ORACLE = "oracle.jdbc.OracleDriver";

    @Autowired
    private JdbcDao jdbcDao;

    @Autowired
    private VelocityHelper velocityHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * This implementation uses a {@link DriverManagerDataSource} and {@link DefaultTransactionDefinition}.
     * Creates a new transaction and delegates to executeJdbcImpl.
     * 
     * Not strictly needed for this service, but here for consistency. There are no Spring managed transactions here, since all DB operations are per request.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JdbcExecutionResponse executeJdbc(JdbcExecutionRequest jdbcExecutionRequest)
    {
        return executeJdbcImpl(jdbcExecutionRequest);
    }

    /**
     * This implementation uses a {@link DriverManagerDataSource}.
     * Uses existing Spring ORM transaction.
     * 
     * @param jdbcExecutionRequest JDBC execution request
     * @return {@link JdbcExecutionResponse}
     */
    protected JdbcExecutionResponse executeJdbcImpl(JdbcExecutionRequest jdbcExecutionRequest)
    {
        validateJdbcExecutionRequest(jdbcExecutionRequest);

        // Optionally, get properties from S3
        S3PropertiesLocation s3PropertiesLocation = jdbcExecutionRequest.getS3PropertiesLocation();
        Map<String, Object> variables = getVariablesFromS3(s3PropertiesLocation);

        // Create data source
        DataSource dataSource = createDataSource(jdbcExecutionRequest.getConnection(), variables);

        // Execute the requested statements
        List<JdbcStatement> requestJdbcStatements = jdbcExecutionRequest.getStatements();
        List<JdbcStatement> responseJdbcStatements = executeStatements(requestJdbcStatements, dataSource, variables);

        // Create and return the execution result
        return new JdbcExecutionResponse(null, responseJdbcStatements);
    }

    /**
     * Returns a map of key-value from the specified S3 properties location. Returns null if the specified location is null.
     * 
     * @param s3PropertiesLocation Location of a Java properties file in S3
     * @return {@link Map} of key-values
     */
    private Map<String, Object> getVariablesFromS3(S3PropertiesLocation s3PropertiesLocation)
    {
        Map<String, Object> variables = null;
        if (s3PropertiesLocation != null)
        {
            Properties properties = getProperties(s3PropertiesLocation);
            variables = new HashMap<>();
            for (Map.Entry<Object, Object> e : properties.entrySet())
            {
                variables.put(e.getKey().toString(), e.getValue());
            }
        }
        return variables;
    }

    /**
     * Gets an S3 object from the specified location, and parses it as a Java properties.
     * 
     * @param s3PropertiesLocation {@link S3PropertiesLocation}
     * @return {@link Properties}
     */
    private Properties getProperties(S3PropertiesLocation s3PropertiesLocation)
    {
        String s3BucketName = s3PropertiesLocation.getBucketName().trim();
        String s3ObjectKey = s3PropertiesLocation.getKey().trim();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = dmDaoHelper.getS3FileTransferRequestParamsDto();
        return s3Dao.getProperties(s3BucketName, s3ObjectKey, s3FileTransferRequestParamsDto);
    }

    /**
     * Validates parameters specified in the request.
     * 
     * @param jdbcExecutionRequest The request to validate.
     * @throws IllegalArgumentException when there are validation errors in any of the parameters
     */
    private void validateJdbcExecutionRequest(JdbcExecutionRequest jdbcExecutionRequest)
    {
        Assert.notNull(jdbcExecutionRequest, "JDBC execution request is required");
        validateJdbcConnection(jdbcExecutionRequest.getConnection());
        validateJdbcStatements(jdbcExecutionRequest.getStatements());
        validateS3PropertiesLocation(jdbcExecutionRequest.getS3PropertiesLocation());
    }

    /**
     * Validates the specified S3 properties location.
     * Asserts that if the given location is not null, bucket name and key are not blank.
     * 
     * @param s3PropertiesLocation The {@link S3PropertiesLocation} to validate.
     */
    private void validateS3PropertiesLocation(S3PropertiesLocation s3PropertiesLocation)
    {
        if (s3PropertiesLocation != null)
        {
            Assert.isTrue(StringUtils.isNotBlank(s3PropertiesLocation.getBucketName()), "S3 properties location bucket name is required");
            Assert.isTrue(StringUtils.isNotBlank(s3PropertiesLocation.getKey()), "S3 properties location key is required");
        }
    }

    /**
     * Validates parameters specified in the given statements.
     * The statements must not be null, and must not be empty.
     * 
     * @param jdbcStatements statements to validate
     */
    private void validateJdbcStatements(List<JdbcStatement> jdbcStatements)
    {
        Assert.notNull(jdbcStatements, "JDBC statements are required");
        Assert.isTrue(!jdbcStatements.isEmpty(), "JDBC statements are required");
        Integer jdbcMaxStatements = configurationHelper.getProperty(ConfigurationValue.JDBC_MAX_STATEMENTS, Integer.class);
        if (jdbcMaxStatements != null)
        {
            Assert.isTrue(jdbcStatements.size() <= jdbcMaxStatements, "The number of JDBC statements exceeded the maximum allowed " + jdbcMaxStatements + ".");
        }

        for (int i = 0; i < jdbcStatements.size(); i++)
        {
            JdbcStatement jdbcStatement = jdbcStatements.get(i);
            validateJdbcStatement(jdbcStatement, i);
        }
    }

    /**
     * Validates parameters specified in the given statement.
     * 
     * @param jdbcStatement statement to validate
     * @param jdbcStatementIndex the index number of the statement in the list
     */
    private void validateJdbcStatement(JdbcStatement jdbcStatement, int jdbcStatementIndex)
    {
        Assert.notNull(jdbcStatement, "JDBC statement [" + jdbcStatementIndex + "] is required");
        Assert.notNull(jdbcStatement.getType(), "JDBC statement [" + jdbcStatementIndex + "] type is required");
        validateSqlStatement(jdbcStatement.getSql(), jdbcStatementIndex);
    }

    /**
     * Validates parameters specified in the given connection.
     * This method does not validate whether the connection can be established.
     * 
     * @param jdbcConnection connection to validate
     */
    private void validateJdbcConnection(JdbcConnection jdbcConnection)
    {
        Assert.notNull(jdbcConnection, "JDBC connection is required");
        validateUrl(jdbcConnection.getUrl());
        Assert.notNull(jdbcConnection.getUsername(), "JDBC connection user name is required");
        Assert.notNull(jdbcConnection.getPassword(), "JDBC connection password is required");
        Assert.notNull(jdbcConnection.getDatabaseType(), "JDBC connection database type is required");
    }

    /**
     * Executes the requested statements in order.
     * Returns the result of the execution.
     * 
     * @param requestJdbcStatements The statements to execute, in order
     * @param dataSource The data source
     * @param variables
     * @return List of response {@link JdbcStatement}
     */
    private List<JdbcStatement> executeStatements(List<JdbcStatement> requestJdbcStatements, DataSource dataSource, Map<String, Object> variables)
    {
        List<JdbcStatement> responseJdbcStatements = new ArrayList<>();

        /*
         * Create a copy of all the request statements.
         * The copied statements are the response statements. The response statements are defaulted to SKIPPED.
         */
        for (JdbcStatement requestJdbcStatement : requestJdbcStatements)
        {
            JdbcStatement responseJdbcStatement = createDefaultResponseJdbcStatement(requestJdbcStatement);
            responseJdbcStatements.add(responseJdbcStatement);
        }

        // We will reuse this template for all executions
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        /*
         * Execute each statement.
         * If there were any errors, and continueOnError is not TRUE, then the execution will stop.
         * The un-executed response statements will remain in their SKIPPED status.
         */
        for (int i = 0; i < responseJdbcStatements.size(); i++)
        {
            JdbcStatement jdbcStatement = responseJdbcStatements.get(i);
            executeStatement(jdbcTemplate, jdbcStatement, variables, i);

            if (JdbcStatementStatus.ERROR.equals(jdbcStatement.getStatus()) && !Boolean.TRUE.equals(jdbcStatement.isContinueOnError()))
            {
                break;
            }
        }

        return responseJdbcStatements;
    }

    /**
     * Executes a single statement using the given JDBC template. The given statement will be updated with the result and status.
     * 
     * @param jdbcTemplate JDBC template
     * @param jdbcStatement JDBC statement to execute
     * @param variables
     * @param jdbcStatementIndex
     */
    private void executeStatement(JdbcTemplate jdbcTemplate, JdbcStatement jdbcStatement, Map<String, Object> variables, int jdbcStatementIndex)
    {
        // This is the exception to be set as the error message in the response
        Throwable exception = null;
        try
        {
            String sql = evaluate(jdbcStatement.getSql(), variables, "jdbc statement sql");
            validateSqlStatement(sql, jdbcStatementIndex);

            // Process UPDATE type statements
            if (JdbcStatementType.UPDATE.equals(jdbcStatement.getType()))
            {
                int result = jdbcDao.update(jdbcTemplate, sql);

                jdbcStatement.setStatus(JdbcStatementStatus.SUCCESS);
                jdbcStatement.setResult(String.valueOf(result));
            }
            // Process QUERY type statements
            else if (JdbcStatementType.QUERY.equals(jdbcStatement.getType()))
            {
                Integer maxResults = configurationHelper.getProperty(ConfigurationValue.JDBC_RESULT_MAX_ROWS, Integer.class);

                JdbcStatementResultSet jdbcStatementResultSet = jdbcDao.query(jdbcTemplate, sql, maxResults);

                jdbcStatement.setStatus(JdbcStatementStatus.SUCCESS);
                jdbcStatement.setResultSet(jdbcStatementResultSet);
            }
            // Any other statement types are unrecognized. This case should not be possible unless developer error.
            else
            {
                throw new IllegalStateException("Unsupported JDBC statement type '" + jdbcStatement.getType() + "'");
            }
        }
        catch (CannotGetJdbcConnectionException cannotGetJdbcConnectionException)
        {
            /*
             * When the statement fails to execute due to connection errors. This usually indicates that the connection information which was specified is
             * wrong, or there is a network issue. Either way, it would indicate user error.
             * We get the wrapped exception and throw again as an IllegalArgumentException.
             */
            Throwable causeThrowable = cannotGetJdbcConnectionException.getCause();
            throw new IllegalArgumentException(String.valueOf(causeThrowable).trim(), cannotGetJdbcConnectionException);
        }
        catch (DataAccessException dataAccessException)
        {
            // DataAccessException's cause is a SQLException which is thrown by driver
            // We will use the SQLException message result
            exception = dataAccessException.getCause();
        }

        // If there was an error
        if (exception != null)
        {
            // Set status to error and result as message
            jdbcStatement.setStatus(JdbcStatementStatus.ERROR);
            jdbcStatement.setErrorMessage(maskSensitiveInformation(exception, variables));
        }
    }

    /**
     * Returns the message of the given exception, masking any sensitive information indicated by the given collection of sensitive data.
     * If the variables is null, no masking will occur.
     * 
     * @param exception Exception message to mask
     * @param variables The mapping of variables with sensitive information.
     * @return The exception message with masked data.
     */
    private String maskSensitiveInformation(Throwable exception, Map<String, Object> variables)
    {
        String message = String.valueOf(exception).trim();
        if (variables != null)
        {
            for (Object sensitiveData : variables.values())
            {
                String sensitiveDataString = String.valueOf(sensitiveData);
                message = message.replace(sensitiveDataString, "****");
            }
        }
        return message;
    }

    /**
     * Validates the given SQL statement where its position in the list of statement is the given index.
     * This method does not validate SQL syntax.
     * 
     * @param sql SQL statement to validate
     * @param jdbcStatementIndex index in the list of statements
     */
    private void validateSqlStatement(String sql, int jdbcStatementIndex)
    {
        Assert.isTrue(StringUtils.isNotBlank(sql), "JDBC statement [" + jdbcStatementIndex + "] SQL is required");
    }

    /**
     * Creates and returns a {@link JdbcStatement} at a state which has not yet been executed based on the given request.
     * The status will be set to {@link JdbcStatementStatus#SKIPPED} and result null.
     * 
     * @param requestJdbcStatement The requested JDBC statement
     * @return a new {@link JdbcStatement}
     */
    private JdbcStatement createDefaultResponseJdbcStatement(JdbcStatement requestJdbcStatement)
    {
        JdbcStatement responseJdbcStatement = new JdbcStatement();
        responseJdbcStatement.setType(requestJdbcStatement.getType());
        responseJdbcStatement.setSql(requestJdbcStatement.getSql());
        responseJdbcStatement.setContinueOnError(requestJdbcStatement.isContinueOnError());
        responseJdbcStatement.setStatus(JdbcStatementStatus.SKIPPED);
        return responseJdbcStatement;
    }

    /**
     * Creates and returns a new data source from the given connection information.
     * Creates a new {@link DriverManagerDataSource}.
     * 
     * @param jdbcConnection The JDBC connection
     * @param variables Optional map of key-value for expression evaluation
     * @return a new {@link DataSource}
     */
    private DataSource createDataSource(JdbcConnection jdbcConnection, Map<String, Object> variables)
    {
        String url = evaluate(jdbcConnection.getUrl(), variables, "jdbc connection url");
        String username = evaluate(jdbcConnection.getUsername(), variables, "jdbc connection username");
        String password = evaluate(jdbcConnection.getPassword(), variables, "jdbc connection password");

        validateUrl(url);

        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setUrl(url);
        driverManagerDataSource.setUsername(username);
        driverManagerDataSource.setPassword(password);
        driverManagerDataSource.setDriverClassName(getDriverClassName(jdbcConnection.getDatabaseType()));
        return driverManagerDataSource;
    }

    /**
     * Validates the given URL. Does not validate URL syntax or whether the URL is accessible.
     * 
     * @param url URL string to validate
     */
    private void validateUrl(String url)
    {
        Assert.isTrue(StringUtils.isNotBlank(url), "JDBC connection URL is required");
    }

    /**
     * Evaluates the given expression as a Velocity template using the given variables.
     * Returns the expression as-is if the variables is null.
     * The given variable name will be used as the log tag.
     * 
     * @param expression
     * @param variables
     * @param variableName
     * @return
     */
    private String evaluate(String expression, Map<String, Object> variables, String variableName)
    {
        String result = expression;
        if (variables != null)
        {
            result = velocityHelper.evaluate(expression, variables, variableName);
        }
        return result;
    }

    /**
     * Returns the fully qualified driver class name of the given JDBC database type.
     * 
     * @param jdbcDatabaseType the JDBC database type
     * @return fully qualified driver class name
     * @throws IllegalArgumentException when the database type is not supported.
     */
    private String getDriverClassName(JdbcDatabaseType jdbcDatabaseType)
    {
        switch (jdbcDatabaseType)
        {
            case ORACLE:
                return DRIVER_ORACLE;
            case POSTGRES:
                return DRIVER_POSTGRES;
            case REDSHIFT:
                return DRIVER_REDSHIFT;
            default:
                throw new IllegalArgumentException("Unsupported database type '" + jdbcDatabaseType + "'");
        }
    }
}
