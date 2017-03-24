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
package org.finra.herd.service;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.model.api.xml.JdbcExecutionRequest;
import org.finra.herd.model.api.xml.JdbcExecutionResponse;
import org.finra.herd.model.api.xml.JdbcStatement;
import org.finra.herd.model.api.xml.JdbcStatementResultSetRow;
import org.finra.herd.model.api.xml.JdbcStatementStatus;
import org.finra.herd.model.api.xml.JdbcStatementType;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Test cases for {@link org.finra.herd.service.JdbcService}
 */
public class JdbcServiceTest extends AbstractServiceTest
{
    @Autowired
    private JdbcServiceTestHelper jdbcServiceTestHelper;

    /**
     * Use case where a single successful statement is executed.
     */
    @Test
    public void testExecuteJdbcStatementSuccess()
    {
        // Get test request
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();

        // Execute
        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        // Assert results
        Assert.assertNull("JDBC connection is not null", jdbcExecutionResponse.getConnection());
        Assert.assertEquals("JDBC statements size", jdbcExecutionRequest.getStatements().size(), jdbcExecutionResponse.getStatements().size());
        {
            JdbcStatement expectedJdbcStatement = jdbcExecutionRequest.getStatements().get(0);
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(0);

            Assert.assertEquals("JDBC statement [0] type", expectedJdbcStatement.getType(), actualJdbcStatement.getType());
            Assert.assertEquals("JDBC statement [0] sql", expectedJdbcStatement.getSql(), actualJdbcStatement.getSql());
            Assert.assertEquals("JDBC statement [0] status", JdbcStatementStatus.SUCCESS, actualJdbcStatement.getStatus());
            Assert.assertEquals("JDBC statement [0] result", "1", actualJdbcStatement.getResult());
        }
    }

    /**
     * Use case where 3 statements are requested to be executed, but the 2nd is erroneous. The first statement should result in SUCCESS. The second should
     * result in ERROR, with appropriate result message. The third should result in SKIPPED with no result.
     */
    @Test
    public void testExecuteJdbcStatementError()
    {
        // Create test request
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        // First statement already included
        // Second statement uses case 2 which throws an error
        jdbcExecutionRequest.getStatements().add(new JdbcStatement(JdbcStatementType.UPDATE, MockJdbcOperations.CASE_2_SQL, null, null, null, null, null));
        jdbcExecutionRequest.getStatements().add(new JdbcStatement(JdbcStatementType.UPDATE, MockJdbcOperations.CASE_1_SQL, null, null, null, null, null));

        // Execute
        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        // Assert results
        Assert.assertNull("JDBC connection is null", jdbcExecutionResponse.getConnection());
        Assert.assertEquals("JDBC statements size", jdbcExecutionRequest.getStatements().size(), jdbcExecutionResponse.getStatements().size());
        {
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(0);
            Assert.assertEquals("JDBC statement [0] status", JdbcStatementStatus.SUCCESS, actualJdbcStatement.getStatus());
        }
        {
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(1);

            Assert.assertEquals("JDBC statement [1] status", JdbcStatementStatus.ERROR, actualJdbcStatement.getStatus());
            Assert.assertNull("JDBC statement [1] result is not null", actualJdbcStatement.getResult());
            Assert.assertEquals("JDBC statement [1] error message", "java.sql.SQLException: test DataIntegrityViolationException cause",
                actualJdbcStatement.getErrorMessage());
        }
        {
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(2);

            Assert.assertEquals("JDBC statement [2] status", JdbcStatementStatus.SKIPPED, actualJdbcStatement.getStatus());
            Assert.assertNull("JDBC statement [2] result is not null", actualJdbcStatement.getResult());
        }
    }

    /**
     * Test case where statements result in errors, but continue on error flag is set to true for those statements. The subsequent statements should continue
     * executing.
     */
    @Test
    public void testExecuteJdbcStatementErrorContinueOnError()
    {
        // Create test request
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        // First statement already included
        // Second statement uses case 2 which throws an error
        jdbcExecutionRequest.getStatements().add(new JdbcStatement(JdbcStatementType.UPDATE, MockJdbcOperations.CASE_2_SQL, true, null, null, null, null));
        jdbcExecutionRequest.getStatements().add(new JdbcStatement(JdbcStatementType.UPDATE, MockJdbcOperations.CASE_1_SQL, false, null, null, null, null));

        // Execute
        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        // Assert results
        Assert.assertNull("JDBC connection is not null", jdbcExecutionResponse.getConnection());
        Assert.assertEquals("JDBC statements size", jdbcExecutionRequest.getStatements().size(), jdbcExecutionResponse.getStatements().size());
        {
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(0);
            Assert.assertEquals("JDBC statement [0] status", JdbcStatementStatus.SUCCESS, actualJdbcStatement.getStatus());
        }
        {
            JdbcStatement expectedJdbcStatement = jdbcExecutionResponse.getStatements().get(1);
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(1);

            Assert.assertEquals("JDBC statement [1] continue on error", expectedJdbcStatement.isContinueOnError(), actualJdbcStatement.isContinueOnError());
            Assert.assertEquals("JDBC statement [1] status", JdbcStatementStatus.ERROR, actualJdbcStatement.getStatus());
            Assert.assertNull("JDBC statement [1] result is not null", actualJdbcStatement.getResult());
            Assert.assertEquals("JDBC statement [1] error message", "java.sql.SQLException: test DataIntegrityViolationException cause",
                actualJdbcStatement.getErrorMessage());
        }
        {
            JdbcStatement expectedJdbcStatement = jdbcExecutionResponse.getStatements().get(2);
            JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(2);

            Assert.assertEquals("JDBC statement [2] status", expectedJdbcStatement.isContinueOnError(), actualJdbcStatement.isContinueOnError());
            Assert.assertEquals("JDBC statement [2] status", JdbcStatementStatus.SUCCESS, actualJdbcStatement.getStatus());
        }
    }

    /**
     * Test case where user specifies a QUERY statement type. A proper result set should be created.
     */
    @Test
    public void testExecuteJdbcStatementTypeQuerySuccess()
    {
        // Get test request
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultQueryJdbcExecutionRequest();
        JdbcStatement expectedJdbcStatement = jdbcExecutionRequest.getStatements().get(0);

        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        Assert.assertEquals("JDBC statements size", 1, jdbcExecutionResponse.getStatements().size());

        JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(0);
        Assert.assertNull("JDBC statement error message is not null", actualJdbcStatement.getErrorMessage());
        Assert.assertNull("JDBC statement result not is null", actualJdbcStatement.getResult());
        Assert.assertEquals("JDBC statement SQL", expectedJdbcStatement.getSql(), actualJdbcStatement.getSql());
        Assert.assertEquals("JDBC statement status", JdbcStatementStatus.SUCCESS, actualJdbcStatement.getStatus());
        Assert.assertEquals("JDBC statement type", expectedJdbcStatement.getType(), actualJdbcStatement.getType());
        Assert.assertNotNull("JDBC statement result set is null", actualJdbcStatement.getResultSet());
        Assert.assertNotNull("JDBC statement result set column names is null", actualJdbcStatement.getResultSet().getColumnNames());
        Assert
            .assertEquals("JDBC statement result set column names", Arrays.asList("COL1", "COL2", "COL3"), actualJdbcStatement.getResultSet().getColumnNames());
        Assert.assertNotNull("JDBC statement result set rows is null", actualJdbcStatement.getResultSet().getRows());
        Assert.assertEquals("JDBC statement result set rows size", 2, actualJdbcStatement.getResultSet().getRows().size());
        {
            JdbcStatementResultSetRow row = actualJdbcStatement.getResultSet().getRows().get(0);
            Assert.assertNotNull("JDBC statement row [0] columns is null", row.getColumns());
            Assert.assertEquals("JDBC statement row [0] columns", Arrays.asList("A", "B", "C"), row.getColumns());
        }
        {
            JdbcStatementResultSetRow row = actualJdbcStatement.getResultSet().getRows().get(1);
            Assert.assertNotNull("JDBC statement row [1] columns is null", row.getColumns());
            Assert.assertEquals("JDBC statement row [1] columns", Arrays.asList("D", "E", "F"), row.getColumns());
        }
    }

    /**
     * Test case where user specifies a QUERY statement type and a maximum number of rows is specified in the environment.
     */
    @Test
    public void testExecuteJdbcStatementTypeQueryMaximumRows()
    {
        int expectedRowSize = 1;
        try
        {
            Map<String, Object> overrideMap = new HashMap<>();
            overrideMap.put(ConfigurationValue.JDBC_RESULT_MAX_ROWS.getKey(), expectedRowSize);
            modifyPropertySourceInEnvironment(overrideMap);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error modifying environment variables", e);
        }

        try
        {
            // Get test request
            JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultQueryJdbcExecutionRequest();

            JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

            Assert.assertEquals("result set row size", expectedRowSize, jdbcExecutionResponse.getStatements().get(0).getResultSet().getRows().size());
        }
        finally
        {
            try
            {
                restorePropertySourceInEnvironment();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error restoring environment variables. Subsequent tests may be affected.", e);
            }
        }
    }

    /**
     * Test case where user specifies a QUERY statement type, but there are SQL errors. The status should be ERROR and no result set should exist in the
     * result.
     */
    @Test
    public void testExecuteJdbcStatementTypeQueryError()
    {
        // Get test request
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultQueryJdbcExecutionRequest();
        JdbcStatement expectedJdbcStatement = jdbcExecutionRequest.getStatements().get(0);
        expectedJdbcStatement.setSql(MockJdbcOperations.CASE_2_SQL);

        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        Assert.assertEquals("JDBC statements size", 1, jdbcExecutionResponse.getStatements().size());

        JdbcStatement actualJdbcStatement = jdbcExecutionResponse.getStatements().get(0);
        Assert.assertNotNull("JDBC statement error message", actualJdbcStatement.getErrorMessage());
        Assert.assertEquals("JDBC statement error message", "java.sql.SQLException: test DataIntegrityViolationException cause",
            actualJdbcStatement.getErrorMessage());
        Assert.assertNull("JDBC statement result", actualJdbcStatement.getResult());
        Assert.assertEquals("JDBC statement status", JdbcStatementStatus.ERROR, actualJdbcStatement.getStatus());
        Assert.assertEquals("JDBC statement type", expectedJdbcStatement.getType(), actualJdbcStatement.getType());
        Assert.assertNull("JDBC statement result set", actualJdbcStatement.getResultSet());
    }

    /**
     * Parameter validation, request object is null
     */
    @Test
    public void testExecuteJdbcParamValidationRequestNull()
    {
        try
        {
            // Execute
            jdbcService.executeJdbc(null);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC execution request is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request connection is null
     */
    @Test
    public void testExecuteJdbcParamValidationConnectionNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setConnection(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request connection URL is empty
     */
    @Test
    public void testExecuteJdbcParamValidationConnectionUrlEmpty()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setUrl(" \t\n\r");

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection URL is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request connection username is null. Username can be empty however, since some databases allow that.
     */
    @Test
    public void testExecuteJdbcParamValidationConnectionUsernameNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setUsername(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection user name is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request connection password is null. Password can be empty however, since some databases allow that.
     */
    @Test
    public void testExecuteJdbcParamValidationConnectionPasswordNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setPassword(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection password is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request connection database type is null.
     */
    @Test
    public void testExecuteJdbcParamValidationConnectionDatabaseTypeNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setDatabaseType(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection database type is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request statement list is null
     */
    @Test
    public void testExecuteJdbcParamValidationStatementsNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setStatements(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC statements are required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request statement list has less statements than configured maximum.
     */
    @Test
    public void testExecuteJdbcParamValidationStatementsMaximumStatementsSpecified()
    {
        /*
         * Update environment variable to use a maximum statement
         */
        int maxStatements = 1;
        try
        {
            HashMap<String, Object> overrideMap = new HashMap<>();
            overrideMap.put(ConfigurationValue.JDBC_MAX_STATEMENTS.getKey(), maxStatements);
            modifyPropertySourceInEnvironment(overrideMap);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error modifying environment variable.", e);
        }

        /*
         * Add 1 more statement to the JDBC request.
         * The default request should already have 1 statement.
         */
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();

        try
        {
            // Execute
            JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

            Assert.assertNotNull("jdbcExecutionResponse", jdbcExecutionResponse);
        }
        finally
        {
            try
            {
                restorePropertySourceInEnvironment();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error restoring environment variables. Subsequent tests may be affected.", e);
            }
        }
    }

    /**
     * Parameter validation, request statement list has more statements than configured maximum.
     */
    @Test
    public void testExecuteJdbcParamValidationStatementsExceedMaximum()
    {
        /*
         * Update environment variable to use a maximum statement
         */
        int maxStatements = 1;
        try
        {
            HashMap<String, Object> overrideMap = new HashMap<>();
            overrideMap.put(ConfigurationValue.JDBC_MAX_STATEMENTS.getKey(), maxStatements);
            modifyPropertySourceInEnvironment(overrideMap);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error modifying environment variable.", e);
        }

        /*
         * Add 1 more statement to the JDBC request.
         * The default request should already have 1 statement.
         */
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().add(new JdbcStatement());

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert
                .assertEquals("thrown exception message", "The number of JDBC statements exceeded the maximum allowed " + maxStatements + ".", e.getMessage());
        }
        finally
        {
            /*
             * 
             */
            try
            {
                restorePropertySourceInEnvironment();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error restoring environment variables. Subsequent tests may be affected.", e);
            }
        }
    }

    /**
     * Parameter validation, request statement list is empty
     */
    @Test
    public void testExecuteJdbcParamValidationStatementsEmpty()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().clear();

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC statements are required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request statement type is null
     */
    @Test
    public void testExecuteJdbcParamValidationStatementTypeNull()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setType(null);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC statement [0] type is required", e.getMessage());
        }
    }

    /**
     * Parameter validation, request statement sql is empty
     */
    @Test
    public void testExecuteJdbcParamValidationStatementTypeSqlEmpty()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql(" \t\n\r");

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC statement [0] SQL is required", e.getMessage());
        }
    }

    @Test
    public void testExecuteJdbcErrorConnection()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql(MockJdbcOperations.CASE_3_SQL);

        try
        {
            // Execute
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "java.sql.SQLException: test CannotGetJdbcConnectionException cause", e.getMessage());
        }
    }

    /**
     * When S3 properties location is specified, bucket name must not be a blank string.
     */
    @Test
    public void testExecuteJdbcParamValidationS3PropertiesLocationBucketNameBlank()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation(BLANK_TEXT, "test_key"));

        try
        {
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "S3 properties location bucket name is required", e.getMessage());
        }
    }

    /**
     * When S3 properties location is specified, object key must not be a blank string.
     */
    @Test
    public void testExecuteJdbcParamValidationS3PropertiesLocationKeyBlank()
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation("test_bucket", BLANK_TEXT));

        try
        {
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "S3 properties location key is required", e.getMessage());
        }
    }

    /**
     * If the result of replacing URL using S3 properties is blank, throws a validation error.
     */
    @Test
    public void testExecuteJdbcWithS3PropertiesParamUrlBlankAfterReplace()
    {
        String s3BucketName = "test_bucket";
        String s3ObjectKey = "test_key";
        String content = "foo=";
        putS3Object(s3BucketName, s3ObjectKey, content);

        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setUrl("${foo}");
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation(s3BucketName, s3ObjectKey));

        try
        {
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC connection URL is required", e.getMessage());
        }
    }

    /**
     * If the result of replacing SQL using S3 properties is blank, throws a validation error.
     */
    @Test
    public void testExecuteJdbcWithS3PropertiesParamSqlBlankAfterReplace()
    {
        String s3BucketName = "test_bucket";
        String s3ObjectKey = "test_key";
        String content = "foo=";
        putS3Object(s3BucketName, s3ObjectKey, content);

        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql("${foo}");
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation(s3BucketName, s3ObjectKey));

        try
        {
            jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.fail("expected an IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "JDBC statement [0] SQL is required", e.getMessage());
        }
    }

    /**
     * Execute JDBC using S3 properties file. Unfortunately, not many assertions that can be done through the service layer. Asserts that no errors are thrown,
     * and that the response SQL does not expose the secrets.
     */
    @Test
    public void testExecuteJdbcWithS3PropertiesSuccess()
    {
        String s3BucketName = "test_bucket";
        String s3ObjectKey = "test_key";
        String content = "foo=bar";
        putS3Object(s3BucketName, s3ObjectKey, content);

        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getConnection().setUrl("test_url_${foo}");
        jdbcExecutionRequest.getConnection().setUsername("test_username_${foo}");
        jdbcExecutionRequest.getConnection().setPassword("test_password_${foo}");
        JdbcStatement jdbcStatement = jdbcExecutionRequest.getStatements().get(0);
        jdbcStatement.setSql("test_sql_${foo}");
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation(s3BucketName, s3ObjectKey));

        try
        {
            JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);
            Assert.assertEquals("jdbc execution response statement [0] sql", "test_sql_${foo}", jdbcExecutionResponse.getStatements().get(0).getSql());
        }
        catch (Exception e)
        {
            Assert.fail("unexpected exception was thrown. " + e);
        }
    }

    /**
     * Some JDBC exception messages echoes back parts of the SQL statement. This is problem for security if some of the variables were replaced, and may
     * accidentally expose secret information in the response error message. The application should mask any values given in the properties which exist in the
     * exception message.
     * <p/>
     * This test will use a SQL that will throw an exception, and the exception message is known. Then asserts that the value has been replaced with a mask in
     * the response error message.
     */
    @Test
    public void testExecuteJdbcSensitiveDataIsMaskedInErrorMessage()
    {
        String s3BucketName = "test_bucket";
        String s3ObjectKey = "test_key";
        String content = "foo=DataIntegrityViolationException";
        putS3Object(s3BucketName, s3ObjectKey, content);

        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql(MockJdbcOperations.CASE_2_SQL);
        jdbcExecutionRequest.setS3PropertiesLocation(new S3PropertiesLocation(s3BucketName, s3ObjectKey));

        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        Assert.assertEquals("jdbc execution response statement [0] error message", "java.sql.SQLException: test **** cause",
            jdbcExecutionResponse.getStatements().get(0).getErrorMessage());
    }

    /**
     * Puts an S3 object with the given parameters directly into S3.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3ObjectKey the S3 object key
     * @param content the content of the S3 object
     */
    private void putS3Object(String s3BucketName, String s3ObjectKey, String content)
    {
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, new ByteArrayInputStream(content.getBytes()), new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);
    }
}
