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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DataTruncation;
import java.sql.SQLException;

import javax.persistence.PersistenceException;
import javax.xml.bind.UnmarshalException;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;

import org.activiti.engine.ActivitiClassLoadingException;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.impl.javax.el.ELException;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.xml.sax.SAXParseException;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.MethodNotAllowedException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ErrorInformation;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Test the herd REST Controller Advice.
 */
public class HerdErrorInformationExceptionHandlerTest extends AbstractServiceTest
{
    @Autowired
    private HerdErrorInformationExceptionHandler exceptionHandler;

    private static final String MESSAGE = "This is a test and is not an actual error. Please ignore.";

    @Test
    public void testHandleInternalServerError() throws Exception
    {
        // Calling handleException will log a stack trace which is normal so don't be concerned if you see it in the logs.
        validateErrorInformation(exceptionHandler.handleInternalServerErrorException(new Exception(MESSAGE)), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testHandleInternalServerErrorNoErrorMessage() throws Exception
    {
        // Get the error information for an exception that has no message. In this case, the message is the exception class name.
        ErrorInformation errorInformation = exceptionHandler.handleInternalServerErrorException(new NullPointerException());

        // Validate the error status information, but not the message.
        validateErrorInformation(errorInformation, HttpStatus.INTERNAL_SERVER_ERROR, false);

        // Validate that the error message is the class name.
        assertEquals(NullPointerException.class.getName(), errorInformation.getMessage());
    }

    @Test
    public void testDataTruncationException() throws Exception
    {
        validateErrorInformation(
            exceptionHandler.handlePersistenceException(getPersistenceException(new DataTruncation(1, true, true, 5, 10)), new MockHttpServletResponse()),
            HttpStatus.BAD_REQUEST, false);
    }

    @Test
    public void testDataTruncationExceptions() throws Exception
    {
        validatePersistenceException(HerdErrorInformationExceptionHandler.ORACLE_SQL_STATE_CODE_ERROR,
                HerdErrorInformationExceptionHandler.ORACLE_DATA_TOO_LARGE_ERROR_CODE);

        validatePersistenceException(HerdErrorInformationExceptionHandler.ORACLE_SQL_STATE_CODE_ERROR,
                HerdErrorInformationExceptionHandler.ORACLE_LONG_DATA_IN_LONG_COLUMN_ERROR_CODE);

        validatePersistenceException(HerdErrorInformationExceptionHandler.POSTGRES_SQL_STATE_CODE_TRUNCATION_ERROR, 0);
    }

    /**
     * Validates that calling handlePersistenceException with a SQL exception with the given SQL state code and error code wrapped in a
     * {@link PersistenceException} returns an {@link ErrorInformation} with a {@link HttpStatus#BAD_REQUEST}.
     *
     * @param sqlState - {@link SQLException#getSQLState()}
     * @param errorCode - {@link SQLException#getErrorCode()}
     */
    private void validatePersistenceException(String sqlState, int errorCode)
    {
        SQLException sqlException = new SQLException("Test Reason", sqlState, errorCode, null);
        PersistenceException persistenceException = getPersistenceException(sqlException);
        ErrorInformation errorInformation = exceptionHandler.handlePersistenceException(persistenceException, new MockHttpServletResponse());
        validateErrorInformation(errorInformation, HttpStatus.BAD_REQUEST, false);
    }

    @Test
    public void testConstraintViolationException() throws Exception
    {
        validateErrorInformation(exceptionHandler
                .handlePersistenceException(getPersistenceException(new ConstraintViolationException(MESSAGE, null, "testConstraint")),
                    new MockHttpServletResponse()), HttpStatus.BAD_REQUEST, false);

        validateErrorInformation(exceptionHandler.handlePersistenceException(getPersistenceException(new SQLException(MESSAGE,
                HerdErrorInformationExceptionHandler.POSTGRES_SQL_STATE_CODE_FOREIGN_KEY_VIOLATION, 0)), new MockHttpServletResponse()), HttpStatus.BAD_REQUEST,
                false);

        validateErrorInformation(exceptionHandler.handlePersistenceException(getPersistenceException(new SQLException(MESSAGE,
                HerdErrorInformationExceptionHandler.POSTGRES_SQL_STATE_CODE_UNIQUE_INDEX_OR_PRIMARY_KEY_VIOLATION, 0)), new MockHttpServletResponse()),
            HttpStatus.BAD_REQUEST, false);
    }

    @Test
    public void testConstraintViolationExceptionNoWrap() throws Exception
    {
        validateErrorInformation(
            exceptionHandler.handlePersistenceException(new ConstraintViolationException(MESSAGE, null, "testConstraint"), new MockHttpServletResponse()),
            HttpStatus.BAD_REQUEST, false);
    }

    @Test
    public void testPersistenceExceptionInternalServerError() throws Exception
    {
        validateErrorInformation(
            exceptionHandler.handlePersistenceException(getPersistenceException(new RuntimeException(MESSAGE)), new MockHttpServletResponse()),
            HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testPersistenceExceptionInternalServerErrorNullException() throws Exception
    {
        validateErrorInformation(exceptionHandler.handlePersistenceException(null, new MockHttpServletResponse()), HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

    @Test
    public void testHandleOperationNotAllowedFound() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleOperationNotAllowedException(new MethodNotAllowedException(MESSAGE)), HttpStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    public void testHandleNotFound() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleNotFoundException(new ObjectNotFoundException(MESSAGE)), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testAlreadyExists() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleConflictException(new AlreadyExistsException(MESSAGE)), HttpStatus.CONFLICT);
    }

    @Test
    public void testIllegalArgument() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleBadRequestException(new IllegalArgumentException(MESSAGE)), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testActivitiExceptionBadRequest() throws Exception
    {
        // Test out both ActivitiClassLoadingException and ELException.
        validateErrorInformation(exceptionHandler
            .handleActivitiException(new ActivitiClassLoadingException(ActivitiClassLoadingException.class.getName(), new RuntimeException(MESSAGE)),
                new MockHttpServletResponse()), HttpStatus.BAD_REQUEST, false);
        validateErrorInformation(exceptionHandler.handleActivitiException(new ELException(MESSAGE), new MockHttpServletResponse()), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testActivitiExceptionInternalServerError() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleActivitiException(new ActivitiException(MESSAGE), new MockHttpServletResponse()),
            HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

    @Test
    public void testAccessDeniedException() throws Exception
    {
        validateErrorInformation(exceptionHandler.handleAccessDeniedException(new AccessDeniedException(MESSAGE)), HttpStatus.FORBIDDEN, false);
    }

    @Test
    public void testIsReportableErrorThrowable() throws Exception
    {
        assertTrue(exceptionHandler.isReportableError(new Throwable(MESSAGE)));
    }

    @Test
    public void testGetErrorWithCause() throws Exception
    {
        Exception ex = new Exception();
        ErrorInformation errorInformation = exceptionHandler.handleBadRequestException(ex);
        assertTrue(errorInformation.getMessageDetails().size() == 0);

        ex = new Exception(new Exception("cause_1_exception", new Exception("cause_2_exception")));
        errorInformation = exceptionHandler.handleBadRequestException(ex);

        assertTrue(errorInformation.getMessageDetails().size() == 2);
        assertEquals("cause_1_exception", errorInformation.getMessageDetails().get(0));
        assertEquals("cause_2_exception", errorInformation.getMessageDetails().get(1));
    }

    @Test
    public void testHttpMessageNotReadableExceptionRootCauseIsSaxParseException()
    {
        // XML parser exception
        String rootErrorMessage = "root error message";
        HttpMessageNotReadableException ex =
            new HttpMessageNotReadableException("errorMessage", new UnmarshalException("cause_2_exception", new SAXParseException(rootErrorMessage, null)));
        ErrorInformation errorInformation = exceptionHandler.handleHttpMessageNotReadableException(ex);

        assertEquals(0, errorInformation.getMessageDetails().size());
        assertEquals(rootErrorMessage, errorInformation.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST.value(), errorInformation.getStatusCode());
        assertEquals(HttpStatus.BAD_REQUEST.getReasonPhrase(), errorInformation.getStatusDescription());
    }

    @Test
    public void testHttpMessageNotReadableExceptionWithCause()
    {
        // JSON parse exception
        String originalErrorMessage = "original error Message";
        HttpMessageNotReadableException ex = new HttpMessageNotReadableException(originalErrorMessage, new JsonMappingException(null, "cause_1_exception"));
        // validate original message is wrapped in Spring HttpMessageNotReadableException
        assertEquals(originalErrorMessage + "; nested exception is com.fasterxml.jackson.databind.JsonMappingException: cause_1_exception", ex.getMessage());

        ErrorInformation errorInformation = exceptionHandler.handleHttpMessageNotReadableException(ex);
        assertEquals(0, errorInformation.getMessageDetails().size());
        assertEquals(originalErrorMessage, errorInformation.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST.value(), errorInformation.getStatusCode());
        assertEquals(HttpStatus.BAD_REQUEST.getReasonPhrase(), errorInformation.getStatusDescription());
    }

    @Test
    public void testHttpMessageNotReadableExceptionWithoutCause()
    {
        // clean error Message without cause exception
        HttpMessageNotReadableException ex = new HttpMessageNotReadableException("errorMessage");
        ErrorInformation errorInformation = exceptionHandler.handleHttpMessageNotReadableException(ex);
        assertEquals(0, errorInformation.getMessageDetails().size());
        assertEquals("errorMessage", errorInformation.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST.value(), errorInformation.getStatusCode());
        assertEquals(HttpStatus.BAD_REQUEST.getReasonPhrase(), errorInformation.getStatusDescription());
    }

    /**
     * Validate the error information by checking the message, the error status code, and the error status description.
     *
     * @param errorInformation the error information to validate.
     * @param expectedStatus the expected status.
     */
    private void validateErrorInformation(ErrorInformation errorInformation, HttpStatus expectedStatus)
    {
        validateErrorInformation(errorInformation, expectedStatus, true);
    }

    /**
     * Gets a new persistence exception that wraps the passed in child exception.
     *
     * @param childException the child exception.
     *
     * @return the persistence exception.
     */
    private PersistenceException getPersistenceException(Exception childException)
    {
        return new PersistenceException("Persistence Error", childException);
    }

    /**
     * Validate the error information by checking the message, the error status code, and the error status description.
     *
     * @param errorInformation the error information to validate.
     * @param expectedStatus the expected status.
     * @param checkMessage If true, the message will be checked. Otherwise, it won't.
     */
    private void validateErrorInformation(ErrorInformation errorInformation, HttpStatus expectedStatus, boolean checkMessage)
    {
        if (checkMessage)
        {
            assertEquals(MESSAGE, errorInformation.getMessage());
        }
        assertEquals(expectedStatus.value(), errorInformation.getStatusCode());
        assertEquals(expectedStatus.getReasonPhrase(), errorInformation.getStatusDescription());
    }
}
