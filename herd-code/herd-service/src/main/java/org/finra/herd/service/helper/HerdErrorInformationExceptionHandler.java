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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import javax.servlet.http.HttpServletResponse;

import org.activiti.engine.ActivitiClassLoadingException;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.impl.javax.el.ELException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.quartz.ObjectAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.MethodNotAllowedException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ErrorInformation;

/**
 * A class that handles various types of exceptions and returns summary error information containing the HTTP status, HTTP status description, and the error
 * message that provides details about the error. Note that all "ExceptionHandler" annotated method that take additional parameters besides the exception itself
 * should ensure that the method can handle cases when the other parameters are null. This is due to the isReportableError method which will invoke the
 * exception handler methods to get the error information which is needed to determine if the exception is reportable.
 */
@Component
public class HerdErrorInformationExceptionHandler
{
    /**
     * The Oracle database specific error code for data too large.
     */
    public static final int ORACLE_DATA_TOO_LARGE_ERROR_CODE = 12899;

    /**
     * The Oracle database specific error code for "can bind a LONG value only for insert into a LONG column". This could happen when the user enters a value
     * that is > 4000 bytes for a VARCHAR.
     */
    public static final int ORACLE_LONG_DATA_IN_LONG_COLUMN_ERROR_CODE = 1461;

    /**
     * Oracle specific SQL state code for generic SQL statement execution errors. https://docs.oracle.com/cd/E15817_01/appdev.111/b31228/appd.htm
     */
    public static final String ORACLE_SQL_STATE_CODE_ERROR = "72000";

    public static final String POSTGRES_SQL_STATE_CODE_FOREIGN_KEY_VIOLATION = "23503";

    public static final String POSTGRES_SQL_STATE_CODE_UNIQUE_INDEX_OR_PRIMARY_KEY_VIOLATION = "23505";

    /**
     * PostgreSQL specific SQL state code for string data truncation errors. http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
     */
    public static final String POSTGRES_SQL_STATE_CODE_TRUNCATION_ERROR = "22001";

    private static final Logger LOGGER = LoggerFactory.getLogger(HerdErrorInformationExceptionHandler.class);

    // A flag that determines whether this class will log errors or not.
    // When using the isReportableError method of this class, we typically don't want to enable logging which is why we're defaulting it to false.
    // When using the class as a normal exception handler (e.g. via a ControllerAdvice bean that extends this class), we typically want to enable logging.
    private boolean loggingEnabled = false;

    // An exception handler method resolver that will resolve exception handling methods based on exceptions.
    @Autowired
    private ExceptionHandlerMethodResolver resolver;

    /**
     * Handle access denied exceptions.
     *
     * @param exception the exception.
     *
     * @return the error information.
     */
    @ExceptionHandler(value = AccessDeniedException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ResponseBody
    public ErrorInformation handleAccessDeniedException(Exception exception)
    {
        return getErrorInformation(HttpStatus.FORBIDDEN, exception);
    }

    /**
     * Handle Activiti exceptions. Note that this method properly handles a null response being passed in.
     *
     * @param exception the exception.
     * @param response the response.
     *
     * @return the error information.
     */
    @ExceptionHandler(value = ActivitiException.class)
    @ResponseBody
    public ErrorInformation handleActivitiException(Exception exception, HttpServletResponse response)
    {
        if ((ExceptionUtils.indexOfThrowable(exception, ActivitiClassLoadingException.class) != -1) ||
            (ExceptionUtils.indexOfType(exception, ELException.class) != -1))
        {
            // These exceptions are caused by invalid workflow configurations (i.e. user error) so they are considered a bad request.
            return getErrorInformationAndSetStatus(HttpStatus.BAD_REQUEST, exception, response);
        }
        else
        {
            // For all other exceptions, something is wrong that we weren't expecting so we'll return this as an internal server error and log the error.
            logError("An Activiti error occurred.", exception);
            return getErrorInformationAndSetStatus(HttpStatus.INTERNAL_SERVER_ERROR, exception, response);
        }
    }

    /**
     * Handle exceptions that result in a "bad request" status.
     */
    @ExceptionHandler(value = {IllegalArgumentException.class, HttpMessageNotReadableException.class, MissingServletRequestParameterException.class,
        TypeMismatchException.class, UnsupportedEncodingException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public ErrorInformation handleBadRequestException(Exception exception)
    {
        return getErrorInformation(HttpStatus.BAD_REQUEST, exception);
    }

    /**
     * Handle exceptions that result in a "conflict" status.
     */
    @ExceptionHandler(value = {AlreadyExistsException.class, ObjectAlreadyExistsException.class, OptimisticLockException.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    @ResponseBody
    public ErrorInformation handleConflictException(Exception exception)
    {
        return getErrorInformation(HttpStatus.CONFLICT, exception);
    }

    /**
     * Handle exceptions that result in an "internal server error" status.
     */
    @ExceptionHandler(value = Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public ErrorInformation handleInternalServerErrorException(Exception exception)
    {
        logError("A general error occurred.", exception);
        return getErrorInformation(HttpStatus.INTERNAL_SERVER_ERROR, exception);
    }

    /**
     * Handle exceptions that result in a "not found" status.
     */
    @ExceptionHandler(value = {org.hibernate.ObjectNotFoundException.class, ObjectNotFoundException.class})
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    public ErrorInformation handleNotFoundException(RuntimeException exception)
    {
        return getErrorInformation(HttpStatus.NOT_FOUND, exception);
    }

    /**
     * Handle exceptions that result in a "operation not allowed" status.
     */
    @ExceptionHandler(value = MethodNotAllowedException.class)
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    @ResponseBody
    public ErrorInformation handleOperationNotAllowedException(RuntimeException exception)
    {
        return getErrorInformation(HttpStatus.METHOD_NOT_ALLOWED, exception);
    }

    /**
     * Handle persistence exceptions thrown by handlers. Note that this method properly handles a null response being passed in.
     *
     * @param exception the exception
     * @param response the HTTP servlet response.
     *
     * @return the error information.
     */
    @ExceptionHandler(value = {JpaSystemException.class, PersistenceException.class})
    @ResponseBody
    public ErrorInformation handlePersistenceException(Exception exception, HttpServletResponse response)
    {
        // Persistence exceptions typically wrap the cause which is what we're interested in to know what specific problem happened so get the root
        // exception.
        Throwable throwable = getRootCause(exception);

        if (isDataTruncationException(throwable))
        {
            // This is because the data being inserted was too large for a specific column in the database. When this happens, it will be due to a bad request.
            // Data truncation exceptions are thrown when we insert data that is too big for the column definition in MySQL.
            // On the other hand, Oracle throws only a generic JDBC exception, but has an error code we can check.
            // An alternative to using this database specific approach would be to define column lengths on the entities (e.g. @Column(length = 50))
            // which should throw a consistent exception by JPA that could be caught here. The draw back to using this approach is that need to custom
            // configure all column widths for all fields and keep that in sync with our DDL.
            return getErrorInformationAndSetStatus(HttpStatus.BAD_REQUEST, throwable, response);
        }
        else if (isCausedByConstraintViolationException(exception))
        {
            // A constraint violation exception will not typically be the root exception, but some exception in the chain. It is thrown when we try
            // to perform a database operation that violated a constraint (e.g. trying to delete a record that still has references to foreign keys
            // that exist, trying to insert duplicate keys, etc.). We are using ExceptionUtils to see if it exists somewhere in the chain.
            return getErrorInformationAndSetStatus(HttpStatus.BAD_REQUEST, new Exception("A constraint has been violated. Reason: " + throwable.getMessage()),
                response);
        }
        else
        {
            // For all other persistence exceptions, something is wrong that we weren't expecting so we'll return this as an internal server error.
            logError("A persistence error occurred.", exception);
            return getErrorInformationAndSetStatus(HttpStatus.INTERNAL_SERVER_ERROR, throwable == null ? new Exception("General Error") : throwable, response);
        }
    }

    public boolean isLoggingEnabled()
    {
        return loggingEnabled;
    }

    public void setLoggingEnabled(boolean loggingEnabled)
    {
        this.loggingEnabled = loggingEnabled;
    }

    /**
     * Returns whether the specified exception is one that should be reported (i.e. a support team should be notified in some way).
     *
     * @param exception the exception to analyze.
     *
     * @return true if the exception is reportable or false if not.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean isReportableError(Throwable exception)
    {
        // By default, the exception is reportable (i.e. the safe route).
        boolean isReportable = true;

        // Only proceed if we have an exception (as opposed to another Throwable) since the exception resolver only works off "exceptions".
        if (exception instanceof Exception)
        {
            // Try to resolve the exception which should yield a method on our exception handler (i.e. this class).
            Method method = resolver.resolveMethod((Exception) exception);

            // Only proceed if we found a valid method and it returns error information. Error information is needed to make the determination
            // whether or not the exception is reportable.
            if ((method != null) && (ErrorInformation.class.isAssignableFrom(method.getReturnType())))
            {
                // Create a list of parameters we will need to pass to the method being invoking.
                List<Object> parameterValues = new ArrayList<>();

                // Get the method parameters as an array of "classes".
                Class[] parameterTypes = method.getParameterTypes();

                // Loop through the method class parameter types and add a parameter value for each one.
                // The parameter will be the exception itself or null for all other cases. Note that we need to ensure that if the handler method takes
                // additional parameters besides exceptions (i.e. the ones we will pass null), that the handler method will "handle" the null case
                // and not throw a null pointer exception.
                for (Class clazz : parameterTypes)
                {
                    if (clazz.isAssignableFrom(exception.getClass()))
                    {
                        // The parameter class is assignable from the actual exception so pass the exception itself.
                        parameterValues.add(exception);
                    }
                    else
                    {
                        // The parameter class is something else so pass null.
                        parameterValues.add(null);
                    }
                }

                try
                {
                    // Invoke the handler method specific to our exception and get the error information back.
                    ErrorInformation errorInformation = (ErrorInformation) method.invoke(this, parameterValues.toArray());

                    // The only error information status that is reportable is "internal server error" so set the flag to false for all other cases.
                    if (errorInformation.getStatusCode() != HttpStatus.INTERNAL_SERVER_ERROR.value())
                    {
                        isReportable = false;
                    }
                }
                catch (IllegalAccessException | InvocationTargetException ex)
                {
                    logError("Unable to invoke method \"" + method.getDeclaringClass().getName() + "." + method.getName() +
                        "\" so couldn't determine if exception is reportable. Defaulting to true.", ex);
                }
            }
        }

        // Return whether the error should be reported.
        return isReportable;
    }

    /**
     * Gets a new error information based on the specified message.
     *
     * @param httpStatus the status of the error.
     * @param exception the exception whose message will be used.
     *
     * @return the error information.
     */
    private ErrorInformation getErrorInformation(HttpStatus httpStatus, Throwable exception)
    {
        ErrorInformation errorInformation = new ErrorInformation();
        errorInformation.setStatusCode(httpStatus.value());
        errorInformation.setStatusDescription(httpStatus.getReasonPhrase());
        String errorMessage = exception.getMessage();
        if (StringUtils.isEmpty(errorMessage))
        {
            errorMessage = exception.getClass().getName();
        }
        errorInformation.setMessage(errorMessage);

        List<String> messageDetails = new ArrayList<>();

        Throwable causeException = exception.getCause();
        while (causeException != null)
        {
            messageDetails.add(causeException.getMessage());
            causeException = causeException.getCause();
        }

        errorInformation.setMessageDetails(messageDetails);
        return errorInformation;
    }

    /**
     * Gets a new error information based on the specified message and sets the HTTP status on the HTTP response.
     *
     * @param httpStatus the status of the error.
     * @param exception the exception whose message will be used.
     * @param response the optional HTTP response that will have its status set from the specified httpStatus.
     *
     * @return the error information.
     */
    private ErrorInformation getErrorInformationAndSetStatus(HttpStatus httpStatus, Throwable exception, HttpServletResponse response)
    {
        // Set the status one response if one was passed in.
        if (response != null)
        {
            response.setStatus(httpStatus.value());
        }

        // Get the error information based on the status and error message.
        return getErrorInformation(httpStatus, exception);
    }

    /**
     * Gets the root cause of the given exception. If the given exception does not have any causes (that is, is already root), returns the given exception.
     *
     * @param throwable - the exception to get the root cause
     *
     * @return the root cause exception
     */
    private Throwable getRootCause(Exception throwable)
    {
        Throwable rootThrowable = ExceptionUtils.getRootCause(throwable);
        if (rootThrowable == null)
        {
            // Use the original exception if there are no causes.
            rootThrowable = throwable;
        }
        return rootThrowable;
    }

    /**
     * Returns {@code true} if the given throwable is or is not caused by a database constraint violation.
     *
     * @param exception - throwable to check.
     *
     * @return {@code true} if is constraint violation, {@code false} otherwise.
     */
    private boolean isCausedByConstraintViolationException(Exception exception)
    {
        // some databases will throw ConstraintViolationException
        boolean isConstraintViolation = ExceptionUtils.indexOfThrowable(exception, ConstraintViolationException.class) != -1;

        // other databases will not throw a nice exception
        if (!isConstraintViolation)
        {
            // We must manually check the error codes
            Throwable rootThrowable = getRootCause(exception);
            if (rootThrowable instanceof SQLException)
            {
                SQLException sqlException = (SQLException) rootThrowable;
                if (POSTGRES_SQL_STATE_CODE_FOREIGN_KEY_VIOLATION.equals(sqlException.getSQLState())
                    || POSTGRES_SQL_STATE_CODE_UNIQUE_INDEX_OR_PRIMARY_KEY_VIOLATION.equals(sqlException.getSQLState()))
                {
                    isConstraintViolation = true;
                }
            }
        }
        return isConstraintViolation;
    }

    /**
     * Returns {@code true} if the given throwable is a data truncation exception. This method does not check the causes of the given throwable.
     * <p/>
     * This method will check the status codes and error codes of the underlying {@link SQLException}.
     *
     * @param throwable - throwable to check
     *
     * @return {@code true} if error is data truncation error, {@code false} otherwise.
     */
    private boolean isDataTruncationException(Throwable throwable)
    {
        boolean isDataTruncationException = false;
        // Exception must be a SQLException
        if (throwable instanceof SQLException)
        {
            SQLException sqlException = (SQLException) throwable;

            if (sqlException instanceof DataTruncation)
            {
                // Some drivers throw nice data truncation errors (e.g. MySQL).
                isDataTruncationException = true;
            }
            else
            {
                // If drivers don't throw nice errors, we need to examine error codes.

                // Check SQL state first to see what kind of error it is.
                switch (sqlException.getSQLState())
                {
                    // Oracle depends on error codes.
                    case ORACLE_SQL_STATE_CODE_ERROR:
                        switch (sqlException.getErrorCode())
                        {
                            // Oracle throws different error codes depending on whether the length was <= 4000 or not
                            case ORACLE_DATA_TOO_LARGE_ERROR_CODE:
                            case ORACLE_LONG_DATA_IN_LONG_COLUMN_ERROR_CODE:
                                isDataTruncationException = true;
                                break;

                            // In all other cases, assume it is not a data truncation exception.
                            default:
                                isDataTruncationException = false;
                                break;
                        }
                        break;

                    // Postgres does not use error codes.
                    case POSTGRES_SQL_STATE_CODE_TRUNCATION_ERROR:
                        isDataTruncationException = true;
                        break;

                    // In all other cases, assume it is not a data truncation exception.
                    default:
                        isDataTruncationException = false;
                        break;
                }
            }
        }
        return isDataTruncationException;
    }

    /**
     * Logs an error if logging is enabled. Otherwise, no logging is performed.
     *
     * @param message the message to log.
     * @param exception the exception to log along with the message.
     */
    protected void logError(String message, Exception exception)
    {
        if (isLoggingEnabled())
        {
            LOGGER.error(message, exception);
        }
    }
}
