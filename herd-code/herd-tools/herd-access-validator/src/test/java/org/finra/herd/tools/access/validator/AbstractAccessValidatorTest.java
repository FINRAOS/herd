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
package org.finra.herd.tools.access.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.AbstractDataBridgeTest;
import org.finra.herd.tools.common.databridge.HttpErrorResponseException;

abstract class AbstractAccessValidatorTest extends AbstractDataBridgeTest
{
    static final Integer BUSINESS_OBJECT_DATA_VERSION = 123;

    static final String BUSINESS_OBJECT_DEFINITION_NAME = "testBusinessObjectDefinitionName";

    static final String BUSINESS_OBJECT_FORMAT_FILE_TYPE = "testBusinessObjectFormatFileType";

    static final String BUSINESS_OBJECT_FORMAT_USAGE = "testBusinessObjectFormatUsage";

    static final Integer BUSINESS_OBJECT_FORMAT_VERSION = 456;

    static final String HERD_BASE_URL = "testHerdBaseUrl";

    static final String HERD_PASSWORD = "testHerdPassword";

    static final String HERD_USERNAME = "testHerdUsername";

    static final String INVALID_PROPERTY = "testInvalidProperty";

    static final String NAMESPACE = "testNamespace";

    static final String PRIMARY_PARTITION_VALUE = "testPrimaryPartitionValue";

    static final String PROPERTIES_FILE_PATH = "test.properties";

    static final String SUB_PARTITION_VALUES = "testSubPartitionValues";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAccessValidatorTest.class);

    @Autowired
    ApplicationContext applicationContext;

    @After
    public void cleanEnv() throws IOException
    {
    }

    @Before
    public void setupEnv() throws IOException
    {
    }

    /**
     * Runs a application application  with the specified arguments and verifies that an expected exception will be thrown. An optional "no logging class" can
     * also be specified.
     *
     * @param accessValidatorApp the application
     * @param args the application arguments
     * @param expectedException an instance of an expected exception that should be thrown. If this is an instance of HttpErrorResponseException, then the
     * response status will also be compared
     *
     * @throws Exception if any errors were found during the execution of the application
     */
    void runApplicationAndCheckReturnValue(AccessValidatorApp accessValidatorApp, String[] args, Object expectedException) throws Exception
    {
        runApplicationAndCheckReturnValue(accessValidatorApp, args, null, expectedException);
    }

    /**
     * Runs a application application with the specified arguments and validates the response against an expected return value. An optional "no logging class"
     * can also be specified.
     *
     * @param accessValidatorApp the application
     * @param args the application arguments
     * @param expectedReturnValue the expected application return value or null if an exception is expected
     * @param expectedException an instance of an expected exception that should be thrown or null if no exception is expected. If this is null, then an
     * expected return value should be populated. If this is an instance of HttpErrorResponseException, then the response status will also be compared
     *
     * @throws Exception if any errors were found during the execution of the application
     */
    private void runApplicationAndCheckReturnValue(final AccessValidatorApp accessValidatorApp, final String[] args,
        final ToolsCommonConstants.ReturnValue expectedReturnValue, final Object expectedException) throws Exception
    {
        try
        {
            executeWithoutLogging((Class<?>) null, () -> {
                ToolsCommonConstants.ReturnValue returnValue = accessValidatorApp.go(args);
                if (expectedException != null)
                {
                    fail("Expected exception of class " + expectedException.getClass().getName() + " that was not thrown.");
                }
                else
                {
                    assertEquals(expectedReturnValue, returnValue);
                    assertEquals(expectedReturnValue.getReturnCode(), returnValue.getReturnCode());
                }
            });
        }
        catch (Exception ex)
        {
            if (expectedException != null)
            {
                if (!(ex.getClass().equals(expectedException.getClass())))
                {
                    LOGGER.error("Error running retention expiration destroyer app.", ex);
                    fail("Expected exception with class " + expectedException.getClass().getName() + ", but got an exception with class " +
                        ex.getClass().getName());
                }
                if (ex instanceof HttpErrorResponseException)
                {
                    // This will ensure the returned status code matches what we are expecting.
                    HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) ex;
                    HttpErrorResponseException expectedHttpErrorResponseException = (HttpErrorResponseException) expectedException;
                    assertTrue("Expecting HTTP response status of " + expectedHttpErrorResponseException.getStatusCode() + ", but got " +
                        httpErrorResponseException.getStatusCode(), expectedException.equals(httpErrorResponseException));
                }
            }
            else
            {
                // Throw the original exception, since we are not expecting any exception.
                throw ex;
            }
        }
    }
}
