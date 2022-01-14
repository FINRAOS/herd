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
package org.finra.herd.tools.retention.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Paths;

import org.joda.time.DateTime;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.Command;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.AbstractDataBridgeTest;
import org.finra.herd.tools.common.databridge.HttpErrorResponseException;

/**
 * This is an abstract base class that provides useful methods for herd retention expiration tool test drivers.
 */
public abstract class AbstractExporterTest extends AbstractDataBridgeTest
{
    static final Integer BUSINESS_OBJECT_DATA_VERSION = 5;

    static final String BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME = "testBusinessObjectDefinitionDisplayName";

    static final String BUSINESS_OBJECT_DEFINITION_NAME = "testBusinessObjectDefinitionName";

    static final String BUSINESS_OBJECT_FORMAT_FILE_TYPE = "testBusinessObjectFormatFileType";

    static final String BUSINESS_OBJECT_FORMAT_USAGE = "testBusinessObjectFormatUsage";

    static final Integer BUSINESS_OBJECT_FORMAT_VERSION = 9;

    static final DateTime END_REGISTRATION_DATE_TIME = new DateTime(getRandomDate());

    static final String LOCAL_OUTPUT_FILE = Paths.get(LOCAL_TEMP_PATH_OUTPUT.toString(), LOCAL_FILE).toString();

    static final String NAMESPACE = "testNamespace";

    static final DateTime START_REGISTRATION_DATE_TIME = new DateTime(getRandomDate());

    static final String UDC_SERVICE_HOSTNAME = "testUdcHostname";

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractExporterTest.class);

    /**
     * Provide easy access to the controller for all test methods.
     */
    @Autowired
    RetentionExpirationExporterController retentionExpirationExporterController;

    /**
     * Provide easy access to the web client for all test methods.
     */
    @Autowired
    RetentionExpirationExporterWebClient retentionExpirationExporterWebClient;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        super.setupEnv();

        // Initialize the uploader web client instance.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).build();
        retentionExpirationExporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);
    }

    /**
     * Runs a application application with the specified arguments and validates the response against an expected return value. An optional "no logging class"
     * can also be specified.
     *
     * @param retentionExpirationExporterApp the application
     * @param args the application arguments
     * @param noLoggingClass an optional class that will have logging turned off
     * @param expectedReturnValue the expected application return value
     *
     * @throws Exception if any errors were found during the execution of the application
     */
    void runApplicationAndCheckReturnValue(RetentionExpirationExporterApp retentionExpirationExporterApp, String[] args, Class<?> noLoggingClass,
        ToolsCommonConstants.ReturnValue expectedReturnValue) throws Exception
    {
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, args, noLoggingClass, expectedReturnValue, null);
    }

    /**
     * Runs a application application  with the specified arguments and verifies that an expected exception will be thrown. An optional "no logging class" can
     * also be specified.
     *
     * @param retentionExpirationExporterApp the application
     * @param args the application arguments
     * @param noLoggingClass an optional class that will have logging turned off
     * @param expectedException an instance of an expected exception that should be thrown. If this is an instance of HttpErrorResponseException, then the
     * response status will also be compared
     *
     * @throws Exception if any errors were found during the execution of the application
     */
    void runApplicationAndCheckReturnValue(RetentionExpirationExporterApp retentionExpirationExporterApp, String[] args, Class<?> noLoggingClass,
        Object expectedException) throws Exception
    {
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, args, noLoggingClass, null, expectedException);
    }

    /**
     * Runs a application application with the specified arguments and validates the response against an expected return value. An optional "no logging class"
     * can also be specified.
     *
     * @param dataBridgeApp the application
     * @param args the application arguments
     * @param noLoggingClass an optional class that will have logging turned off
     * @param expectedReturnValue the expected application return value or null if an exception is expected
     * @param expectedException an instance of an expected exception that should be thrown or null if no exception is expected. If this is null, then an
     * expected return value should be populated. If this is an instance of HttpErrorResponseException, then the response status will also be compared
     *
     * @throws Exception if any errors were found during the execution of the application
     */
    private void runApplicationAndCheckReturnValue(final RetentionExpirationExporterApp dataBridgeApp, final String[] args, Class<?> noLoggingClass,
        final ToolsCommonConstants.ReturnValue expectedReturnValue, final Object expectedException) throws Exception
    {
        try
        {
            executeWithoutLogging(noLoggingClass, new Command()
            {
                @Override
                public void execute() throws Exception
                {
                    ToolsCommonConstants.ReturnValue returnValue = dataBridgeApp.go(args);
                    if (expectedException != null)
                    {
                        fail("Expected exception of class " + expectedException.getClass().getName() + " that was not thrown.");
                    }
                    else
                    {
                        assertEquals(expectedReturnValue, returnValue);
                        assertEquals(expectedReturnValue.getReturnCode(), returnValue.getReturnCode());
                    }
                }
            });
        }
        catch (Exception ex)
        {
            if (expectedException != null)
            {
                if (!(ex.getClass().equals(expectedException.getClass())))
                {
                    LOGGER.error("Error running Data Bridge.", ex);
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
