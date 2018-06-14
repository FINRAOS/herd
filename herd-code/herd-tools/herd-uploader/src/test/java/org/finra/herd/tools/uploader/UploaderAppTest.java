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
package org.finra.herd.tools.uploader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;
import org.springframework.context.ApplicationContext;

import org.finra.herd.tools.common.databridge.DataBridgeApp;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for UploaderApp class.
 */
public class UploaderAppTest extends AbstractUploaderTest
{
    private UploaderApp uploaderApp = new UploaderApp()
    {
        protected ApplicationContext createApplicationContext()
        {
            return applicationContext;
        }
    };

    @Test
    public void testGoInvalidDisableHostnameVerificationValue() throws Exception
    {
        String[] arguments = {"--s3AccessKey", S3_ACCESS_KEY, "--s3SecretKey", S3_SECRET_KEY, "--s3Endpoint", S3_ENDPOINT_US_STANDARD, "--localPath",
            LOCAL_TEMP_PATH_INPUT.toString(), "--manifestPath", STRING_VALUE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort",
            WEB_SERVICE_HTTPS_PORT.toString(), "--httpProxyHost", HTTP_PROXY_HOST, "--httpProxyPort", HTTP_PROXY_PORT.toString(), "--ssl", "true", "-u",
            WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
            "INVALID_BOOLEAN_VALUE"};

        // We are expecting this to fail with a NullPointerException when AwsHostNameUtils is trying to parse a region name.
        runDataBridgeAndCheckReturnValue(uploaderApp, arguments, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidSslValue() throws Exception
    {
        String[] arguments = {"--s3AccessKey", S3_ACCESS_KEY, "--s3SecretKey", S3_SECRET_KEY, "--s3Endpoint", S3_ENDPOINT_US_STANDARD, "--localPath",
            LOCAL_TEMP_PATH_INPUT.toString(), "--manifestPath", STRING_VALUE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort",
            WEB_SERVICE_HTTPS_PORT.toString(), "--httpProxyHost", HTTP_PROXY_HOST, "--httpProxyPort", HTTP_PROXY_PORT.toString(), "--ssl",
            "INVALID_BOOLEAN_VALUE", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
            "--disableHostnameVerification", "true"};

        // We are expecting this to fail with a NullPointerException when AwsHostNameUtils is trying to parse a region name.
        runDataBridgeAndCheckReturnValue(uploaderApp, arguments, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidTrustSelfSignedCertificateValue() throws Exception
    {
        String[] arguments = {"--s3AccessKey", S3_ACCESS_KEY, "--s3SecretKey", S3_SECRET_KEY, "--s3Endpoint", S3_ENDPOINT_US_STANDARD, "--localPath",
            LOCAL_TEMP_PATH_INPUT.toString(), "--manifestPath", STRING_VALUE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort",
            WEB_SERVICE_HTTPS_PORT.toString(), "--httpProxyHost", HTTP_PROXY_HOST, "--httpProxyPort", HTTP_PROXY_PORT.toString(), "--ssl", "true", "-u",
            WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "INVALID_BOOLEAN_VALUE",
            "--disableHostnameVerification", "true"};

        // We are expecting this to fail with a NullPointerException when AwsHostNameUtils is trying to parse a region name.
        runDataBridgeAndCheckReturnValue(uploaderApp, arguments, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testGoSuccess() throws Exception
    {
        // Create local test data files.
        createTestDataFiles(LOCAL_TEMP_PATH_INPUT, testManifestFiles);

        // Create the uploader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestUploaderInputManifestDto());

        String[] arguments =
            {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_INPUT.toString(), "-m", manifestFile.getPath(),
                "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString(), "-s", "true",
                "-u", WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true"};

        // We are expecting this to fail with a NullPointerException when AwsHostNameUtils is trying to parse a region name.
        runDataBridgeAndCheckReturnValue(uploaderApp, arguments, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testParseCommandLineArgumentsInvalidMaxRetryAttempts() throws Exception
    {
        String[] arguments =
            {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_INPUT.toString(), "-m", STRING_VALUE, "-H",
                WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString(), "-R",
                "INVALID_INTEGER"};
        assertEquals(DataBridgeApp.ReturnValue.FAILURE, uploaderApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsNone() throws Exception
    {
        assertEquals(DataBridgeApp.ReturnValue.FAILURE, uploaderApp.parseCommandLineArguments(new String[] {}, applicationContext));
    }

    @Test
    public void testParseShortCommandLineArgumentsSuccess() throws Exception
    {
        String[] arguments =
            {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_INPUT.toString(), "-m", STRING_VALUE, "-H",
                WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString(), "-s", "true", "-u",
                WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true"};
        assertNull(uploaderApp.parseCommandLineArguments(arguments, applicationContext));
    }
}
