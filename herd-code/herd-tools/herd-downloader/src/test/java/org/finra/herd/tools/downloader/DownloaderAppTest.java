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
package org.finra.herd.tools.downloader;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import org.finra.herd.tools.common.databridge.DataBridgeApp;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for DownloaderApp class.
 */
public class DownloaderAppTest extends AbstractDownloaderTest
{
    private DownloaderApp downloaderApp = new DownloaderApp()
    {
        protected ApplicationContext createApplicationContext()
        {
            return applicationContext;
        }
    };

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        uploadAndRegisterTestData(S3_SIMPLE_TEST_PATH);
    }

    @Test
    public void testDownloaderApp() throws Exception
    {
        // Create the downloader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m",
                manifestFile.getPath(), "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME,
                "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true", "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testDownloaderAppHelpOptionSet() throws Exception
    {
        // Displaying help is a success condition.
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"-h"}, null, DataBridgeApp.ReturnValue.SUCCESS);
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"--help"}, null, DataBridgeApp.ReturnValue.SUCCESS);
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"-h", "-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY}, null, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testDownloaderAppInvalidArguments() throws Exception
    {
        // Run the uploader with no arguments which is invalid.
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {}, DownloaderApp.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppInvalidBooleanValueForDisableHostnameVerificationOption() throws Exception
    {
        // Set the SSL argument value to an invalid boolean value.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", STRING_VALUE,
                "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w",
                WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "INVALID_BOOLEAN_VALUE", "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail since SSL is enabled and password is not passed.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppInvalidBooleanValueForSslOption() throws Exception
    {
        // Set the SSL argument value to an invalid boolean value.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", STRING_VALUE,
                "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "INVALID_BOOLEAN_VALUE", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w",
                WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true", "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail since SSL is enabled and password is not passed.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppInvalidBooleanValueForTrustSelfSignedCertificateOption() throws Exception
    {
        // Set the SSL argument value to an invalid boolean value.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", STRING_VALUE,
                "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w",
                WEB_SERVICE_HTTPS_PASSWORD, "-C", "INVALID_BOOLEAN_VALUE", "-d", "true", "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail since SSL is enabled and password is not passed.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppInvalidS3Endpoint() throws Exception
    {
        // Create the downloader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", "INVALID_S3_ENDPOINT", "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m",
                manifestFile.getPath(), "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME,
                "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true", "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail with a IllegalArgumentException when AwsHostNameUtils is trying to parse a region name.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, new IllegalArgumentException());
    }

    @Test
    public void testDownloaderAppLongOptions() throws Exception
    {
        // Create the downloader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        final String[] args =
            new String[] {"--s3AccessKey", S3_ACCESS_KEY, "--s3SecretKey", S3_SECRET_KEY, "--s3Endpoint", S3_ENDPOINT_US_STANDARD, "--localPath",
                LOCAL_TEMP_PATH_OUTPUT.toString(), "--manifestPath", manifestFile.getPath(), "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort",
                WEB_SERVICE_HTTPS_PORT.toString(), "--ssl", "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD,
                "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true", "--httpProxyHost", HTTP_PROXY_HOST, "--httpProxyPort",
                HTTP_PROXY_PORT.toString()};

        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testDownloaderAppMissingOptionalParameters() throws Exception
    {
        // Create the downloader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        // Do not set optional parameters, except for HTTP proxy hostname and port - that also implies not enabling SSL.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", manifestFile.getPath(), "-H",
                WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_PORT.toString(), "-n", HTTP_PROXY_HOST, "-o", HTTP_PROXY_PORT.toString()};

        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testDownloaderAppSslDisabled() throws Exception
    {
        // Create the downloader manifest file.
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        // Make sure that we explicitly disable SSL.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-e", S3_ENDPOINT_US_STANDARD, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m",
                manifestFile.getPath(), "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_PORT.toString(), "-s", "false", "-n", HTTP_PROXY_HOST, "-o",
                HTTP_PROXY_PORT.toString()};

        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.SUCCESS);
    }

    @Test
    public void testDownloaderAppSslEnabledMissingPassword() throws Exception
    {
        // Set an SSL option without providing a password.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", STRING_VALUE, "-H", WEB_SERVICE_HOSTNAME,
                "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-C", "true", "-d", "true", "-n", HTTP_PROXY_HOST,
                "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail since SSL is enabled and password is not passed.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppSslEnabledMissingUsername() throws Exception
    {
        // Set an SSL option without providing a username.
        final String[] args =
            new String[] {"-a", S3_ACCESS_KEY, "-p", S3_SECRET_KEY, "-l", LOCAL_TEMP_PATH_OUTPUT.toString(), "-m", STRING_VALUE, "-H", WEB_SERVICE_HOSTNAME,
                "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true", "-n", HTTP_PROXY_HOST,
                "-o", HTTP_PROXY_PORT.toString()};

        // We are expecting this to fail since SSL is enabled and username is not passed.
        runDataBridgeAndCheckReturnValue(downloaderApp, args, DataBridgeWebClient.class, DataBridgeApp.ReturnValue.FAILURE);
    }

    @Test
    public void testDownloaderAppVersionOptionSet() throws Exception
    {
        // Displaying version information is a success condition.
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"-v"}, null, DataBridgeApp.ReturnValue.SUCCESS);
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"--version"}, null, DataBridgeApp.ReturnValue.SUCCESS);
        runDataBridgeAndCheckReturnValue(downloaderApp, new String[] {"-a", S3_ACCESS_KEY, "-v", "-p", S3_SECRET_KEY}, null, DataBridgeApp.ReturnValue.SUCCESS);
    }
}
