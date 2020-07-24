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
package org.finra.herd.tools.retention.destroyer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.springframework.context.ApplicationContext;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

public class RetentionExpirationDestroyerAppTest extends AbstractRetentionExpirationDestroyerTest
{
    @Rule
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private RetentionExpirationDestroyerApp exporterApp = new RetentionExpirationDestroyerApp()
    {
        protected ApplicationContext createApplicationContext()
        {
            return applicationContext;
        }
    };

    @Test
    public void testGoInvalidDisableHostnameVerificationValue() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "INVALID_BOOLEAN_VALUE"};

        runApplicationAndCheckReturnValue(exporterApp, arguments, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidSslValue() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "INVALID_BOOLEAN_VALUE", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate",
                "true", "--disableHostnameVerification", "true"};

        runApplicationAndCheckReturnValue(exporterApp, arguments, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidTrustSelfSignedCertificateValue() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate",
                "INVALID_BOOLEAN_VALUE", "--disableHostnameVerification", "true"};

        runApplicationAndCheckReturnValue(exporterApp, arguments, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoMissingOptionalParameters() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString()};

        // We are expecting this to fail with an FileNotFoundException.
        runApplicationAndCheckReturnValue(exporterApp, arguments, new FileNotFoundException());
    }

    @Test
    public void testGoSuccess() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an FileNotFoundException.
        runApplicationAndCheckReturnValue(exporterApp, arguments, new FileNotFoundException());
    }

    @Test
    public void testParseCommandLineArgumentsHelpOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--help"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, exporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        assertTrue("Incorrect usage information returned.", output.startsWith("usage: " + RetentionExpirationDestroyerApp.APPLICATION_NAME));
    }

    @Test
    public void testGoSuccessSslTrueEnvPassword() throws Exception
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        // Set an SSL option with env password variable
        String[] arguments =
            {"-i", LOCAL_INPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME,
                "-env", "true", "-C", "true", "-d", "true"};
        // We are expecting this to fail with an FileNotFoundException.
        runApplicationAndCheckReturnValue(exporterApp, arguments, new FileNotFoundException());
        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void testGoSuccessSslTrueCliAndEnvPassword() throws Exception
    {
        environmentVariables.set("HERD_PASSWORD", "");
        // CLI Password being used
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "-env", "true", "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};
        // We are expecting this to fail with an FileNotFoundException.
        runApplicationAndCheckReturnValue(exporterApp, arguments, new FileNotFoundException());

        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        // ENV Password being used
        String[] argumentsUsingEnvPassword =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", "", "-env", "true", "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};
        // We are expecting this to fail with an FileNotFoundException.
        runApplicationAndCheckReturnValue(exporterApp, argumentsUsingEnvPassword, new FileNotFoundException());
        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void testParseCommandLineArgumentsInvalidRegServerPort() throws Exception
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", "INVALID_INTEGER", "--ssl", "true", "--username",
                WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
                "true"};

        // We are expecting this to fail with a NumberFormatException.
        runApplicationAndCheckReturnValue(exporterApp, arguments, new NumberFormatException());
    }

    @Test
    public void testParseCommandLineArgumentsNone()
    {
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, exporterApp.parseCommandLineArguments(new String[] {}, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoPassword()
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoUsername()
    {
        String[] arguments =
            {"--localInputFile", LOCAL_INPUT_FILE, "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--ssl",
                "true", "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueNoPassword()
    {
        String[] arguments =
            {"-i", LOCAL_INPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-C",
                "true", "-d", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueEnableEnvFalse()
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        String[] arguments =
            {"-i", LOCAL_INPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME,
                "-env", "false", "-C", "true", "-d", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueEnvPassword()
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        String[] arguments =
            {"-i", LOCAL_INPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME,
                "-env", "true", "-C", "true", "-d", "true"};
        assertNull(exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsVersionOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--version"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, exporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);

        assertEquals("output", String
            .format(DataBridgeApp.BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(), buildInformation.getBuildOs(),
                buildInformation.getBuildUser()), output);
    }

    @Test
    public void testParseShortCommandLineArgumentsSuccess()
    {
        String[] arguments =
            {"-i", LOCAL_INPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w",
                WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true"};
        assertNull(exporterApp.parseCommandLineArguments(arguments, applicationContext));
    }
}
