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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.ConnectException;

import com.sun.jersey.api.client.ClientHandlerException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.springframework.context.ApplicationContext;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

public class RetentionExpirationExporterAppTest extends AbstractExporterTest
{
    @Rule
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private RetentionExpirationExporterApp retentionExpirationExporterApp = new RetentionExpirationExporterApp()
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
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "INVALID_BOOLEAN_VALUE"};

        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidSslValue() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "INVALID_BOOLEAN_VALUE", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate",
                "true", "--disableHostnameVerification", "true"};

        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoInvalidTrustSelfSignedCertificateValue() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate",
                "INVALID_BOOLEAN_VALUE", "--disableHostnameVerification", "true"};

        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testGoSuccess() throws Exception
    {
        String[] arguments = {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--startRegistrationDateTime",
            START_REGISTRATION_DATE_TIME_AS_TEXT, "--endRegistrationDateTime", END_REGISTRATION_DATE_TIME_AS_TEXT, "--localOutputFile", LOCAL_OUTPUT_FILE,
            "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
            "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
            "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new ClientHandlerException());
    }

    @Test
    public void testGoSuccessOAuth() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "-T", WEB_SERVICE_HTTPS_ACCESS_TOKEN_URL,
                "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new ConnectException());
    }

    @Test
    public void testGoSuccessSslTrueCliAndEnvPassword() throws Exception
    {
        environmentVariables.set("HERD_PASSWORD", "");
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "-E", "true", "--trustSelfSignedCertificate",
                "true", "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new ClientHandlerException());

        environmentVariables.set("HERD_PASSWORD", "WEB_SERVICE_HTTPS_PASSWORD");
        String[] argumentsUsingEnvPassword =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", "", "-E", "true", "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, argumentsUsingEnvPassword, null, new ClientHandlerException());
    }

    @Test
    public void testGoSuccessSslTrueEnvPassword() throws Exception
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "-E", "true", "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
                "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new ClientHandlerException());
    }

    @Test
    public void testParseCommandLineArgumentsEnableEnvVarFalse()
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "-E", "false", "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
                "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void testParseCommandLineArgumentsHelpOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--help"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        assertTrue("Incorrect usage information returned.", output.startsWith("usage: " + RetentionExpirationExporterApp.APPLICATION_NAME));
    }

    @Test
    public void testParseCommandLineArgumentsInvalidEndRegistrationDateTime() throws Exception
    {
        String[] arguments = {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--startRegistrationDateTime",
            START_REGISTRATION_DATE_TIME_AS_TEXT, "--endRegistrationDateTime", STRING_VALUE, "--localOutputFile", LOCAL_OUTPUT_FILE, "--regServerHost",
            WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl", "true", "--username",
            WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
            "true"};

        // We are expecting this to fail with a IllegalArgumentException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new IllegalArgumentException());
    }

    @Test
    public void testParseCommandLineArgumentsInvalidRegServerPort() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", "INVALID_INTEGER", "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl", "true",
                "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};

        // We are expecting this to fail with a NumberFormatException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new NumberFormatException());
    }

    @Test
    public void testParseCommandLineArgumentsInvalidStartRegistrationDateTime() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--startRegistrationDateTime", STRING_VALUE,
                "--endRegistrationDateTime", END_REGISTRATION_DATE_TIME_AS_TEXT, "--localOutputFile", LOCAL_OUTPUT_FILE, "--regServerHost",
                WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl", "true",
                "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};

        // We are expecting this to fail with a IllegalArgumentException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new IllegalArgumentException());
    }

    @Test
    public void testParseCommandLineArgumentsNone()
    {
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(new String[] {}, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoPassword()
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoUsername()
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueEnvPassword()
    {
        environmentVariables.set("HERD_PASSWORD", WEB_SERVICE_HTTPS_PASSWORD);
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "-E", "true", "--trustSelfSignedCertificate", "true", "--disableHostnameVerification",
                "true"};
        assertNull(retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));

        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void testParseCommandLineArgumentsVersionOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--version"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);

        assertEquals("output", String.format(DataBridgeApp.BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(),
            buildInformation.getBuildUser()), output);
    }

    @Test
    public void testParseShortCommandLineArgumentsSuccess()
    {
        String[] arguments =
            {"-n", NAMESPACE, "-b", BUSINESS_OBJECT_DEFINITION_NAME, "-S", START_REGISTRATION_DATE_TIME_AS_TEXT, "-N", END_REGISTRATION_DATE_TIME_AS_TEXT, "-o",
                LOCAL_OUTPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P", WEB_SERVICE_HTTPS_PORT.toString(), "-c", UDC_SERVICE_HOSTNAME, "-s", "true", "-u",
                WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD, "-C", "true", "-d", "true"};
        assertNull(retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }
}
