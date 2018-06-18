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

import java.net.UnknownHostException;

import org.junit.Test;
import org.springframework.context.ApplicationContext;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

public class RetentionExpirationExporterAppTest extends AbstractExporterTest
{
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
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true",
                "--disableHostnameVerification", "true"};

        // We are expecting this to fail with an UnknownHostException.
        runApplicationAndCheckReturnValue(retentionExpirationExporterApp, arguments, null, new UnknownHostException());
    }

    @Test
    public void testParseCommandLineArgumentsHelpOpt() throws Exception
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--help"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        assertTrue("Incorrect usage information returned.", output.startsWith("usage: " + RetentionExpirationExporterApp.APPLICATION_NAME));
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
    public void testParseCommandLineArgumentsNone() throws Exception
    {
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(new String[] {}, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoPassword() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--username", WEB_SERVICE_HTTPS_USERNAME, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsSslTrueAndNoUsername() throws Exception
    {
        String[] arguments =
            {"--namespace", NAMESPACE, "--businessObjectDefinitionName", BUSINESS_OBJECT_DEFINITION_NAME, "--localOutputFile", LOCAL_OUTPUT_FILE,
                "--regServerHost", WEB_SERVICE_HOSTNAME, "--regServerPort", WEB_SERVICE_HTTPS_PORT.toString(), "--udcServerHost", UDC_SERVICE_HOSTNAME, "--ssl",
                "true", "--password", WEB_SERVICE_HTTPS_PASSWORD, "--trustSelfSignedCertificate", "true", "--disableHostnameVerification", "true"};
        assertEquals(ToolsCommonConstants.ReturnValue.FAILURE, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }

    @Test
    public void testParseCommandLineArgumentsVersionOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--version"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
        });

        BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);

        assertEquals("output", String
            .format(DataBridgeApp.BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(), buildInformation.getBuildOs(),
                buildInformation.getBuildUser()), output);
    }

    @Test
    public void testParseShortCommandLineArgumentsSuccess() throws Exception
    {
        String[] arguments = {"-n", NAMESPACE, "-b", BUSINESS_OBJECT_DEFINITION_NAME, "-o", LOCAL_OUTPUT_FILE, "-H", WEB_SERVICE_HOSTNAME, "-P",
            WEB_SERVICE_HTTPS_PORT.toString(), "-c", UDC_SERVICE_HOSTNAME, "-s", "true", "-u", WEB_SERVICE_HTTPS_USERNAME, "-w", WEB_SERVICE_HTTPS_PASSWORD,
            "-C", "true", "-d", "true"};
        assertNull(retentionExpirationExporterApp.parseCommandLineArguments(arguments, applicationContext));
    }
}
