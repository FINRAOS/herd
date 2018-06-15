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

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

public class RetentionExpirationExporterControllerTest extends AbstractExporterTest
{
    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Set the web client logger to warn level so we don't get unnecessary info level logging on the output.
        setLogLevel(DataBridgeWebClient.class, LogLevel.WARN);
        setLogLevel(RetentionExpirationExporterWebClient.class, LogLevel.WARN);
    }

    @Test
    public void testGetBusinessObjectDefinitionDisplayName()
    {
        // Create a business object definition name without a display name.
        BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();
        businessObjectDefinition.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);

        // Get a business object defintion display name and validate that it defaults to the business objetct definition name.
        assertEquals(BUSINESS_OBJECT_DEFINITION_NAME, retentionExpirationExporterController.getBusinessObjectDefinitionDisplayName(businessObjectDefinition));

        // Set business object definition display name for the test business object definition.
        businessObjectDefinition.setDisplayName(BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME);

        // Get and validate business object definition display name.
        assertEquals(BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME,
            retentionExpirationExporterController.getBusinessObjectDefinitionDisplayName(businessObjectDefinition));
    }

    @Test
    public void testGetBusinessObjectDefinitionUdcUri() throws Exception
    {
        assertEquals(String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME),
            retentionExpirationExporterController.getBusinessObjectDefinitionUdcUri(UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME));

        assertEquals("https://testUdcHostname/data-entities/testNamespace,%22/testBusinessObjectDefinitionName,%22", retentionExpirationExporterController
            .getBusinessObjectDefinitionUdcUri(UDC_SERVICE_HOSTNAME, NAMESPACE + ",\"", BUSINESS_OBJECT_DEFINITION_NAME + ",\""));
    }

    @Test
    public void testPerformDownloadOutputFileAlreadyExist() throws Exception
    {
        File outputFile = new File(LOCAL_OUTPUT_FILE);

        // Create an output file to test file already exists.
        assertTrue(outputFile.createNewFile());

        // Try to perform the retention expiration export.
        try
        {
            retentionExpirationExporterController
                .performRetentionExpirationExport(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, outputFile, new RegServerAccessParamsDto(), UDC_SERVICE_HOSTNAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The specified local output file \"%s\" already exists.", outputFile.toString()), e.getMessage());
        }
    }

    @Test
    public void testPerformRetentionExpirationExport() throws Exception
    {
        File outputFile = new File(LOCAL_OUTPUT_FILE);

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).withTrustSelfSignedCertificate(true)
                .withDisableHostnameVerification(true).build();

        // Perform the retention expiration export.
        retentionExpirationExporterController
            .performRetentionExpirationExport(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, outputFile, regServerAccessParamsDto, UDC_SERVICE_HOSTNAME);

        // Create the expected URI.
        String expectedUri = String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);

        // Create the expected output file content.
        String expectedOutputFileContent =
            ("\"Namespace\",\"Business Object Definition Name\",\"Business Object Format Usage\",\"Business Object Format File Type\"," +
                "\"Business Object Format Version\",\"Primary Partition Value\",\"Sub-Partition Value 1\",\"Sub-Partition Value 2\"," +
                "\"Sub-Partition Value 3\",\"Sub-Partition Value 4\",\"Business Object Data Version\",\"Business Object Definition Display Name\"," +
                "\"Business Object Definition URI\"") + System.lineSeparator() + String
                .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"%n", NAMESPACE,
                    BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION,
                    "primaryPartitionValue", "subPartitionValue1", "subPartitionValue2", "subPartitionValue3", "subPartitionValue4",
                    BUSINESS_OBJECT_DATA_VERSION, BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, expectedUri) + String
                .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"%n", NAMESPACE,
                    BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION,
                    "primaryPartitionValue", "", "", "", "", BUSINESS_OBJECT_DATA_VERSION, BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, expectedUri);

        // Validate the output file.
        String outputFileContent;
        try (FileInputStream inputStream = new FileInputStream(outputFile))
        {
            outputFileContent = IOUtils.toString(inputStream, Charset.defaultCharset());
        }
        assertEquals(expectedOutputFileContent, outputFileContent);
    }
}
