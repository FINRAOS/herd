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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

public class RetentionExpirationDestroyerControllerTest extends AbstractRetentionExpirationDestroyerTest
{
    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Set the web client logger to warn level so we don't get unnecessary info level logging on the output.
        setLogLevel(DataBridgeWebClient.class, LogLevel.WARN);
        setLogLevel(RetentionExpirationDestroyerWebClient.class, LogLevel.WARN);
    }

    @Test
    public void testGetBusinessObjectDataKeyInvalidBusinessObjectDataVersion()
    {
        // Try to get business object data key when business object data version is not a valid integer.
        try
        {
            String[] line = {NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION.toString(), PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1),
                SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3), INVALID_INTEGER_VALUE, BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME,
                BUSINESS_OBJECT_DEFINITION_URI};
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object data version must be an integer.", LINE_NUMBER,
                    LOCAL_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeyInvalidBusinessObjectFormatVersion()
    {
        // Try to get business object data key when business object format version is not a valid integer.
        try
        {
            String[] line = {NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, INVALID_INTEGER_VALUE,
                PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1), SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3),
                BUSINESS_OBJECT_DATA_VERSION.toString(), BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, BUSINESS_OBJECT_DEFINITION_URI};
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object format version must be an integer.",
                    LINE_NUMBER, LOCAL_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeyInvalidCsvLineFormat()
    {
        // Try to get business object data key when line does not have the expected number of columns.
        try
        {
            String[] line = {};
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Line number %d of input file \"%s\" does not match the expected format.", LINE_NUMBER, LOCAL_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeys() throws Exception
    {
        // Create a local input CSV file.
        File inputCsvFile = createLocalInputCsvFile();

        // Get and validate a list of business object data keys.
        List<BusinessObjectDataKey> result = retentionExpirationDestroyerController.getBusinessObjectDataKeys(inputCsvFile);

        // Validate the results.
        assertEquals(Arrays.asList(
            new BusinessObjectDataKey(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES, BUSINESS_OBJECT_DATA_VERSION),
            new BusinessObjectDataKey(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, NO_SUB_PARTITION_VALUES, BUSINESS_OBJECT_DATA_VERSION),
            new BusinessObjectDataKey(NAMESPACE + ",\"", BUSINESS_OBJECT_DEFINITION_NAME + ",\"", BUSINESS_OBJECT_FORMAT_USAGE + ",\"",
                BUSINESS_OBJECT_FORMAT_FILE_TYPE + ",\"", BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE + ",\"", Arrays
                .asList(SUB_PARTITION_VALUES.get(0) + ",\"", SUB_PARTITION_VALUES.get(1) + ",\"", SUB_PARTITION_VALUES.get(2) + ",\"",
                    SUB_PARTITION_VALUES.get(3) + ",\""), BUSINESS_OBJECT_DATA_VERSION)), result);
    }

    @Test
    public void testGetBusinessObjectDataKeysMissingCsvHeader() throws IOException
    {
        // Create an input CSV file without a header.
        File inputCsvFile = new File(LOCAL_INPUT_FILE);
        FileUtils.writeStringToFile(inputCsvFile, STRING_VALUE, StandardCharsets.UTF_8);

        // Try to get business object data keys when CSV file does not have an expected header.
        try
        {
            retentionExpirationDestroyerController.getBusinessObjectDataKeys(inputCsvFile);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Input file \"%s\" does not contain the expected CSV file header.", inputCsvFile.toString()), e.getMessage());
        }
    }

    @Test
    public void testPerformRetentionExpirationDestruction() throws Exception
    {
        // Create a local input CSV file.
        File inputCsvFile = createLocalInputCsvFile();

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).withTrustSelfSignedCertificate(true)
                .withDisableHostnameVerification(true).build();

        // Perform the retention expiration destruction.
        retentionExpirationDestroyerController.performRetentionExpirationDestruction(inputCsvFile, regServerAccessParamsDto);
    }

    /**
     * Creates a local SCV file with a header and two business object data entries.
     *
     * @return the local input file
     * @throws IOException if any problems were encountered
     */
    private File createLocalInputCsvFile() throws IOException
    {
        // Create an input CSV file.
        File inputCsvFile = new File(LOCAL_INPUT_FILE);

        // Create business object definition URI.
        String expectedUri = String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);

        // Build the input file content.
        String stringBuilder = ("\"Namespace\",\"Business Object Definition Name\",\"Business Object Format Usage\",\"Business Object Format File Type\"," +
            "\"Business Object Format Version\",\"Primary Partition Value\",\"Sub-Partition Value 1\",\"Sub-Partition Value 2\",\"Sub-Partition Value 3\"," +
            "\"Sub-Partition Value 4\",\"Business Object Data Version\",\"Business Object Definition Display Name\",\"Business Object Definition URI\"") +
            System.lineSeparator() + String
            .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"%n", NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME,
                BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE,
                SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1), SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3),
                BUSINESS_OBJECT_DATA_VERSION, BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, expectedUri) + String
            .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"%n", NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME,
                BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, "", "", "", "",
                BUSINESS_OBJECT_DATA_VERSION, BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, expectedUri) + String
            .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"%n", NAMESPACE + ",\"\"",
                BUSINESS_OBJECT_DEFINITION_NAME + ",\"\"", BUSINESS_OBJECT_FORMAT_USAGE + ",\"\"", BUSINESS_OBJECT_FORMAT_FILE_TYPE + ",\"\"",
                BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE + ",\"\"", SUB_PARTITION_VALUES.get(0) + ",\"\"", SUB_PARTITION_VALUES.get(1) + ",\"\"",
                SUB_PARTITION_VALUES.get(2) + ",\"\"", SUB_PARTITION_VALUES.get(3) + ",\"\"", BUSINESS_OBJECT_DATA_VERSION,
                BUSINESS_OBJECT_DEFINITION_DISPLAY_NAME, expectedUri);

        // Add a CSV header.

        // Add business object data with sub-partitions.

        // Add a business object data without sub-partitions.

        // Add a business object data that uses CSV file separator and quote characters in its alternate key values.

        // Write to the input CSV file.
        FileUtils.writeStringToFile(inputCsvFile, stringBuilder, StandardCharsets.UTF_8);

        return inputCsvFile;
    }
}
