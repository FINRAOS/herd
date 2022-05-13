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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.RetentionExpirationExporterAggregateStatsDto;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.BusinessObjectDefinition;
import org.finra.herd.sdk.model.BusinessObjectFormatKey;
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
                .performRetentionExpirationExport(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, START_REGISTRATION_DATE_TIME, END_REGISTRATION_DATE_TIME,
                    outputFile, new RegServerAccessParamsDto(), UDC_SERVICE_HOSTNAME);
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
        File outputFile = new File(LOCAL_EXCEL_OUTPUT_FILE);

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).withTrustSelfSignedCertificate(true)
                .withDisableHostnameVerification(true).build();

        // Perform the retention expiration export.
        retentionExpirationExporterController
            .performRetentionExpirationExport(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, NO_REGISTRATION_DATE_TIME, NO_REGISTRATION_DATE_TIME, outputFile,
                regServerAccessParamsDto, UDC_SERVICE_HOSTNAME);

        // Create the expected URI.
        String expectedUri = String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);

        // Get the output Excel file.
        XSSFWorkbook workbook = new XSSFWorkbook(outputFile);

        // Get the summary sheet
        XSSFSheet summarySheet = workbook.getSheetAt(0);

        // Validate summary headers.
        XSSFRow row = summarySheet.getRow(0);
        List<String> outputHeaders = new ArrayList<>();
        for (Cell cell : row)
        {
            outputHeaders.add(cell.getStringCellValue());
        }
        List<String> expectedOutputHeaders = RetentionExpirationExporterController.SUMMARY_HEADER;
        assertEquals(expectedOutputHeaders, outputHeaders);

        // Validate summary contents.
        row = summarySheet.getRow(1);
        List<String> outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        List<String> expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "primaryPartitionValue", "2", "", "", "", "", "testBusinessObjectDefinitionDisplayName", expectedUri);
        assertEquals(expectedOutputContents, outputContents);

        // Get business object data sheet
        XSSFSheet dataSheet = workbook.getSheetAt(1);

        // Validate business object data headers.
        row = dataSheet.getRow(0);
        outputHeaders = new ArrayList<>();
        for (Cell cell : row)
        {
            outputHeaders.add(cell.getStringCellValue());
        }
        expectedOutputHeaders = RetentionExpirationExporterController.BUSINESS_OBJECT_DATA_HEADER;
        assertEquals(expectedOutputHeaders, outputHeaders);

        // Validate business object data contents.
        row = dataSheet.getRow(1);
        outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "subPartitionValue1", "subPartitionValue2", "subPartitionValue3", "subPartitionValue4", "5");
        assertEquals(expectedOutputContents, outputContents);

        row = dataSheet.getRow(2);
        outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "", "", "", "", "5");
        assertEquals(expectedOutputContents, outputContents);
    }

    @Test
    public void testPerformRetentionExpirationExportWithRegistrationDateTimeRange() throws Exception
    {
        File outputFile = new File(LOCAL_EXCEL_OUTPUT_FILE);

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).withTrustSelfSignedCertificate(true)
                .withDisableHostnameVerification(true).build();

        // Perform the retention expiration export.
        retentionExpirationExporterController
            .performRetentionExpirationExport(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, START_REGISTRATION_DATE_TIME_2, END_REGISTRATION_DATE_TIME_2,
                outputFile, regServerAccessParamsDto, UDC_SERVICE_HOSTNAME);

        // Create the expected URI.
        String expectedUri = String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);

        // Get the output Excel file.
        XSSFWorkbook workbook = new XSSFWorkbook(outputFile);

        // Get summary sheet
        XSSFSheet summarySheet = workbook.getSheetAt(0);

        // Validate summary headers.
        XSSFRow row = summarySheet.getRow(0);
        List<String> outputHeaders = new ArrayList<>();
        for (Cell cell : row)
        {
            outputHeaders.add(cell.getStringCellValue());
        }
        List<String> expectedOutputHeaders = RetentionExpirationExporterController.SUMMARY_HEADER;
        assertEquals(expectedOutputHeaders, outputHeaders);

        // Validate summary contents.
        row = summarySheet.getRow(1);
        List<String> outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        List<String> expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "primaryPartitionValue", "2", START_REGISTRATION_DATE_TIME_2.toString(), END_REGISTRATION_DATE_TIME_2.toString(), "",
                "", "testBusinessObjectDefinitionDisplayName", expectedUri);
        assertEquals(expectedOutputContents, outputContents);

        // Get business object data sheet
        XSSFSheet dataSheet = workbook.getSheetAt(1);

        // Validate business object data headers.
        row = dataSheet.getRow(0);
        outputHeaders = new ArrayList<>();
        for (Cell cell : row)
        {
            outputHeaders.add(cell.getStringCellValue());
        }
        expectedOutputHeaders = RetentionExpirationExporterController.BUSINESS_OBJECT_DATA_HEADER;
        assertEquals(expectedOutputHeaders, outputHeaders);

        // Validate business object data contents.
        row = dataSheet.getRow(1);
        outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "subPartitionValue1", "subPartitionValue2", "subPartitionValue3", "subPartitionValue4", "5");
        assertEquals(expectedOutputContents, outputContents);

        row = dataSheet.getRow(2);
        outputContents = new ArrayList<>();
        for (Cell cell : row)
        {
            outputContents.add(cell.getStringCellValue());
        }
        expectedOutputContents = Arrays
            .asList("testNamespace", "testBusinessObjectDefinitionName", "testBusinessObjectFormatUsage", "testBusinessObjectFormatFileType", "9",
                "primaryPartitionValue", "", "", "", "", "5");
        assertEquals(expectedOutputContents, outputContents);
    }

    @Test
    public void testGetAggregateStats()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setNamespace(NAMESPACE);
        businessObjectData.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
        businessObjectFormatKey.setNamespace(NAMESPACE);
        businessObjectFormatKey.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 1, null, null);

        expectedResults.put(businessObjectFormatKey, retentionExpirationExporterAggregateStatsDto);

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsMinMaxPartitionValue()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setNamespace(NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData1.setPartitionValue(PRIMARY_PARTITION_VALUE);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setNamespace(NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData2.setPartitionValue(PRIMARY_PARTITION_VALUE_2);

        BusinessObjectData businessObjectData3 = new BusinessObjectData();
        businessObjectData3.setNamespace(NAMESPACE_2);
        businessObjectData3.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData3.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData3.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);


        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData1, businessObjectData2, businessObjectData3);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey1 = new BusinessObjectFormatKey();
        businessObjectFormatKey1.setNamespace(NAMESPACE);
        businessObjectFormatKey1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        BusinessObjectFormatKey businessObjectFormatKey2 = new BusinessObjectFormatKey();
        businessObjectFormatKey2.setNamespace(NAMESPACE_2);
        businessObjectFormatKey2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto1 =
            new RetentionExpirationExporterAggregateStatsDto(PRIMARY_PARTITION_VALUE, PRIMARY_PARTITION_VALUE_2, 2, null, null);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto2 =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 1, null, null);

        expectedResults.put(businessObjectFormatKey1, retentionExpirationExporterAggregateStatsDto1);
        expectedResults.put(businessObjectFormatKey2, retentionExpirationExporterAggregateStatsDto2);

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsEqualPartitionValue()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setNamespace(NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData1.setPartitionValue(PRIMARY_PARTITION_VALUE);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setNamespace(NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData2.setPartitionValue(PRIMARY_PARTITION_VALUE);

        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData1, businessObjectData2);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey1 = new BusinessObjectFormatKey();
        businessObjectFormatKey1.setNamespace(NAMESPACE);
        businessObjectFormatKey1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto1 =
            new RetentionExpirationExporterAggregateStatsDto(PRIMARY_PARTITION_VALUE, PRIMARY_PARTITION_VALUE, 2, null, null);

        expectedResults.put(businessObjectFormatKey1, retentionExpirationExporterAggregateStatsDto1);

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsMultipleFormatVersion()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setNamespace(NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData1.setBusinessObjectFormatVersion(BUSINESS_OBJECT_FORMAT_VERSION);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setNamespace(NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData2.setBusinessObjectFormatVersion(BUSINESS_OBJECT_FORMAT_VERSION_2);

        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData1, businessObjectData2);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey1 = new BusinessObjectFormatKey();
        businessObjectFormatKey1.setNamespace(NAMESPACE);
        businessObjectFormatKey1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectFormatKey1.setBusinessObjectFormatVersion(BUSINESS_OBJECT_FORMAT_VERSION);

        BusinessObjectFormatKey businessObjectFormatKey2 = new BusinessObjectFormatKey();
        businessObjectFormatKey2.setNamespace(NAMESPACE);
        businessObjectFormatKey2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectFormatKey2.setBusinessObjectFormatVersion(BUSINESS_OBJECT_FORMAT_VERSION_2);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto1 =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 1, null, null);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto2 =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 1, null, null);

        expectedResults.put(businessObjectFormatKey1, retentionExpirationExporterAggregateStatsDto1);
        expectedResults.put(businessObjectFormatKey2, retentionExpirationExporterAggregateStatsDto2);

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsEmptyDataList()
    {
        // Create a list of business object data.
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsRegistrationDateTime()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setNamespace(NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData1.setCreatedOn(START_REGISTRATION_DATE_TIME_2);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setNamespace(NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData2.setCreatedOn(START_REGISTRATION_DATE_TIME_3);

        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData1, businessObjectData2);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey1 = new BusinessObjectFormatKey();
        businessObjectFormatKey1.setNamespace(NAMESPACE);
        businessObjectFormatKey1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto1 =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 2, HerdDateUtils.getXMLGregorianCalendarValue(START_REGISTRATION_DATE_TIME_2.toDate()),
                HerdDateUtils.getXMLGregorianCalendarValue(START_REGISTRATION_DATE_TIME_3.toDate()));

        expectedResults.put(businessObjectFormatKey1, retentionExpirationExporterAggregateStatsDto1);

        // Validate the results.
        assertEquals(expectedResults, results);
    }

    @Test
    public void testGetAggregateStatsEqualRegistrationDateTime()
    {
        // Create a list of business object data.
        BusinessObjectData businessObjectData1 = new BusinessObjectData();
        businessObjectData1.setNamespace(NAMESPACE);
        businessObjectData1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData1.setCreatedOn(START_REGISTRATION_DATE_TIME_2);

        BusinessObjectData businessObjectData2 = new BusinessObjectData();
        businessObjectData2.setNamespace(NAMESPACE);
        businessObjectData2.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectData2.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectData2.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        businessObjectData2.setCreatedOn(START_REGISTRATION_DATE_TIME_2);

        List<BusinessObjectData> businessObjectDataList = Arrays.asList(businessObjectData1, businessObjectData2);

        // Call the method under test.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> results =
            retentionExpirationExporterController.getAggregateStats(businessObjectDataList);

        // Create expected result map.
        Map<BusinessObjectFormatKey, RetentionExpirationExporterAggregateStatsDto> expectedResults = new LinkedHashMap<>();

        BusinessObjectFormatKey businessObjectFormatKey1 = new BusinessObjectFormatKey();
        businessObjectFormatKey1.setNamespace(NAMESPACE);
        businessObjectFormatKey1.setBusinessObjectDefinitionName(BUSINESS_OBJECT_DEFINITION_NAME);
        businessObjectFormatKey1.setBusinessObjectFormatUsage(BUSINESS_OBJECT_FORMAT_USAGE);
        businessObjectFormatKey1.setBusinessObjectFormatFileType(BUSINESS_OBJECT_FORMAT_FILE_TYPE);

        RetentionExpirationExporterAggregateStatsDto retentionExpirationExporterAggregateStatsDto1 =
            new RetentionExpirationExporterAggregateStatsDto(null, null, 2, HerdDateUtils.getXMLGregorianCalendarValue(START_REGISTRATION_DATE_TIME_2.toDate()),
                HerdDateUtils.getXMLGregorianCalendarValue(START_REGISTRATION_DATE_TIME_2.toDate()));

        expectedResults.put(businessObjectFormatKey1, retentionExpirationExporterAggregateStatsDto1);

        // Validate the results.
        assertEquals(expectedResults, results);
    }
}
