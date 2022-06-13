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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.finra.herd.sdk.model.BusinessObjectDataKey;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

import static org.junit.Assert.*;

public class RetentionExpirationDestroyerControllerTest extends AbstractRetentionExpirationDestroyerTest
{

    private static final List<String> BUSINESS_OBJECT_DATA_HEADERS = Arrays
        .asList("Namespace", "Business Object Definition Name", "Business Object Format Usage", "Business Object Format File Type",
            "Business Object Format Version", "Primary Partition Value", "Sub-Partition Value 1", "Sub-Partition Value 2", "Sub-Partition Value 3",
            "Sub-Partition Value 4", "Business Object Data Version");

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
            List<String> line = Arrays.asList(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION.toString(), PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1),
                SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3), INVALID_INTEGER_VALUE);
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_EXCEL_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object data version must be an integer.", LINE_NUMBER,
                    LOCAL_EXCEL_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeyInvalidBusinessObjectFormatVersion() throws IOException
    {
        // Try to get business object data key when business object format version is not a valid integer.
        try
        {
            List<String> line = Arrays
                .asList(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, INVALID_INTEGER_VALUE,
                    PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1), SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3),
                    BUSINESS_OBJECT_DATA_VERSION.toString());
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_EXCEL_INPUT_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object format version must be an integer.",
                    LINE_NUMBER, LOCAL_EXCEL_INPUT_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeyInvalidExcelLineFormat()
    {
        // Try to get business object data key when line does not have the expected number of columns.
        try
        {
            List<String> line = new ArrayList<>();
            retentionExpirationDestroyerController.getBusinessObjectDataKey(line, LINE_NUMBER, new File(LOCAL_EXCEL_FILE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Line number %d of input file \"%s\" does not match the expected format.", LINE_NUMBER, LOCAL_EXCEL_FILE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataKeys() throws Exception
    {
        // Create a local input Excel file.
        File inputExcelFile = createLocalInputExcelFile();

        // Get and validate a list of business object data keys.
        List<BusinessObjectDataKey> result = retentionExpirationDestroyerController.getBusinessObjectDataKeys(inputExcelFile);

        // Validate the results.
        assertEquals(Arrays.asList(retentionExpirationDestroyerController
                .buildBusinessObjectDataKey(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                    BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES, BUSINESS_OBJECT_DATA_VERSION),
            retentionExpirationDestroyerController
                .buildBusinessObjectDataKey(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                    BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE, NO_SUB_PARTITION_VALUES, BUSINESS_OBJECT_DATA_VERSION),
            retentionExpirationDestroyerController
                .buildBusinessObjectDataKey(NAMESPACE + ",\"\"", BUSINESS_OBJECT_DEFINITION_NAME + ",\"\"", BUSINESS_OBJECT_FORMAT_USAGE + ",\"\"",
                    BUSINESS_OBJECT_FORMAT_FILE_TYPE + ",\"\"", BUSINESS_OBJECT_FORMAT_VERSION, PRIMARY_PARTITION_VALUE + ",\"\"", Arrays
                        .asList(SUB_PARTITION_VALUES.get(0) + ",\"\"", SUB_PARTITION_VALUES.get(1) + ",\"\"", SUB_PARTITION_VALUES.get(2) + ",\"\"",
                            SUB_PARTITION_VALUES.get(3) + ",\"\""), BUSINESS_OBJECT_DATA_VERSION)), result);
    }

    @Test
    public void testGetBusinessObjectDataKeysMissingExcelHeader() throws IOException
    {
        // Create an input Excel file without a header.
        File inputExcelFile = new File(LOCAL_EXCEL_INPUT_FILE);

        // Create xssf workbook.
        XSSFWorkbook workbook = new XSSFWorkbook();

        // Create a blank summary sheet (No need to test summary).
        workbook.createSheet("Summary");

        // Create business object data sheet.
        XSSFSheet dataSheet = workbook.createSheet("Business Object Data");

        // Initialize row number.
        int businessObjectDataRowNum = 0;

        // Create a blank header.
        dataSheet.createRow(businessObjectDataRowNum++);

        workbook.write(new FileOutputStream(inputExcelFile));
        workbook.close();

        // Try to get business object data keys when Excel file does not have an expected header.
        try
        {
            retentionExpirationDestroyerController.getBusinessObjectDataKeys(inputExcelFile);
            fail();
        }
        catch (IllegalArgumentException | InvalidFormatException e)
        {
            assertEquals(String.format("Input file \"%s\" does not contain the expected Excel file header.", inputExcelFile.toString()), e.getMessage());
        }
    }

    @Test
    public void testPerformRetentionExpirationDestruction() throws Exception
    {
        // Create a local input Excel file.
        File inputExcelFileFile = createLocalInputExcelFile();

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).withTrustSelfSignedCertificate(true)
                .withDisableHostnameVerification(true).build();

        // Perform the retention expiration destruction.
        retentionExpirationDestroyerController.performRetentionExpirationDestruction(inputExcelFileFile, regServerAccessParamsDto, false);
    }

    /**
     * Creates a local Excel file with a header and two business object data entries.
     *
     * @return the local input file
     * @throws IOException if any problems were encountered
     */
    private File createLocalInputExcelFile() throws IOException
    {
        // Create an input Excel file.
        File outPutExcelFile = new File(LOCAL_EXCEL_INPUT_FILE);

        // Create xssf workbook.
        XSSFWorkbook workbook = new XSSFWorkbook();

        // Create a blank summary sheet (No need to test summary).
        workbook.createSheet("Summary");

        // Create business object data sheet.
        XSSFSheet dataSheet = workbook.createSheet("Business Object Data");

        // Initialize row number.
        int businessObjectDataRowNum = 0;

        // Write detailed headers.
        XSSFRow dataHeaders = dataSheet.createRow(businessObjectDataRowNum++);
        int dataCellIndex = 0;
        for (String header : BUSINESS_OBJECT_DATA_HEADERS)
        {
            Cell cell = dataHeaders.createCell(dataCellIndex++);
            cell.setCellValue(header);
        }

        // Write detailed information for each business object data for each line.
        List<String> businessObjectDataRow = Arrays
            .asList(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
                BUSINESS_OBJECT_FORMAT_VERSION.toString(), PRIMARY_PARTITION_VALUE, SUB_PARTITION_VALUES.get(0), SUB_PARTITION_VALUES.get(1),
                SUB_PARTITION_VALUES.get(2), SUB_PARTITION_VALUES.get(3), BUSINESS_OBJECT_DATA_VERSION.toString());
        XSSFRow dataContents = dataSheet.createRow(businessObjectDataRowNum++);
        // Write value to each cell.
        dataCellIndex = 0;
        for (String businessObjectDataCell : businessObjectDataRow)
        {
            Cell cell = dataContents.createCell(dataCellIndex++);
            cell.setCellValue(businessObjectDataCell);
        }

        businessObjectDataRow = Arrays.asList(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME, BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE,
            BUSINESS_OBJECT_FORMAT_VERSION.toString(), PRIMARY_PARTITION_VALUE, "", "", "", "", BUSINESS_OBJECT_DATA_VERSION.toString());
        dataContents = dataSheet.createRow(businessObjectDataRowNum++);
        // Write value to each cell.
        dataCellIndex = 0;
        for (String businessObjectDataCell : businessObjectDataRow)
        {
            Cell cell = dataContents.createCell(dataCellIndex++);
            cell.setCellValue(businessObjectDataCell);
        }

        businessObjectDataRow = Arrays.asList(NAMESPACE + ",\"\"", BUSINESS_OBJECT_DEFINITION_NAME + ",\"\"", BUSINESS_OBJECT_FORMAT_USAGE + ",\"\"",
            BUSINESS_OBJECT_FORMAT_FILE_TYPE + ",\"\"", BUSINESS_OBJECT_FORMAT_VERSION.toString(), PRIMARY_PARTITION_VALUE + ",\"\"",
            SUB_PARTITION_VALUES.get(0) + ",\"\"", SUB_PARTITION_VALUES.get(1) + ",\"\"", SUB_PARTITION_VALUES.get(2) + ",\"\"",
            SUB_PARTITION_VALUES.get(3) + ",\"\"", BUSINESS_OBJECT_DATA_VERSION.toString());
        dataContents = dataSheet.createRow(businessObjectDataRowNum++);
        // Write value to each cell.
        dataCellIndex = 0;
        for (String businessObjectDataCell : businessObjectDataRow)
        {
            Cell cell = dataContents.createCell(dataCellIndex++);
            cell.setCellValue(businessObjectDataCell);
        }

        // Autosize the business object data sheet.
        for (int columnIndex = 0; columnIndex < BUSINESS_OBJECT_DATA_HEADERS.size(); columnIndex++)
        {
            dataSheet.autoSizeColumn(columnIndex);
        }

        // Write the workbook to input Excel file.
        workbook.write(new FileOutputStream(outPutExcelFile));
        workbook.close();

        return outPutExcelFile;
    }

    @Test
    public void testBuildBusinessObjectDataKey()
    {
        BusinessObjectDataKey businessObjectDataKey =
            retentionExpirationDestroyerController.buildBusinessObjectDataKey("namespace", "bdefName", "formatUsage", "fileType", 0, "partitionValue",
                Arrays.asList("a", "b"), 1);
        assertEquals("namespace", businessObjectDataKey.getNamespace());
        assertEquals("bdefName", businessObjectDataKey.getBusinessObjectDefinitionName());
        assertEquals("formatUsage", businessObjectDataKey.getBusinessObjectFormatUsage());
        assertEquals("fileType", businessObjectDataKey.getBusinessObjectFormatFileType());
        assertEquals(0, businessObjectDataKey.getBusinessObjectFormatVersion().intValue());
        assertEquals("partitionValue", businessObjectDataKey.getPartitionValue());
        assertEquals(Arrays.asList("a", "b"), businessObjectDataKey.getSubPartitionValues());
        assertEquals(1, businessObjectDataKey.getBusinessObjectDataVersion().intValue());
    }
}
