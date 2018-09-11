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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.HivePartitionDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the Hive13DdlGenerator class.
 */
public class Hive13DdlGeneratorTest extends AbstractServiceTest
{
    @Test
    public void testGetHivePartitions()
    {
        // Create a test business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, true,
                BDATA_STATUS);

        List<SchemaColumn> autoDiscoverableSubPartitionColumns;
        List<String> storageFilePaths;
        List<HivePartitionDto> expectedHivePartitions;
        List<HivePartitionDto> resultHivePartitions;

        // Get business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // No storage files.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = new ArrayList<>();
        expectedHivePartitions = new ArrayList<>();
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, businessObjectDataEntity,
                STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Single level partitioning.
        autoDiscoverableSubPartitionColumns = new ArrayList<>();
        storageFilePaths = getStorageFilePaths(Arrays.asList("/file1.dat", "/file2.dat"));
        expectedHivePartitions = Arrays.asList(HivePartitionDto.builder().withPath("").withPartitionValues(Arrays.asList(PARTITION_VALUE)).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, businessObjectDataEntity,
                STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Test that we match column names in storage file paths ignoring the case.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Arrays.asList("/COLUMN1=111/COLUMN2=222/file.dat", "/column1=aa/column2=bb/"));
        expectedHivePartitions = Arrays
            .asList(HivePartitionDto.builder().withPath("/COLUMN1=111/COLUMN2=222").withPartitionValues(Arrays.asList(PARTITION_VALUE, "111", "222")).build(),
                HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, businessObjectDataEntity,
                STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);
    }

    @Test
    public void testGetHivePartitionsPatternMismatch()
    {
        // Create a test business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, true,
                BDATA_STATUS);

        List<SchemaColumn> autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        String pattern = hive13DdlGenerator.getHivePathPattern(autoDiscoverableSubPartitionColumns).pattern();

        List<String> badFilePaths = Arrays.asList("/column1=a/column2=b/extra-folder/file.dat",   // extra folder
            "/column2=a/column1=b/file.dat",                // partition columns out of order
            "/column1=a/file.dat",                          // missing partition sub-directory
            "/column1=a/column2=/file.dat",                 // missing partition value
            "/column1=a/column2/file.dat",                  // missing partition value
            "/column1=a/a/column2=2/file.dat",              // slash in a partition value
            "/column1=a/column2=2"                          // missing trailing '/' character
        );

        // Get business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        for (int i = 0; i < badFilePaths.size(); i++)
        {
            List<String> storageFilePaths = getStorageFilePaths(badFilePaths.subList(i, i + 1));
            try
            {
                hive13DdlGenerator.getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths,
                    businessObjectDataEntity, STORAGE_NAME);
                fail("Should throw an IllegalArgumentException when storage file does not match the expected Hive sub-directory pattern.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Registered storage file or directory does not match the expected Hive sub-directory pattern. " +
                        "Storage: {%s}, file/directory: {%s}, business object data: {%s}, S3 key prefix: {%s}, pattern: {^%s$}", STORAGE_NAME,
                    storageFilePaths.get(0), businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity), TEST_S3_KEY_PREFIX,
                    pattern), e.getMessage());
            }
        }
    }

    @Test
    public void testGetHivePartitionsMultiplePathsFound()
    {
        // Create a test business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION, true,
                BDATA_STATUS);

        List<SchemaColumn> autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        List<String> partitionPaths = Arrays.asList("/COLUMN1=111/COLUMN2=222", "/column1=111/COLUMN2=222");
        List<String> storageFilePaths = getStorageFilePaths(Arrays.asList(partitionPaths.get(0) + "/file.dat", partitionPaths.get(1) + "/file.dat"));

        try
        {
            hive13DdlGenerator
                .getHivePartitions(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), autoDiscoverableSubPartitionColumns,
                    TEST_S3_KEY_PREFIX, storageFilePaths, businessObjectDataEntity, STORAGE_NAME);
            fail("Should throw an IllegalArgumentException when multiple locations exist for the same Hive partition.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found two different locations for the same Hive partition. " +
                    "Storage: {%s}, business object data: {%s}, S3 key prefix: {%s}, path[1]: {%s}, path[2]: {%s}", STORAGE_NAME,
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity), TEST_S3_KEY_PREFIX, partitionPaths.get(0),
                partitionPaths.get(1)), e.getMessage());
        }
    }

    @Test
    public void testGetDdlCharacterValueEmptyString()
    {
        assertEquals("", hive13DdlGenerator.getDdlCharacterValue(""));
    }

    @Test
    public void testGetDdlCharacterValueLineFeed()
    {
        // Linefeed character is an ASCII octal \012 (decimal 10) which gets escaped as part of the DDL generation.
        assertEquals("\\012", hive13DdlGenerator.getDdlCharacterValue("\n"));
    }

    @Test
    public void testGetDdlCharacterValueAsciiPrintable()
    {
        assertEquals("|", hive13DdlGenerator.getDdlCharacterValue("|"));
    }

    @Test
    public void testGetDdlCharacterValueAsciiNonAsciiMix()
    {
        // It makes no sense to output a single non-ASCII printable character in the middle of other ASCII printable characters,
        // but that's what we do if a user actually specifies this. We might want to add more validation to prevent this scenario in the future
        // since Hive shouldn't allow this.  Please note that decimal 128 is 200 in octal.
        assertEquals("A\\200B", hive13DdlGenerator.getDdlCharacterValue("A" + String.valueOf((char) 128) + "B"));
    }

    @Test
    public void testGetDdlCharacterValueTwoNonAsciiPrintableChars()
    {
        // Decimal 128 is 200 in octal.
        assertEquals("\\200\\001", hive13DdlGenerator.getDdlCharacterValue(String.valueOf((char) 128) + String.valueOf((char) 1)));
    }

    @Test
    public void testGetDdlCharacterValueEscapeSingleBackslash()
    {
        assertEquals("\\", hive13DdlGenerator.getDdlCharacterValue("\\"));
        assertEquals("\\", hive13DdlGenerator.getDdlCharacterValue("\\", false));
        assertEquals("\\\\", hive13DdlGenerator.getDdlCharacterValue("\\", true));
        assertEquals("\\\\", hive13DdlGenerator.getDdlCharacterValue("\\\\", true));
    }

    @Test
    public void testEscapeSingleQuotes()
    {
        // Create a test vector with key=input and value=output values.
        LinkedHashMap<String, String> testVector = new LinkedHashMap<>();
        testVector.put("some text without single quotes", "some text without single quotes");
        testVector.put("'some \\'text\\' with single 'quotes'", "\\'some \\'text\\' with single \\'quotes\\'");
        testVector.put("'", "\\'");
        testVector.put("''''", "\\'\\'\\'\\'");
        testVector.put("'", "\\'");
        testVector.put("'\'\\'", "\\'\\'\\'");

        // Loop over all entries in the test vector.
        for (Object set : testVector.entrySet())
        {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) set;
            assertEquals(entry.getValue(), hive13DdlGenerator.escapeSingleQuotes((String) entry.getKey()));
        }
    }

    /**
     * Asserts that generateReplaceColumnsStatement() generates the correct DDL statement.
     */
    @Test
    public void testGenerateReplaceColumnsStatement()
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setTableName(TABLE_NAME);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(0);
            schemaColumnEntity.setName("col1");
            schemaColumnEntity.setType("varchar");
            schemaColumnEntity.setSize("255");
            schemaColumnEntity.setDescription("lorem ipsum");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(1);
            schemaColumnEntity.setName("col2");
            schemaColumnEntity.setType("date");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        String actual = hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);

        String expected =
            "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" + "    `col1` VARCHAR(255) COMMENT 'lorem ipsum',\n" +
                "    `col2` DATE);";

        Assert.assertEquals("generated DDL", expected, actual);
    }

    /**
     * Asserts that generateReplaceColumnsStatement() throws an IllegalArgumentException when the format entity only specified partitions, but no columns.
     */
    @Test
    public void testGenerateReplaceColumnsStatementAssertionErrorIfColumnsEmpty()
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setTableName(TABLE_NAME);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPartitionLevel(0);
            schemaColumnEntity.setName("col1");
            schemaColumnEntity.setType("date");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }

        try
        {
            hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "No schema columns specified for business object format {namespace: \"" + NAMESPACE + "\", businessObjectDefinitionName: \"" + BDEF_NAME +
                    "\", businessObjectFormatUsage: \"" + FORMAT_USAGE_CODE + "\", businessObjectFormatFileType: \"" + FORMAT_FILE_TYPE_CODE +
                    "\", businessObjectFormatVersion: " + FORMAT_VERSION + "}.", e.getMessage());
        }
    }

    private List<SchemaColumn> getPartitionColumns(List<String> partitionColumnNames)
    {
        List<SchemaColumn> schemaColumns = new ArrayList<>();

        for (String partitionColumnName : partitionColumnNames)
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumns.add(schemaColumn);
            schemaColumn.setName(partitionColumnName);
        }

        return schemaColumns;
    }

    private List<String> getStorageFilePaths(List<String> storageFileRelativePaths)
    {
        List<String> storageFilePaths = new ArrayList<>();

        for (String storageFileRelativePath : storageFileRelativePaths)
        {
            storageFilePaths.add(String.format("%s%s", TEST_S3_KEY_PREFIX, storageFileRelativePath));
        }

        return storageFilePaths;
    }
}
