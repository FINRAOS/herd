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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.HivePartitionDto;
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
        List<SchemaColumn> autoDiscoverableSubPartitionColumns;
        List<String> storageFilePaths;
        List<HivePartitionDto> expectedHivePartitions;
        List<HivePartitionDto> resultHivePartitions;

        // Create a business object data key without any sub-partitions.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        // No storage files.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = new ArrayList<>();
        expectedHivePartitions = new ArrayList<>();
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Single level partitioning.
        autoDiscoverableSubPartitionColumns = new ArrayList<>();
        storageFilePaths = getStorageFilePaths(Arrays.asList("/file1.dat", "/file2.dat", "/"));
        expectedHivePartitions =
            Collections.singletonList(HivePartitionDto.builder().withPath("").withPartitionValues(Collections.singletonList(PARTITION_VALUE)).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Test that we match column names in storage file paths ignoring the case.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Arrays.asList("/COLUMN1=111/COLUMN2=222/file.dat", "/column1=aa/column2=bb/"));
        expectedHivePartitions = Arrays
            .asList(HivePartitionDto.builder().withPath("/COLUMN1=111/COLUMN2=222").withPartitionValues(Arrays.asList(PARTITION_VALUE, "111", "222")).build(),
                HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);
    }

    @Test
    public void testGetHivePartitionsPatternMismatch()
    {
        List<SchemaColumn> autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        String pattern = hive13DdlGenerator.getHivePathPattern(autoDiscoverableSubPartitionColumns).pattern();

        List<String> badFilePaths = Arrays.asList("/column1=a/column2=b/extra-folder/file.dat",   // unexpected extra folder after the last sub-partition
            "/extra-folder/column1=a/column2=b/file.dat",   // unexpected extra folder before the first sub-partition
            "//column1=a/column2=b/file.dat",               // unexpected extra '/' character before the first sub-partition
            "/column2=a/column1=b/file.dat",                // partition columns out of order
            "/column1=a/file.dat",                          // missing partition sub-directory
            "/column1=a/column2=/file.dat",                 // missing partition value
            "/column1=a/column2/file.dat",                  // missing partition value
            "/column1=a/a/column2=2/file.dat",              // slash in a partition value
            "/column1=a/column2=2",                         // missing trailing '/' character
            "/file",                                        // unexpected file
            "_$folder$/",                                   // unexpected '/' character after empty folder marker
            "_$folder$/file",                               // unexpected file after empty folder marker
            "/column1=a/file",                              // unexpected file
            "/column1=a//",                                 // double trailing '/' character
            "/column1=a_$folder$/file",                     // unexpected file after empty folder marker
            "/column1=a/column2=2//"                        // double trailing '/' character
        );

        // Create a business object data key without any sub-partitions.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        for (int i = 0; i < badFilePaths.size(); i++)
        {
            List<String> storageFilePaths = getStorageFilePaths(badFilePaths.subList(i, i + 1));
            try
            {
                hive13DdlGenerator
                    .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
                fail(String.format("Should throw an IllegalArgumentException when storage file does not match the expected Hive sub-directory pattern.%n" +
                        "    storageFilePaths: %s%n" + "    pattern: %s", StringUtils.join(storageFilePaths, " "),
                    hive13DdlGenerator.getHivePathPattern(autoDiscoverableSubPartitionColumns).pattern()));
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Registered storage file or directory does not match the expected Hive sub-directory pattern. " +
                        "Storage: {%s}, file/directory: {%s}, business object data: {%s}, S3 key prefix: {%s}, pattern: {%s}", STORAGE_NAME,
                    storageFilePaths.get(0), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), TEST_S3_KEY_PREFIX, pattern),
                    e.getMessage());
            }
        }
    }

    @Test
    public void testGetHivePartitionsPatternBadEmptyPartitions()
    {
        List<SchemaColumn> autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        String pattern = hive13DdlGenerator.getHivePathPattern(autoDiscoverableSubPartitionColumns).pattern();

        // Create bad empty sub-partition file paths - one in upper case and one in mixed case.
        List<String> badEmptyPartitionFilePaths = Arrays.asList("/column1=a/column2=b_$FOLDER$", "/column1=a/column2=b_$FolDeR$");

        // Create a business object data key without any sub-partitions.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        for (int i = 0; i < badEmptyPartitionFilePaths.size(); i++)
        {
            List<String> storageFilePaths = getStorageFilePaths(badEmptyPartitionFilePaths.subList(i, i + 1));
            try
            {
                hive13DdlGenerator
                    .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
                fail("Should throw an IllegalArgumentException when storage file does not match the expected Hive sub-directory pattern.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Registered storage file or directory does not match the expected Hive sub-directory pattern. " +
                        "Storage: {%s}, file/directory: {%s}, business object data: {%s}, S3 key prefix: {%s}, pattern: {%s}", STORAGE_NAME,
                    storageFilePaths.get(0), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), TEST_S3_KEY_PREFIX, pattern),
                    e.getMessage());
            }
        }
    }

    @Test
    public void testGetHivePartitionEmptyPartitions()
    {
        List<SchemaColumn> autoDiscoverableSubPartitionColumns;
        List<String> storageFilePaths;
        List<HivePartitionDto> expectedHivePartitions;
        List<HivePartitionDto> resultHivePartitions;

        // Create a business object data key without any sub-partitions.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        // No auto-discoverable sub-partition columns with empty partition represented by "/".
        autoDiscoverableSubPartitionColumns = new ArrayList<>();
        storageFilePaths = getStorageFilePaths(Collections.singletonList("/"));
        expectedHivePartitions =
            Collections.singletonList(HivePartitionDto.builder().withPath("").withPartitionValues(Collections.singletonList(PARTITION_VALUE)).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // No auto-discoverable sub-partition columns with empty partition represented by "_$folder$".
        autoDiscoverableSubPartitionColumns = new ArrayList<>();
        storageFilePaths = getStorageFilePaths(Collections.singletonList("_$folder$"));
        expectedHivePartitions =
            Collections.singletonList(HivePartitionDto.builder().withPath("").withPartitionValues(Collections.singletonList(PARTITION_VALUE)).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Single sub-partition column with empty partition represented by "/".
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Collections.singletonList("column1"));
        storageFilePaths = getStorageFilePaths(Collections.singletonList("/column1=aa/"));
        expectedHivePartitions =
            Collections.singletonList(HivePartitionDto.builder().withPath("/column1=aa").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Single sub-partition column with empty partition represented by "_$folder$".
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Collections.singletonList("column1"));
        storageFilePaths = getStorageFilePaths(Collections.singletonList("/column1=aa_$folder$"));
        expectedHivePartitions =
            Collections.singletonList(HivePartitionDto.builder().withPath("/column1=aa").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Two sub-partition columns with empty partition represented by "/".
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Collections.singletonList("/column1=aa/column2=bb/"));
        expectedHivePartitions = Collections.singletonList(
            HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Two sub-partition columns with empty partition represented by "_$folder$".
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Collections.singletonList("/column1=aa/column2=bb_$folder$"));
        expectedHivePartitions = Collections.singletonList(
            HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Two sub-partition columns with empty partition represented by "/" and with data files present.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Arrays.asList("/column1=aa/column2=bb/", "/column1=aa/column2=bb/file.dat"));
        expectedHivePartitions = Collections.singletonList(
            HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Two sub-partition columns with empty partition represented by "_$folder$" and with data files present.
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        storageFilePaths = getStorageFilePaths(Arrays.asList("/column1=aa/column2=bb_$folder$", "/column1=aa/column2=bb/file.dat"));
        expectedHivePartitions = Collections.singletonList(
            HivePartitionDto.builder().withPath("/column1=aa/column2=bb").withPartitionValues(Arrays.asList(PARTITION_VALUE, "aa", "bb")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);

        // Maximum supported number of sub-partition columns with empty folders present on every level and with data files present at the last sub-partition.
        // Different partition values are used to validate discovery logic for the fully qualified partitions.
        // There are also the following two special cases added:
        // - sub-partitions added that contain empty folder marker as a sub-partition value, which is allowed
        // - sub-partitions with data files that start, contain, and/or end with an empty folder marker
        autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column_1", "Column_2", "Column_3", "Column_4"));
        storageFilePaths = getStorageFilePaths(Arrays.asList("/",                       // an empty folder for the primary partition
            "_$folder$",                                                                // an empty folder for the primary partition
            "/column_1=a01/",                                                           // an empty folder for the first sub-partition
            "/column_1=a02_$folder$",                                                   // an empty folder for the first sub-partition
            "/column_1=a03/column-2=b01/",                                              // an empty folder for the second sub-partition
            "/column_1=a04/column-2=b02_$folder$",                                      // an empty folder for the second sub-partition
            "/column_1=a05/column-2=b03/COLUMN_3=c01/",                                 // an empty folder for the third sub-partition
            "/column_1=a06/column-2=b04/COLUMN_3=c02_$folder$",                         // an empty folder for the third sub-partition
            "/column_1=a07/column-2=b05/COLUMN_3=c03/COLUMN-4=d01/",                    // an empty folder for the forth sub-partition
            "/column_1=a08/column-2=b06/COLUMN_3=c04/COLUMN-4=d02_$folder$",            // an empty folder for the forth sub-partition
            "/column_1=a09/column-2=b07/COLUMN_3=c05/COLUMN-4=d03/file.dat",            // a data file present in the forth sub-partition
            "/column_1=a10/column-2=b08/COLUMN_3=c06/COLUMN-4=d04_$folder$/",           // a sub-partition value is an empty folder marker
            "/column_1=a11/column-2=b09/COLUMN_3=c07/COLUMN-4=d05_$folder$/file.dat",   // a sub-partition value is an empty folder marker
            "/column_1=a12/column-2=b10/COLUMN_3=c08/COLUMN-4=d06/_$folder$file.dat",   // data file name starts an empty folder marker
            "/column_1=a13/column-2=b11/COLUMN_3=c09/COLUMN-4=d07/file_$folder$.dat",   // data file name contains an empty folder marker
            "/column_1=a14/column-2=b12/COLUMN_3=c10/COLUMN-4=d08/file.dat_$folder$",   // data file name ends with an empty folder marker
            "/column_1=a15/column-2=b13/COLUMN_3=c11/COLUMN-4=d09/_$folder$"            // data file name is an empty folder marker
        ));
        expectedHivePartitions = Arrays.asList(HivePartitionDto.builder().withPath("/column_1=a07/column-2=b05/COLUMN_3=c03/COLUMN-4=d01")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a07", "b05", "c03", "d01")).build(),
            HivePartitionDto.builder().withPath("/column_1=a08/column-2=b06/COLUMN_3=c04/COLUMN-4=d02")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a08", "b06", "c04", "d02")).build(),
            HivePartitionDto.builder().withPath("/column_1=a09/column-2=b07/COLUMN_3=c05/COLUMN-4=d03")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a09", "b07", "c05", "d03")).build(),
            HivePartitionDto.builder().withPath("/column_1=a10/column-2=b08/COLUMN_3=c06/COLUMN-4=d04_$folder$")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a10", "b08", "c06", "d04_$folder$")).build(),
            HivePartitionDto.builder().withPath("/column_1=a11/column-2=b09/COLUMN_3=c07/COLUMN-4=d05_$folder$")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a11", "b09", "c07", "d05_$folder$")).build(),
            HivePartitionDto.builder().withPath("/column_1=a12/column-2=b10/COLUMN_3=c08/COLUMN-4=d06")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a12", "b10", "c08", "d06")).build(),
            HivePartitionDto.builder().withPath("/column_1=a13/column-2=b11/COLUMN_3=c09/COLUMN-4=d07")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a13", "b11", "c09", "d07")).build(),
            HivePartitionDto.builder().withPath("/column_1=a14/column-2=b12/COLUMN_3=c10/COLUMN-4=d08")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a14", "b12", "c10", "d08")).build(),
            HivePartitionDto.builder().withPath("/column_1=a15/column-2=b13/COLUMN_3=c11/COLUMN-4=d09")
                .withPartitionValues(Arrays.asList(PARTITION_VALUE, "a15", "b13", "c11", "d09")).build());
        resultHivePartitions = hive13DdlGenerator
            .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
        assertEquals(expectedHivePartitions, resultHivePartitions);
    }

    @Test
    public void testGetHivePartitionsMultiplePathsFound()
    {
        List<SchemaColumn> autoDiscoverableSubPartitionColumns = getPartitionColumns(Arrays.asList("Column1", "column2"));
        List<String> partitionPaths = Arrays.asList("/COLUMN1=111/COLUMN2=222", "/column1=111/COLUMN2=222");
        List<String> storageFilePaths = getStorageFilePaths(Arrays.asList(partitionPaths.get(0) + "/file.dat", partitionPaths.get(1) + "/file.dat"));

        // Create a business object data key without any sub-partitions.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        try
        {
            hive13DdlGenerator
                .getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, TEST_S3_KEY_PREFIX, storageFilePaths, STORAGE_NAME);
            fail("Should throw an IllegalArgumentException when multiple locations exist for the same Hive partition.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found two different locations for the same Hive partition. " +
                    "Storage: {%s}, business object data: {%s}, S3 key prefix: {%s}, path[1]: {%s}, path[2]: {%s}", STORAGE_NAME,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), TEST_S3_KEY_PREFIX, partitionPaths.get(0),
                partitionPaths.get(1)), e.getMessage());
        }
    }

    @Test
    public void testGetHivePathRegex()
    {
        List<String> expectedRegularExpressions = Arrays
            .asList("^(?:(\\/[^/]*|_\\$folder\\$))$", "^(?:(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_1|Column-1)=([^/]+)(\\/[^/]*|_\\$folder\\$))))$",
                "^(?:(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_1|Column-1)=([^/]+)(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_2|Column-2)=([^/]+)" +
                    "(\\/[^/]*|_\\$folder\\$))))))$",
                "^(?:(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_1|Column-1)=([^/]+)(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_2|Column-2)=([^/]+)" +
                    "(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_3|Column-3)=([^/]+)(\\/[^/]*|_\\$folder\\$))))))))$",
                "^(?:(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_1|Column-1)=([^/]+)(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_2|Column-2)=([^/]+)" +
                    "(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_3|Column-3)=([^/]+)(?:(?:\\/|_\\$folder\\$)|(?:\\/(?:(?i)Column_4|Column-4)=([^/]+)" +
                    "(\\/[^/]*|_\\$folder\\$))))))))))$");

        assertEquals(expectedRegularExpressions.get(0), hive13DdlGenerator.getHivePathRegex(new ArrayList<>()));
        assertEquals(expectedRegularExpressions.get(1), hive13DdlGenerator.getHivePathRegex(getPartitionColumns(Collections.singletonList("Column_1"))));
        assertEquals(expectedRegularExpressions.get(2), hive13DdlGenerator.getHivePathRegex(getPartitionColumns(Arrays.asList("Column_1", "Column_2"))));
        assertEquals(expectedRegularExpressions.get(3),
            hive13DdlGenerator.getHivePathRegex(getPartitionColumns(Arrays.asList("Column_1", "Column_2", "Column_3"))));
        assertEquals(expectedRegularExpressions.get(4),
            hive13DdlGenerator.getHivePathRegex(getPartitionColumns(Arrays.asList("Column_1", "Column_2", "Column_3", "Column_4"))));
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
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY);
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
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(2);
            schemaColumnEntity.setName("col3");
            // Use complex schema data type with whitespaces in it.
            // Expect the whitespaces to be removed whilke validating the complex data types.
            schemaColumnEntity.setType("map <double, array<bigint(5)>>");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(3);
            schemaColumnEntity.setName("col4");
            schemaColumnEntity.setType("uniontype<int,double,array<string>,struct<a:int,b:string>>");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(3);
            schemaColumnEntity.setName("col4");
            schemaColumnEntity.setType("struct<s:string,f:float,m:map<double,array<bigint>>>");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        String actual = hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);

        String expected =
            "ALTER TABLE `" + businessObjectFormatDdlRequest.getTableName() + "` REPLACE COLUMNS (\n" + "    `col1` VARCHAR(255) COMMENT 'lorem ipsum',\n" +
                "    `col2` DATE,\n" + "    `col3` map<double,array<bigint(5)>>,\n" +
                "    `col4` uniontype<int,double,array<string>,struct<a:int,b:string>>,\n" +
                "    `col4` struct<s:string,f:float,m:map<double,array<bigint>>>);";

        Assert.assertEquals("generated DDL", expected, actual);
    }

    @Test
    public void testGenerateReplaceColumnsStatementErrorNotComplexType()
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setTableName(TABLE_NAME);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY);
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(0);
            schemaColumnEntity.setName("col1");
            schemaColumnEntity.setType("MAP<DOUBLE,");
            schemaColumnEntity.setSize("255");
            schemaColumnEntity.setDescription("lorem ipsum");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        try
        {
            String actual = hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Column \"col1\" has an unsupported data type \"MAP<DOUBLE,\" in the schema for business object format {namespace: \"" + NAMESPACE +
                    "\", businessObjectDefinitionName: \"" + BDEF_NAME + "\", businessObjectFormatUsage: \"" + FORMAT_USAGE_CODE +
                    "\", businessObjectFormatFileType: \"" + FORMAT_FILE_TYPE_CODE + "\", businessObjectFormatVersion: " + FORMAT_VERSION +
                    "}. Exception : \"Error: type expected at the end of 'map<double,'\"", e.getMessage());
        }
    }

    @Test
    public void testGenerateReplaceColumnsStatementErrorNotValidSchemaColumnDataType()
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setTableName(TABLE_NAME);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY);
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(0);
            schemaColumnEntity.setName("col1");
            schemaColumnEntity.setType("fooobaar");
            schemaColumnEntity.setSize("255");
            schemaColumnEntity.setDescription("lorem ipsum");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        try
        {
            String actual = hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Column \"col1\" has an unsupported data type \"fooobaar\" in the schema for business object format {namespace: \"" + NAMESPACE +
                    "\", businessObjectDefinitionName: \"" + BDEF_NAME + "\", businessObjectFormatUsage: \"" + FORMAT_USAGE_CODE +
                    "\", businessObjectFormatFileType: \"" + FORMAT_FILE_TYPE_CODE + "\", businessObjectFormatVersion: " + FORMAT_VERSION +
                    "}. Exception : \"Error: type expected at the position 0 of 'fooobaar' but 'fooobaar' is found.\"", e.getMessage());
        }
    }

    @Test
    public void testGenerateReplaceColumnsStatementErrorValidSchemaColumnPrimitiveDataType()
    {
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setTableName(TABLE_NAME);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY);
        {
            SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
            schemaColumnEntity.setPosition(0);
            schemaColumnEntity.setName("col1");
            schemaColumnEntity.setType("int(25)");
            schemaColumnEntity.setDescription("lorem ipsum");
            businessObjectFormatEntity.getSchemaColumns().add(schemaColumnEntity);
        }
        try
        {
            String actual = hive13DdlGenerator.generateReplaceColumnsStatement(businessObjectFormatDdlRequest, businessObjectFormatEntity);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Column \"col1\" has an unsupported data type \"int(25)\" in the schema for business object format {namespace: \"" + NAMESPACE +
                    "\", businessObjectDefinitionName: \"" + BDEF_NAME + "\", businessObjectFormatUsage: \"" + FORMAT_USAGE_CODE +
                    "\", businessObjectFormatFileType: \"" + FORMAT_FILE_TYPE_CODE + "\", businessObjectFormatVersion: " + FORMAT_VERSION +
                    "}. Exception : \"null\"", e.getMessage());
        }
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
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, true, PARTITION_KEY);

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
