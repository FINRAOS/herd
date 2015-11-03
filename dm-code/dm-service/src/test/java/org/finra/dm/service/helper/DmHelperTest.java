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
package org.finra.dm.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import org.finra.dm.dao.impl.MockEc2OperationsImpl;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.EmrClusterDefinition;
import org.finra.dm.model.api.xml.InstanceDefinition;
import org.finra.dm.model.api.xml.InstanceDefinitions;
import org.finra.dm.model.api.xml.MasterInstanceDefinition;
import org.finra.dm.model.api.xml.NodeTag;
import org.finra.dm.model.api.xml.Storage;
import org.finra.dm.model.api.xml.StorageFile;
import org.finra.dm.model.api.xml.StorageUnit;
import org.finra.dm.service.AbstractServiceTest;

/**
 * This class tests functionality within the DmHelper class.
 */
public class DmHelperTest extends AbstractServiceTest
{
    private static String YEAR = "2014";
    private static String MONTH = "12";
    private static String DAY = "31";
    private static String DATE_STRING = String.format("%s-%s-%s", YEAR, MONTH, DAY);

    protected static final Path LOCAL_TEMP_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "dm-helper-test", RANDOM_SUFFIX);

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv()
    {
        // Create a local temporary directory.
        LOCAL_TEMP_PATH.toFile().mkdirs();
    }

    /**
     * Cleans up the test environment.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(LOCAL_TEMP_PATH.toFile());
    }

    @Test
    public void testGetDateFromString() throws Exception
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dmHelper.getDateFromString(DATE_STRING));
        assertTrue(calendar.get(Calendar.YEAR) == new Integer(YEAR));
        assertTrue(calendar.get(Calendar.MONTH) == new Integer(MONTH) - 1); // Month is zero based so need to subtract one.
        assertTrue(calendar.get(Calendar.DAY_OF_MONTH) == new Integer(DAY));
    }

    @Test
    public void testGetDateFromStringNoDate() throws Exception
    {
        assertNull(dmHelper.getDateFromString(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDateFromStringMissingDay() throws Exception
    {
        dmHelper.getDateFromString(YEAR + MONTH);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDateFromStringInvalidDate() throws Exception
    {
        dmHelper.getDateFromString(YEAR + MONTH + "32");
    }

    @Test
    public void testGetHttpProxyHost()
    {
        for (String testHttpProxyHost : Arrays.asList(null, "", BLANK_TEXT, "Test-Hostname"))
        {
            // Test an environment specific value.
            assertEquals(StringUtils.isBlank(testHttpProxyHost) ? null : testHttpProxyHost, dmHelper.getHttpProxyHost(testHttpProxyHost, null));
            // Test an override value.
            assertEquals(testHttpProxyHost, dmHelper.getHttpProxyHost(BLANK_TEXT, testHttpProxyHost));
        }
    }

    @Test
    public void testGetHttpProxyPort()
    {
        for (Integer testHttpProxyPort : Arrays.asList(null, 0, 999, Integer.MIN_VALUE, Integer.MAX_VALUE))
        {
            // Test an environment specific value.
            assertEquals(testHttpProxyPort, dmHelper.getHttpProxyPort(testHttpProxyPort == null ? null : String.valueOf(testHttpProxyPort), null));
            // Test an override value.
            assertEquals(testHttpProxyPort, dmHelper.getHttpProxyPort(BLANK_TEXT, testHttpProxyPort));
        }
    }

    @Test
    public void testGetHttpProxyPortInvalidValue()
    {
        try
        {
            dmHelper.getHttpProxyPort("NOT_AN_INTEGER", null);
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().startsWith("Configured HTTP proxy port value ") && e.getMessage().endsWith(" is not an integer."));
        }
    }

    @Test
    public void testValidateNoDuplicateQueryStringParams() throws Exception
    {
        // Add a key with a single value which is allowed.
        Map<String, String[]> parameterMap = new HashMap<>();
        String[] singleValue = new String[1];
        singleValue[0] = "testValue"; // Single Value
        parameterMap.put("testKey1", singleValue);

        // Add a key with 2 values which which isn't normally allowed, but is not a problem because we aren't looking for it in the validate method below.
        String[] multipleValues = new String[2];
        multipleValues[0] = "testValue1";
        multipleValues[1] = "testValue2";
        parameterMap.put("testKey2", multipleValues);

        // Validate the query string parameters, but only for "testKey1" and not "testKey2".
        dmHelper.validateNoDuplicateQueryStringParams(parameterMap, "testKey1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateNoDuplicateQueryStringParamsWithException() throws Exception
    {
        Map<String, String[]> parameterMap = new HashMap<>();
        String[] values = new String[2];
        values[0] = "testValue1"; // Duplicate Values which aren't allowed.
        values[1] = "testValue2";
        parameterMap.put("testKey", values);
        dmHelper.validateNoDuplicateQueryStringParams(parameterMap, "testKey");
    }

    @Test
    public void getStorageUnitByStorageName()
    {
        // Create business object data with several test storage units.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        List<String> testStorageNames = Arrays.asList("Storage_1", "storage-2", "STORAGE3");

        List<StorageUnit> storageUnits = new ArrayList<>();
        businessObjectData.setStorageUnits(storageUnits);
        for (String testStorageName : testStorageNames)
        {
            StorageUnit storageUnit = new StorageUnit();
            storageUnits.add(storageUnit);

            Storage storage = new Storage();
            storageUnit.setStorage(storage);
            storage.setName(testStorageName);
        }

        // Validate that we can find all storage units regardless of the storage name case.
        for (String testStorageName : testStorageNames)
        {
            assertEquals(testStorageName, dmHelper.getStorageUnitByStorageName(businessObjectData, testStorageName).getStorage().getName());
            assertEquals(testStorageName, dmHelper.getStorageUnitByStorageName(businessObjectData, testStorageName.toUpperCase()).getStorage().getName());
            assertEquals(testStorageName, dmHelper.getStorageUnitByStorageName(businessObjectData, testStorageName.toLowerCase()).getStorage().getName());
        }
    }

    @Test
    public void getStorageUnitByStorageNameStorageUnitNoExists()
    {
        String testStorageName = "I_DO_NOT_EXIST";

        // Try to get a non-existing storage unit.
        try
        {
            dmHelper.getStorageUnitByStorageName(new BusinessObjectData(), testStorageName);
            fail("Should throw a RuntimeException when storage unit does not exist.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Business object data has no storage unit with storage name \"%s\".", testStorageName);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateS3Files() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);

        List<String> actualS3Files = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            actualS3Files.add(String.format("%s/%s", TEST_S3_KEY_PREFIX, file));
        }

        dmHelper.validateS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
    }

    @Test
    public void testValidateS3FilesS3KeyPrefixMismatch() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit("SOME_S3_KEY_PREFIX", LOCAL_FILES, FILE_SIZE_1_KB);

        // Try to validate S3 files when we have not registered S3 file.
        try
        {
            dmHelper.validateS3Files(storageUnit, new ArrayList<String>(), TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when registered S3 file does match S3 key prefix.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Storage file S3 key prefix \"%s\" does not match the expected S3 key prefix \"%s\".",
                storageUnit.getStorageFiles().get(0).getFilePath(), TEST_S3_KEY_PREFIX);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateS3FilesRegisteredFileNoExists() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);
        List<String> actualS3Files = new ArrayList<>();

        // Try to validate S3 files when actual S3 files do not exist.
        try
        {
            dmHelper.validateS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when actual S3 files do not exist.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Registered file \"%s\" does not exist in \"%s\" storage.", storageUnit.getStorageFiles().get(0).getFilePath(),
                    storageUnit.getStorage().getName());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateS3NotRegisteredS3FileFound() throws IOException
    {
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, new ArrayList<String>(), FILE_SIZE_1_KB);
        List<String> actualS3Files = Arrays.asList(String.format("%s/%s", TEST_S3_KEY_PREFIX, LOCAL_FILES.get(0)));

        // Try to validate S3 files when we have not registered S3 file.
        try
        {
            dmHelper.validateS3Files(storageUnit, actualS3Files, TEST_S3_KEY_PREFIX);
            fail("Should throw a RuntimeException when S3 contains unregistered S3 file.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Found S3 file \"%s\" in \"%s\" storage not registered with this business object data.", actualS3Files.get(0),
                storageUnit.getStorage().getName());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3Files() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);
        dmHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
    }

    @Test
    public void testValidateDownloadedS3FilesZeroFiles() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        // Create an empty target local directory.
        assertTrue(targetLocalDirectory.mkdirs());
        // Create a storage unit entity without any storage files.
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, null, null);
        // Validate an empty set of the downloaded S3 files.
        dmHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
    }

    @Test
    public void testValidateDownloadedS3FilesFileCountMismatch() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        createLocalFile(targetLocalDirectory.getPath(), "EXTRA_FILE", FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB);

        // Try to validate the local files, when number of files does not match to the storage unit information.
        try
        {
            dmHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when number of local files does not match to the storage unit information.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Number of downloaded files does not match the storage unit information (expected %d files, actual %d files).",
                    storageUnit.getStorageFiles().size(), LOCAL_FILES.size() + 1);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3FilesFileNoExists() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFile(targetLocalDirectory.getPath(), "ACTUAL_FILE", FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, Arrays.asList("EXPECTED_FILE"), FILE_SIZE_1_KB);

        // Try to validate non-existing local files.
        try
        {
            dmHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when actual local files do not exist.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String.format("Downloaded \"%s\" file doesn't exist.",
                Paths.get(LOCAL_TEMP_PATH.toString(), storageUnit.getStorageFiles().get(0).getFilePath()).toFile().getPath());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void testValidateDownloadedS3FilesFileSizeMismatch() throws IOException
    {
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX).toFile();
        createLocalFiles(targetLocalDirectory.getPath(), FILE_SIZE_1_KB);
        StorageUnit storageUnit = createStorageUnit(TEST_S3_KEY_PREFIX, LOCAL_FILES, FILE_SIZE_1_KB * 2);

        // Try to validate the local files, when actual file sizes do not not match to the storage unit information.
        try
        {
            dmHelper.validateDownloadedS3Files(LOCAL_TEMP_PATH.toString(), TEST_S3_KEY_PREFIX, storageUnit);
            fail("Should throw a RuntimeException when actual file sizes do not not match to the storage unit information.");
        }
        catch (RuntimeException e)
        {
            String expectedErrMsg = String
                .format("Size of the downloaded \"%s\" S3 file does not match the expected value (expected %d bytes, actual %d bytes).",
                    Paths.get(LOCAL_TEMP_PATH.toString(), storageUnit.getStorageFiles().get(0).getFilePath()).toFile().getPath(), FILE_SIZE_1_KB * 2,
                    FILE_SIZE_1_KB);
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    /**
     * Creates a storage unit with the specified list of files.
     *
     * @param s3KeyPrefix the S3 key prefix.
     * @param files the list of files.
     * @param fileSizeBytes the file size in bytes.
     *
     * @return the storage unit with the list of file paths
     */
    private StorageUnit createStorageUnit(String s3KeyPrefix, List<String> files, Long fileSizeBytes)
    {
        StorageUnit storageUnit = new StorageUnit();

        Storage storage = new Storage();
        storageUnit.setStorage(storage);

        storage.setName("TEST_STORAGE");
        List<StorageFile> storageFiles = new ArrayList<>();
        storageUnit.setStorageFiles(storageFiles);
        if (!CollectionUtils.isEmpty(files))
        {
            for (String file : files)
            {
                StorageFile storageFile = new StorageFile();
                storageFiles.add(storageFile);
                storageFile.setFilePath(String.format("%s/%s", s3KeyPrefix, file));
                storageFile.setFileSizeBytes(fileSizeBytes);
            }
        }

        return storageUnit;
    }

    @Test
    public void testGetStorageAttributeValueByName()
    {
        final String testStorageName = "MY_TEST_STORAGE";
        final String testAttributeNameNoExists = "I_DO_NOT_EXIST";

        Storage testStorage = new Storage();
        testStorage.setName(testStorageName);
        testStorage.setAttributes(getNewAttributes());

        assertEquals(ATTRIBUTE_VALUE_1, dmHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, testStorage, Boolean.FALSE));
        assertEquals(ATTRIBUTE_VALUE_2, dmHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_2_MIXED_CASE, testStorage, Boolean.TRUE));

        // Testing attribute name case insensitivity.
        assertEquals(ATTRIBUTE_VALUE_1, dmHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), testStorage, Boolean.TRUE));
        assertEquals(ATTRIBUTE_VALUE_1, dmHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), testStorage, Boolean.TRUE));

        assertNull(dmHelper.getStorageAttributeValueByName(testAttributeNameNoExists, testStorage, Boolean.FALSE));

        // Try to get a required attribute value what does not exist.
        try
        {
            dmHelper.getStorageAttributeValueByName(testAttributeNameNoExists, testStorage, Boolean.TRUE);
            fail("Suppose to throw a RuntimeException when required storage attribute does not exist or has a blank value.");
        }
        catch (RuntimeException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", testAttributeNameNoExists, testStorage.getName()),
                e.getMessage());
        }
    }

    @Test
    public void testBusinessObjectDataKeyToString()
    {
        // Create test business object data key.
        BusinessObjectDataKey testBusinessObjectDataKey = new BusinessObjectDataKey();
        testBusinessObjectDataKey.setNamespace(NAMESPACE_CD);
        testBusinessObjectDataKey.setBusinessObjectDefinitionName(BOD_NAME);
        testBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        testBusinessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        testBusinessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        testBusinessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        testBusinessObjectDataKey.setSubPartitionValues(SUBPARTITION_VALUES);
        testBusinessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);

        // Create the expected test output.
        String expectedOutput = String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
            "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, StringUtils.join(SUBPARTITION_VALUES, ","), DATA_VERSION);

        assertEquals(expectedOutput, dmHelper.businessObjectDataKeyToString(testBusinessObjectDataKey));
        assertEquals(expectedOutput, dmHelper
            .businessObjectDataKeyToString(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION));
    }

    @Test
    public void testValidateEmrClusterDefinitionConfigurationNullSubnet()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.setSubnetId(null);
        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() without modifications. The definition should be
     * valid.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationValid()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        }
        catch (Exception e)
        {
            fail("expected no exception, but " + e.getClass() + " was thrown. " + e);
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance spot price is specified The
     * definition should be valid because spot price is allowed when max search price is not specified.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterSpotPriceSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceSpotPrice(BigDecimal.ONE);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        }
        catch (Exception e)
        {
            fail("expected no exception, but " + e.getClass() + " was thrown. " + e);
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance max search price is
     * specified The definition should be valid because max search price is allowed when no instance spot price is specified.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterMaxSearchPriceSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(BigDecimal.ONE);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        }
        catch (Exception e)
        {
            fail("expected no exception, but " + e.getClass() + " was thrown. " + e);
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance max search price and
     * on-demand threshold is specified The definition should be valid because on-demand threshold can be used with max search price.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterMaxSearchPriceAndOnDemandThresholdSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(BigDecimal.ONE);
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceOnDemandThreshold(BigDecimal.ONE);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        }
        catch (Exception e)
        {
            fail("expected no exception, but " + e.getClass() + " was thrown. " + e);
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The task instance is not specified. The
     * definition should be valid because task instance is optional.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationTaskInstanceDefinitionNotSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().setTaskInstances(null);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        }
        catch (Exception e)
        {
            fail("expected no exception, but " + e.getClass() + " was thrown. " + e);
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The subnet is whitespace only. The definition is
     * not valid. Subnet is required.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationBlankSubnet()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.setSubnetId(" \r\t\n");
        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The subnet is a list, and contains at least 1
     * whitespace-only element The definition is not valid. All elements in subnet list must not be blank.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationSubnetListBlankElement()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.setSubnetId(MockEc2OperationsImpl.SUBNET_1 + ", \r\t\n");
        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master spot price is negative. The
     * definition is not valid. All prices must be positive.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterSpotPriceNegative()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceSpotPrice(BigDecimal.ONE.negate());

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance spot price and max search
     * price is specified. The definition is not valid. The two parameters are exclusive.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterSpotPriceAndMaxSearchPriceSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceSpotPrice(BigDecimal.ONE);
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(BigDecimal.ONE);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance max search price is
     * negative. The definition is not valid. All prices must be positive.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterMaxSearchPriceNegative()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(BigDecimal.ONE.negate());

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance on-demand threshold is
     * specified. The definition is not valid. On-demand threshold is only allowed when max search price is specified.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterOnDemandThresholdSpecified()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceOnDemandThreshold(BigDecimal.ONE);

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Tests case where validation is run against the definition generated by createValidEmrClusterDefinition() The master instance on-demand threshold is
     * negative. The definition is not valid. All prices must be positive.
     */
    @Test
    public void testValidateEmrClusterDefinitionConfigurationMasterMaxSearchPriceSpecifiedAndOnDemandThresholdNegative()
    {
        EmrClusterDefinition emrClusterDefinition = createValidEmrClusterDefinition();
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceMaxSearchPrice(BigDecimal.ONE);
        emrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceOnDemandThreshold(BigDecimal.ONE.negate());

        try
        {
            dmHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
            fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            System.out.println(e);
            assertEquals("thrown exception", IllegalArgumentException.class, e.getClass());
        }
    }

    /**
     * Creates a EMR cluster definition which does not cause validateEmrClusterDefinitionConfiguration() to throw an exception.
     * <p/>
     * - One subnet is specified - Master, core, and task instances are specified - Instance count, and instance type are specified for each instance
     * definition. - One node tag is specified
     *
     * @return A new instance of {@link EmrClusterDefinition}
     */
    private EmrClusterDefinition createValidEmrClusterDefinition()
    {
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setSubnetId(MockEc2OperationsImpl.SUBNET_1);
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();

        MasterInstanceDefinition masterInstanceDefinition = new MasterInstanceDefinition();
        masterInstanceDefinition.setInstanceCount(1);
        masterInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        instanceDefinitions.setMasterInstances(masterInstanceDefinition);

        InstanceDefinition coreInstanceDefinition = new InstanceDefinition();
        coreInstanceDefinition.setInstanceCount(1);
        coreInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        instanceDefinitions.setCoreInstances(coreInstanceDefinition);

        InstanceDefinition taskInstanceDefinition = new InstanceDefinition();
        taskInstanceDefinition.setInstanceCount(1);
        taskInstanceDefinition.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        instanceDefinitions.setTaskInstances(taskInstanceDefinition);

        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);

        List<NodeTag> nodeTags = new ArrayList<>();
        {
            nodeTags.add(new NodeTag("test_nodeTagName", "test_nodeTagValue"));
        }
        emrClusterDefinition.setNodeTags(nodeTags);

        return emrClusterDefinition;
    }
}
