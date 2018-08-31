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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.RegistrationDateRangeFilter;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.AbstractServiceTest;

public class BusinessObjectDataHelperTest extends AbstractServiceTest
{
    @After
    public void cleanupEnv()
    {
        try
        {
            restorePropertySourceInEnvironment();
        }
        catch (Exception e)
        {
            // This method throws an exception when no override happens. Ignore the error.
        }
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
            assertEquals(testStorageName, businessObjectDataHelper.getStorageUnitByStorageName(businessObjectData, testStorageName).getStorage().getName());
            assertEquals(testStorageName,
                businessObjectDataHelper.getStorageUnitByStorageName(businessObjectData, testStorageName.toUpperCase()).getStorage().getName());
            assertEquals(testStorageName,
                businessObjectDataHelper.getStorageUnitByStorageName(businessObjectData, testStorageName.toLowerCase()).getStorage().getName());
        }
    }

    @Test
    public void getStorageUnitByStorageNameStorageUnitNoExists()
    {
        String testStorageName = "I_DO_NOT_EXIST";

        // Try to get a non-existing storage unit.
        try
        {
            businessObjectDataHelper.getStorageUnitByStorageName(
                new BusinessObjectData(INTEGER_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, NO_STORAGE_UNITS,
                    NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                    NO_RETENTION_EXPIRATION_DATE), testStorageName);
            fail("Should throw a IllegalStateException when storage unit does not exist.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Business object data has no storage unit with storage name \"%s\".", testStorageName), e.getMessage());
        }
    }

    @Test
    public void testBusinessObjectDataKeyToString()
    {
        // Create test business object data key.
        BusinessObjectDataKey testBusinessObjectDataKey = new BusinessObjectDataKey();
        testBusinessObjectDataKey.setNamespace(NAMESPACE);
        testBusinessObjectDataKey.setBusinessObjectDefinitionName(BDEF_NAME);
        testBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        testBusinessObjectDataKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        testBusinessObjectDataKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        testBusinessObjectDataKey.setPartitionValue(PARTITION_VALUE);
        testBusinessObjectDataKey.setSubPartitionValues(SUBPARTITION_VALUES);
        testBusinessObjectDataKey.setBusinessObjectDataVersion(DATA_VERSION);

        // Create the expected test output.
        String expectedOutput = String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, StringUtils.join(SUBPARTITION_VALUES, ","), DATA_VERSION);

        assertEquals(expectedOutput, businessObjectDataHelper.businessObjectDataKeyToString(testBusinessObjectDataKey));
        assertEquals(expectedOutput, businessObjectDataHelper
            .businessObjectDataKeyToString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));
    }

    @Test
    public void testBusinessObjectDataKeyToStringWithNull()
    {
        assertNull(businessObjectDataHelper.businessObjectDataKeyToString(null));
    }

    @Test
    public void testGetPartitionValue()
    {
        // Create and persist test database entities.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Retrieve primary and sub-partition values along with trying the "out of bounds" cases.
        assertEquals(null, businessObjectDataHelper.getPartitionValue(businessObjectDataEntity, 0));
        assertEquals(PARTITION_VALUE, businessObjectDataHelper.getPartitionValue(businessObjectDataEntity, 1));
        for (int partitionColumnPosition = 2; partitionColumnPosition <= BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; partitionColumnPosition++)
        {
            assertEquals(SUBPARTITION_VALUES.get(partitionColumnPosition - 2),
                businessObjectDataHelper.getPartitionValue(businessObjectDataEntity, partitionColumnPosition));
        }
        assertEquals(null, businessObjectDataHelper.getPartitionValue(businessObjectDataEntity, BusinessObjectDataEntity.MAX_SUBPARTITIONS + 2));
    }

    @Test
    public void testSubPartitions()
    {
        String namespace = NAMESPACE;
        String businessObjectDefinitionName = BDEF_NAME;
        String businessObjectFormatUsage = FORMAT_USAGE_CODE;
        String fileType = FORMAT_FILE_TYPE_CODE;
        Integer businessObjectFormatVersion = FORMAT_VERSION;
        String businessObjectFormatPartitionKey = PARTITION_KEY;
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionValue = PARTITION_VALUE;
        List<String> subPartitionValues = SUBPARTITION_VALUES;
        Integer businessObjectDataVersion = DATA_VERSION;

        String actualS3KeyPrefix = buildS3KeyPrefix(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatPartitionKey, schemaColumns, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion);

        String expectedS3KeyPrefix = getExpectedS3KeyPrefix(namespace, DATA_PROVIDER_NAME, businessObjectDefinitionName, businessObjectFormatUsage, fileType,
            businessObjectFormatVersion, businessObjectFormatPartitionKey, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion);

        assertEquals(expectedS3KeyPrefix, actualS3KeyPrefix);
    }

    @Test
    public void testValidateRegistrationDateRangeFilter() throws DatatypeConfigurationException
    {
        XMLGregorianCalendar start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2018-04-01");
        XMLGregorianCalendar end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2018-04-02");

        // Start date is less then end date
        businessObjectDataHelper.validateRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));

        // Start date is equel end date
        businessObjectDataHelper.validateRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, start));
    }

    @Test
    public void testValidateRegistrationDateRangeFilterMissingStartAndEndDate()
    {
        try
        {
            businessObjectDataHelper.validateRegistrationDateRangeFilter(new RegistrationDateRangeFilter());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Either start registration date or end registration date must be specified.", e.getMessage());
        }
    }

    @Test
    public void testValidateRegistrationDateRangeFilterStartDateIsGreaterThenEndDate() throws DatatypeConfigurationException
    {
        XMLGregorianCalendar end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2018-04-01");
        XMLGregorianCalendar start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2018-04-02");
        try
        {
            businessObjectDataHelper.validateRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The start registration date \"%s\" cannot be greater than the end registration date \"%s\".", start, end),
                e.getMessage());
        }
    }

    private String buildS3KeyPrefix(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String fileType,
        Integer businessObjectFormatVersion, String businessObjectFormatPartitionKey, List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns,
        String partitionValue, List<String> subPartitionValues, Integer businessObjectDataVersion)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion, null,
                null, LATEST_VERSION_FLAG_SET, businessObjectFormatPartitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, null, null, null, schemaColumns,
                partitionColumns);
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion, partitionValue,
                subPartitionValues, businessObjectDataVersion);

        return s3KeyPrefixHelper
            .buildS3KeyPrefix(AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity, businessObjectDataKey, STORAGE_NAME);
    }

    private String getExpectedS3KeyPrefix(String namespace, String dataProvider, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String fileType, Integer businessObjectFormatVersion, String businessObjectFormatPartitionKey, List<SchemaColumn> partitionColumns,
        String partitionValue, List<String> subPartitionValues, Integer businessObjectDataVersion)
    {
        partitionColumns = partitionColumns.subList(0, Math.min(partitionColumns.size(), 5));
        SchemaColumn[] subPartitionKeys = new SchemaColumn[partitionColumns.size() - 1];
        subPartitionKeys = partitionColumns.subList(1, partitionColumns.size()).toArray(subPartitionKeys);

        String[] subPartitionValueArray = new String[subPartitionValues.size()];
        subPartitionValueArray = subPartitionValues.toArray(subPartitionValueArray);

        return getExpectedS3KeyPrefix(namespace, dataProvider, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatPartitionKey, partitionValue, subPartitionKeys, subPartitionValueArray, businessObjectDataVersion);
    }
}
