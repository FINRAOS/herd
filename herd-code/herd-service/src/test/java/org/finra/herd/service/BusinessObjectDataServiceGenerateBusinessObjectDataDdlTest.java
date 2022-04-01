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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests generateBusinessObjectDataDdl functionality within the business object data service.
 */
public class BusinessObjectDataServiceGenerateBusinessObjectDataDdlTest extends AbstractServiceTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueList()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);

        // Retrieve business object data ddl when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueListStandalonePartitionValueFilter()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl using request with a standalone partition value filter.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueRange()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;
        String expectedDdl;

        // Retrieve business object data ddl when start partition value is less than the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, END_PARTITION_VALUE, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
            ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);

        // Retrieve business object data ddl when start partition value is equal to the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, START_PARTITION_VALUE, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(START_PARTITION_VALUE), SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestBeforePartitionValue()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Check an availability using a latest before partition value filter option.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                    DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                    NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION,
                    NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                    DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                    NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE), AbstractServiceTest.NO_AS_OF_TIME),
                resultBusinessObjectDataDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestBeforePartitionValueWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare database entities required for testing.
        StorageUnitEntity storageUnitEntity = businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.UPLOADING);
        assertNotNull(businessObjectDataStatusEntity);
        storageUnitEntity.getBusinessObjectData().setStatus(businessObjectDataStatusEntity);
        businessObjectDataDao.saveAndRefresh(storageUnitEntity.getBusinessObjectData());

        // Create request with latest before partition value filter.
        BusinessObjectDataDdlRequest request =
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue(PARTITION_VALUE), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES);

        // Confirm that, by default, no results are returned since there are no VALID business object data instances.
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertTrue(e.getMessage()
                .startsWith(String.format("Failed to find partition value which is the latest before partition value = \"%s\"", PARTITION_VALUE)));
        }

        // Confirm that no results are returned for business object data status explicitly set to VALID.
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertTrue(e.getMessage()
                .startsWith(String.format("Failed to find partition value which is the latest before partition value = \"%s\"", PARTITION_VALUE)));
        }

        // Generate partitions using latest before partition value filter option with business object data status set to UPLOADING.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            // Update request and generate partitions.
            request.getPartitionValueFilters().get(0).getLatestBeforePartitionValue().setPartitionValue(upperBoundPartitionValue);
            request.setBusinessObjectDataVersion(null);
            request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.UPLOADING);
            BusinessObjectDataDdl result = businessObjectDataService.generateBusinessObjectDataDdl(request);

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                NO_DATA_VERSION, BusinessObjectDataStatusEntity.UPLOADING, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL,
                TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE),
                AbstractServiceTest.NO_AS_OF_TIME), result);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestAfterPartitionValue()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Check an availability using a latest after partition value filter option.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS,
                    NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS,
                    NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE_2), AbstractServiceTest.NO_AS_OF_TIME),
                resultBusinessObjectDataDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestAfterPartitionValueWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare database entities required for testing.
        StorageUnitEntity storageUnitEntity = businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.UPLOADING);
        assertNotNull(businessObjectDataStatusEntity);
        storageUnitEntity.getBusinessObjectData().setStatus(businessObjectDataStatusEntity);
        businessObjectDataDao.saveAndRefresh(storageUnitEntity.getBusinessObjectData());

        // Create request with latest after partition value filter.
        BusinessObjectDataDdlRequest request =
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    new LatestAfterPartitionValue(PARTITION_VALUE_2))), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES);

        // Confirm that, by default, no results are returned since there are no VALID business object data instances.
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertTrue(e.getMessage()
                .startsWith(String.format("Failed to find partition value which is the latest after partition value = \"%s\"", PARTITION_VALUE_2)));
        }

        // Confirm that no results are returned for business object data status explicitly set to VALID.
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertTrue(e.getMessage()
                .startsWith(String.format("Failed to find partition value which is the latest after partition value = \"%s\"", PARTITION_VALUE_2)));
        }

        // Generate partitions using latest after partition value filter option with business object data status set to UPLOADING.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            // Update request and generate partitions.
            request.getPartitionValueFilters().get(0).getLatestAfterPartitionValue().setPartitionValue(lowerBoundPartitionValue);
            request.setBusinessObjectDataVersion(null);
            request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.UPLOADING);
            BusinessObjectDataDdl result = businessObjectDataService.generateBusinessObjectDataDdl(request);

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    BusinessObjectDataStatusEntity.UPLOADING, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                    NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE_2), AbstractServiceTest.NO_AS_OF_TIME),
                result);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingRequiredParameters()
    {
        BusinessObjectDataDdlRequest request;

        // Try to retrieve business object data ddl when business object definition name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when business object format usage parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when business object format file type parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition key is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when start partition value is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(BLANK_TEXT, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when start partition values is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A start partition value for the partition value range must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when end partition value is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, BLANK_TEXT, null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when end partition values is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An end partition value for the partition value range must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value list has no partition values specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(new ArrayList<>());
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value list has no partition values specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one partition value must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when one of the partition values in the partition value list is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when one of the partition values in the partition value list is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when the latest before partition value filter option has no partition value specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(NO_PARTITION_VALUES);
        for (String partitionValue : Arrays.asList(null, BLANK_TEXT))
        {
            request.getPartitionValueFilters().get(0).setLatestBeforePartitionValue(new LatestBeforePartitionValue(partitionValue));
            try
            {
                businessObjectDataService.generateBusinessObjectDataDdl(request);
                fail("Should throw an IllegalArgumentException when the latest before partition value filter option has no partition value specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("A partition value must be specified.", e.getMessage());
            }
        }

        // Try to retrieve business object data ddl when the latest after partition value filter option has no partition value specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(NO_PARTITION_VALUES);
        for (String partitionValue : Arrays.asList(null, BLANK_TEXT))
        {
            request.getPartitionValueFilters().get(0).setLatestAfterPartitionValue(new LatestAfterPartitionValue(partitionValue));
            try
            {
                businessObjectDataService.generateBusinessObjectDataDdl(request);
                fail("Should throw an IllegalArgumentException when the latest after partition value filter option has no partition value specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("A partition value must be specified.", e.getMessage());
            }
        }

        // Try to retrieve business object data ddl when standalone storage name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when standalone storage name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to check business object data availability when standalone storage name parameter value is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName(null);
        request.setStorageNames(Arrays.asList(BLANK_TEXT));
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when storage name parameter in the list of storage names is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when output format parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setOutputFormat(null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when output format parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An output format must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when table name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setTableName(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when table name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A table name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when maximum number of partitions in combined alter
        // table statement is specified without combineMultiplePartitionsInSingleAlterTable being enabled.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setCombineMultiplePartitionsInSingleAlterTable(NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE);
        request.setCombinedAlterTableMaxPartitions(COMBINED_ALTER_TABLE_MAX_PARTITIONS);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("CombineMultiplePartitionsInSingleAlterTable must be enabled when combinedAlterTableMaxPartitions is specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when maximum number of partitions in combined alter
        // table statement is zero or a negative integer.
        for (Integer combinedAlterTableMaxPartitions : Arrays.asList(0, -1))
        {
            request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
            request.setCombineMultiplePartitionsInSingleAlterTable(COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE);
            request.setCombinedAlterTableMaxPartitions(combinedAlterTableMaxPartitions);
            try
            {

                businessObjectDataService.generateBusinessObjectDataDdl(request);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Maximum number of partitions in combined alter table statement must be a positive integer.", e.getMessage());
            }
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueList()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request without optional parameters.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueListAllowMissingDataWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create business object data ddl request for without optional parameters, except for:
        // - allowMissingData is set to true
        // - business object data status for available business object data is set to UPLOADING (one of pre-registration statuses)
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.UPLOADING);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(true);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                MULTI_STORAGE_AVAILABLE_AS_UPLOADING_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueListAllowMissingDataWithBusinessObjectDataStatusSetToValid()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create business object data ddl request for without optional parameters, except for:
        // - allowMissingData is set to true
        // - business object data status for available business object data is set to VALID
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(true);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                MULTI_STORAGE_AVAILABLE_AS_VALID_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueListStandalonePartitionValueFilter()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl using request with a standalone partition value filter and without optional parameters.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.getPartitionValueFilter().setPartitionKey(BLANK_TEXT);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueListWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create business object data ddl request without optional parameters, except for setting business
        // object data status for available business object data to UPLOADING (one of the pre-registration statuses).
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(STORAGE_1_AVAILABLE_AS_UPLOADING_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.UPLOADING);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_AS_UPLOADING_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueListWithBusinessObjectDataStatusSetToValid()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create business object data ddl request without optional parameters, except
        // for setting business object data status for available business object data to VALID.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setAllowMissingData(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingOptionalParametersPartitionValueRange()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data ddl request without optional parameters.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setStorageName(null);
        request.setStorageNames(null);
        request.setIncludeDropTableStatement(null);
        request.setIncludeIfNotExistsOption(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestBeforePartitionValueNoStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Check an availability using a latest before partition value filter option and without specifying any storage.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                    DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                    NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION,
                    NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                    DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                    NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE), AbstractServiceTest.NO_AS_OF_TIME),
                resultBusinessObjectDataDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestAfterPartitionValueNoStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Check an availability using a latest after partition value filter option and without specifying any storage.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS,
                    NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

            // Validate the response object.
            assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS,
                    NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE_2), AbstractServiceTest.NO_AS_OF_TIME),
                resultBusinessObjectDataDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlTrimParametersPartitionValueList()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data ddl request with all string values requiring trimming.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(addWhitespace(BusinessObjectDataStatusEntity.VALID));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        for (int i = 0; i < request.getPartitionValueFilters().get(0).getPartitionValues().size(); i++)
        {
            request.getPartitionValueFilters().get(0).getPartitionValues()
                .set(i, addWhitespace(request.getPartitionValueFilters().get(0).getPartitionValues().get(i)));
        }
        request.setStorageName(addWhitespace(request.getStorageName()));
        request.setTableName(addWhitespace(request.getTableName()));
        request.setCustomDdlName(addWhitespace(request.getCustomDdlName()));
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES, SUBPARTITION_VALUES, false, true,
                true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlTrimParametersPartitionValueRange()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data ddl request with all string values requiring trimming.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataDdlRequest(addWhitespace(START_PARTITION_VALUE), addWhitespace(END_PARTITION_VALUE), addWhitespace(CUSTOM_DDL_NAME));
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(addWhitespace(BusinessObjectDataStatusEntity.VALID));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        request.setStorageName(addWhitespace(request.getStorageName()));
        request.setTableName(addWhitespace(request.getTableName()));
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlUpperCaseParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data ddl request with all parameter values in upper case (except for case-sensitive partition values).
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID.toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        request.setCustomDdlName(request.getCustomDdlName().toUpperCase());
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES,
            SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLowerCaseParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data ddl request with all parameter values in lower case (except for case-sensitive partition values).
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toLowerCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toLowerCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toLowerCase());
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.VALID.toLowerCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toLowerCase());
        request.setStorageName(request.getStorageName().toLowerCase());
        request.setCustomDdlName(request.getCustomDdlName().toLowerCase());
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES,
            SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlInvalidParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;

        // Try to retrieve business object data ddl using non-existing format.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(request.getNamespace(), request.getBusinessObjectDefinitionName(),
                        request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()),
                e.getMessage());
        }

        // Try to retrieve business object data ddl using non-existing business object data status.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(I_DO_NOT_EXIST);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to retrieve business object data ddl using neither VALID nor pre-registered status value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDataVersion(NO_DATA_VERSION);
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.INVALID);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data status specified in the request must be \"%s\" or one of the pre-registration statuses.",
                BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to retrieve business object data ddl using non-existing partition key (partition column).
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).setPartitionKey("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when non-existing partition key is used.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The partition key \"%s\" does not exist in first %d partition columns in the schema for business object format " +
                    "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d}.", request.getPartitionValueFilters().get(0).getPartitionKey(),
                BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, request.getNamespace(), request.getBusinessObjectDefinitionName(),
                request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }

        // Try to retrieve business object data ddl when both partition value filter and partition value filter list are specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setPartitionValueFilter(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME.toUpperCase(), new ArrayList<>(UNSORTED_PARTITION_VALUES), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when both a list of partition value filters and a standalone partition value filter are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of partition value filters and a standalone partition value filter cannot be both specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value filter has none or more than one partition value filter option specified.
        for (PartitionValueFilter partitionValueFilter : businessObjectDataServiceTestHelper.getInvalidPartitionValueFilters())
        {
            request = new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                Arrays.asList(partitionValueFilter), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION,
                NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES);

            try
            {
                businessObjectDataService.generateBusinessObjectDataDdl(request);
                fail("Should throw an IllegalArgumentException when partition value filter has more than one partition value filter option.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Exactly one partition value filter option must be specified.", e.getMessage());
            }
        }

        // Try to retrieve business object data ddl when partition value token is specified with a partition value range.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value token is specified with a partition value range.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value token is specified with a partition value range.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value token is specified with a partition value range.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(END_PARTITION_VALUE, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value filter has start partition value which is greater than the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(END_PARTITION_VALUE, START_PARTITION_VALUE, CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when start partition value which is greater than the end partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                    .format("The start partition value \"%s\" cannot be greater than the end partition value \"%s\".", END_PARTITION_VALUE, START_PARTITION_VALUE),
                e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value filter has specifies a range that results in no valid partition values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest("I_DO_NOT_EXIST_1", "I_DO_NOT_EXIST_2", null, CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value filter has specifies a range that results in no valid partition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                    "Partition value range [\"%s\", \"%s\"] contains no valid partition values in partition key group \"%s\". " + "Business object format: {%s}",
                    request.getPartitionValueFilters().get(0).getPartitionValueRange().getStartPartitionValue(),
                    request.getPartitionValueFilters().get(0).getPartitionValueRange().getEndPartitionValue(), PARTITION_KEY_GROUP,
                    businessObjectFormatServiceTestHelper
                        .getExpectedBusinessObjectFormatKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION)),
                e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value filter has specifies a range that results in no valid partition values.
        String invalidPartitionValue = "INVALID_PARTITION_VALUE_/";
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(Arrays.asList(invalidPartitionValue), CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value contains a '/' character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition value \"%s\" can not contain a '/' character.", request.getPartitionValueFilters().get(0).getPartitionValues().get(0)),
                e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value token is specified as a partition value.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataDdlRequest(Arrays.asList(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN), CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified as a partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of partition values.", e.getMessage());
        }

        // Try to retrieve business object data ddl when partition value token is specified as a partition value.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataDdlRequest(Arrays.asList(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN), CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified as a partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of partition values.", e.getMessage());
        }

        // Try to retrieve business object data ddl when both business object data version and business object data status values are specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDataVersion(DATA_VERSION);
        request.setBusinessObjectDataStatus(BDATA_STATUS);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version and business object data status cannot be both specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl when both a list of storage names and standalone storage name are specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName(BLANK_TEXT);
        request.setStorageNames(STORAGE_NAMES);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when both a list of storage names and standalone storage name are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of storage names and a standalone storage name cannot be both specified.", e.getMessage());
        }

        // Try to retrieve business object data ddl passing a non-existing storage as a standalone storage name.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setStorageName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing storage is used as a standalone storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageName()), e.getMessage());
        }

        // Try to retrieve business object data ddl passing a non-existing storage in the list of storage names.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName(null);
        request.setStorageNames(Arrays.asList("I_DO_NOT_EXIST"));
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing storage is used in the list of storage names.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageNames().get(0)), e.getMessage());
        }

        // Try to retrieve business object data ddl using non-existing custom ddl.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setCustomDdlName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when non-existing custom ddl is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", " +
                    "business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%d\".", request.getCustomDdlName(),
                request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlTwoPartitionValueRanges()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data ddl when two partition value ranges are specified.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        request.setPartitionValueFilters(partitionValueFilters);
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, null, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, null, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when more than one partition value range is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot specify more than one partition value range.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlDuplicatePartitionColumns()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data ddl using partition value filters with duplicate partition columns.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        request.setPartitionValueFilters(partitionValueFilters);
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME.toUpperCase(), new ArrayList<>(UNSORTED_PARTITION_VALUES), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME.toLowerCase(), new ArrayList<>(UNSORTED_PARTITION_VALUES), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition value filters use duplicate partition columns.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition value filters specify duplicate partition columns.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlNoSchemaExists()
    {
        // Prepare test data without schema.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, null, null, null, null, null, null,
            null, null, null, null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl when the business object format has no schema.
        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object format has no schema.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", " +
                    "format file type \"%s\", and format version \"%s\" doesn't have schema information.", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlPartitionColumnIsAlsoRegularColumn()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        // Override the first schema column to be a partition column.
        schemaColumns.set(0, partitionColumns.get(0));
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns, false, null, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(partitionColumns.size(), "ORGNL_PRTN_CLMN001", "DATE", CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueRangeFormatWithoutPartitionKeyGroup()
    {
        // Prepare test data with business object format having no partition key group value specified.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to generate business object data ddl using partition value range when business object format has no partition key group.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, END_PARTITION_VALUE, CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when generating ddl for a partition value range and " +
                "business object format has no partition key group.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A partition key group, which is required to use partition value ranges, " +
                    "is not specified for the business object format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueRangeExpectedPartitionValueMatchesMaxPartitionValueToken()
    {
        // Prepare test data with expected partition value set to the maximum partition value token.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        String startPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "a");
        String endPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "z");
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to generate business object data ddl when expected partition value matches to the maximum partition value token.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the maximum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueRangeExpectedPartitionValueMatchesMinPartitionValueToken()
    {
        // Prepare test data with expected partition value set to minimum partition value token.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        String startPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "a");
        String endPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "z");
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to generate business object data ddl when expected partition value matches to the maximum partition value token.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, CUSTOM_DDL_NAME);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the minimum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlSingleLevelPartitioningPartitionValueList()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null,
            true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);

        // Retrieve business object data ddl when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlSingleLevelPartitioningPartitionValueRange()
    {
        // Prepare test data without custom ddl and with partition key using NO_PARTITIONING_PARTITION_KEY.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY;
        partitionColumns.get(0).setName(partitionKey);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data ddl without specifying custom ddl name.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        request.getPartitionValueFilters().get(0).setPartitionKey(partitionKey);
        businessObjectDataService.generateBusinessObjectDataDdl(request);

        // TODO: Validate the results.
        businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
            CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true, true);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlTblPropertiesIncludeSingleLocationPartitionedTable()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE,
            partitionColumns.get(0).getName(), null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES,
            NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, SCHEMA_CUSTOM_TBL_PROPERTIES,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for partitioned table by
        // - specifying custom ddl name
        // - including single location
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        assertNull(request.getCustomDdlName());
        request.setIncludeSingleLocation(true);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl =
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(1, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT,
                CUSTOM_CLUSTERED_BY_VALUE, CUSTOM_TBL_PROPERTIES, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true,
                NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoPartitioning()
    {
        // Prepare non-partitioned test business object data with custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE,
            Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues,
            NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), null, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a non-partitioned table.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
            CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, STORAGE_1_AVAILABLE_PARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoPartitioningInvalidPartitionKey()
    {
        // Prepare non-partitioned test business object data with custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE,
            Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues,
            NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null, false, CUSTOM_DDL_NAME, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a business object data generate ddl request for a non-partitioned table with an invalid partition key value.
        BusinessObjectDataDdlRequest businessObjectDataDdlRequest =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        businessObjectDataDdlRequest.getPartitionValueFilters().get(0).setPartitionKey(INVALID_VALUE);
        businessObjectDataDdlRequest.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);

        // Try to retrieve business object data ddl.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(businessObjectDataDdlRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The partition key \"%s\" does not exist in first %d partition columns in the schema for business object format {" +
                    "namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", INVALID_VALUE, BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1,
                businessObjectDataDdlRequest.getNamespace(), businessObjectDataDdlRequest.getBusinessObjectDefinitionName(),
                businessObjectDataDdlRequest.getBusinessObjectFormatUsage(), businessObjectDataDdlRequest.getBusinessObjectFormatFileType(),
                businessObjectDataDdlRequest.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlNoPartitioning()
    {
        // Prepare test data without custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE,
            Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues,
            NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a non-partitioned table and without specifying custom ddl name.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
            CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSubpartitionKeysHaveHyphens()
    {
        // Prepare test data with subpartition using key values with hyphens instead of underscores.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey,
            PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES,
            SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, true, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.  Please note that we expect hyphens in subpartition key values.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            true, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlMissingSchemaDelimiterCharacterWithEscapeCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, null, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED ESCAPED BY '\\\\' NULL DEFINED AS '\\N'";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlMissingSchemaDelimiterCharacterWithoutEscapeCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, null, null, null, null, null,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED NULL DEFINED AS '\\N'";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlMissingSchemaEscapeCharacter()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null, null,
            null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' NULL DEFINED AS '\\N'";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlMissingSchemaNullValue()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, null,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS ''";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlEscapeSingleQuoteInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SINGLE_QUOTE, SINGLE_QUOTE, SINGLE_QUOTE,
            SINGLE_QUOTE, null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SINGLE_QUOTE, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'' ESCAPED BY '\\'' COLLECTION ITEMS TERMINATED BY '\\'' " +
            "MAP KEYS TERMINATED BY '\\'' NULL DEFINED AS '\\''";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlEscapeBackslashInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, BACKSLASH, BACKSLASH, BACKSLASH,
            BACKSLASH, null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, BACKSLASH, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results - please note that we do not escape single backslash in null value.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\' ESCAPED BY '\\\\' COLLECTION ITEMS TERMINATED BY '\\\\' " +
            "MAP KEYS TERMINATED BY '\\\\' NULL DEFINED AS '\\'";
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlUnprintableCharactersInRowFormat()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        // Set schemaDelimiterCharacter to char(1), schemaEscapeCharacter to char(10), and schemaNullValue to char(128).
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, String.valueOf((char) 1),
            String.valueOf((char) 2), String.valueOf((char) 3), String.valueOf((char) 10), null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
            String.valueOf((char) 128), schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request without business object format and data versions.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results - please note that 1 decimal = 1 octal, 10 decimal = 12 octal, and 128 decimal = 200 octal.
        String expectedRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' ESCAPED BY '\\012' COLLECTION ITEMS TERMINATED BY '\\002' " +
            "MAP KEYS TERMINATED BY '\\003' NULL DEFINED AS '\\200'";
        String expectedDdl =
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
                expectedRowFormat, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlWithCustomRowFormat()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, null, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, null, null,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlWithCustomClusteredByValue()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingBusinessObjectDataDoNotAllowMissingData()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl for the non-existing business object data with "allow missing data" flag set to "false".
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).setPartitionValues(Arrays.asList("I_DO_NOT_EXIST"));
        request.setAllowMissingData(false);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %s, partitionValue: \"%s\", " +
                    "subpartitionValues: \",,,\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion(),
                request.getPartitionValueFilters().get(0).getPartitionValues().get(request.getPartitionValueFilters().get(0).getPartitionValues().size() - 1),
                request.getBusinessObjectDataVersion(), request.getStorageName()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNotAvailableStorageUnitDoNotAllowMissingData()
    {
        // Prepare database entities required for testing.
        StorageUnitEntity storageUnitEntity = businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Generate DDL for a collection of business object data.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(Arrays.asList(PARTITION_VALUE));
        request.setIncludeDropPartitions(true);
        request.setAllowMissingData(false);
        BusinessObjectDataDdl result = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the response object.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE), AbstractServiceTest.NO_AS_OF_TIME), result);

        // Update the storage unit status to a non-available one.
        storageUnitEntity.setStatus(
            storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS, DESCRIPTION, NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET));
        herdDao.saveAndRefresh(storageUnitEntity);

        // Try to retrieve business object data ddl when storage unit has a non-available status and with "allow missing data" flag set to "false".
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an ObjectNotFoundException when business object data is not available.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %s, partitionValue: \"%s\", " +
                    "subpartitionValues: \",,,\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion(),
                request.getPartitionValueFilters().get(0).getPartitionValues().get(request.getPartitionValueFilters().get(0).getPartitionValues().size() - 1),
                request.getBusinessObjectDataVersion(), request.getStorageName()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingBusinessObjectDataAllowMissingDataSomeDataNoExists()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME,
            PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES,
            SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl when some of the business object data is not available and "allow missing data" flag is set to "true".
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).getPartitionValues().add("I_DO_NOT_EXIST");
        assertTrue(request.isAllowMissingData());
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingBusinessObjectDataAllowMissingDataAllDataNoExists()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                partitionColumns, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl when all of the business object data is not available and "allow missing data" flag is set to "true".
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(Arrays.asList("I_DO_NOT_EXIST"), CUSTOM_DDL_NAME);
        assertTrue(request.isAllowMissingData());
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlMissingBusinessObjectDataAllowMissingDataIncludeDropPartitionsDataNoExists()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Retrieve business object data ddl when all of the business object data is not available
        // and both "allow missing data" and "include drop partitions" flags are set to "true".
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(Arrays.asList(PARTITION_VALUE_2));
        request.setIncludeDropPartitions(true);
        request.setAllowMissingData(true);
        BusinessObjectDataDdl result = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the response object.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_VALUE_2, null), AbstractServiceTest.NO_AS_OF_TIME), result);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlNoPartitioningMissingBusinessObjectDataAllowMissingData()
    {
        // Prepare test data without custom ddl.  Please note that we do not use NO_PARTITIONING_PARTITION_VALUE for the test
        // business object data instance, so we can use that special value and find no data once we send a generate DDL request.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(PARTITION_VALUE), NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a non-partitioned table for a missing business object data instance and without specifying custom ddl name.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE));
        assertTrue(request.isAllowMissingData());
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
            CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlForwardSlashInPartitionColumnName()
    {
        // Prepare test data without custom ddl.
        String invalidPartitionColumnName = "INVALID_/_PRTN_CLMN";
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        partitionColumns.get(0).setName(invalidPartitionColumnName);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl for the format that uses unsupported schema column data type.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(invalidPartitionColumnName);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when partition column name contains a '/' character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Partition column name \"%s\" can not contain a '/' character. Business object format: " +
                    "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}", invalidPartitionColumnName, request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlNotSupportedSchemaColumnDataType()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        SchemaColumn schemaColumn = new SchemaColumn();
        schemaColumns.add(schemaColumn);
        schemaColumn.setName("COLUMN");
        schemaColumn.setType("UNKNOWN");
        String partitionKey = schemaColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, schemaColumnDaoTestHelper.getTestPartitionColumns(), false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl for the format that uses unsupported schema column data type.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object format has a column with an unsupported data type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Column \"%s\" has an unsupported data type \"%s\" in the schema for business object format " +
                        "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                        "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}. Exception : \"%s\"", schemaColumn.getName(), schemaColumn.getType(),
                    request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                    request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion(), "Internal error parsing position 0 of 'unknown'"),
                e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlAllKnownFileTypes()
    {
        // Create an S3 storage entity with the relative attributes.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Expected business object format file type to Hive file format mapping.
        HashMap<String, String> businessObjectFormatFileTypeMap = new HashMap<>();
        businessObjectFormatFileTypeMap.put(FileTypeEntity.BZ_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.GZ_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.ORC_FILE_TYPE, Hive13DdlGenerator.ORC_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.PARQUET_FILE_TYPE, Hive13DdlGenerator.PARQUET_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);
        businessObjectFormatFileTypeMap.put(FileTypeEntity.JSON_FILE_TYPE, Hive13DdlGenerator.JSON_HIVE_FILE_FORMAT);

        for (String businessObjectFormatFileType : businessObjectFormatFileTypeMap.keySet())
        {
            // Prepare test data for the respective business object format file type.
            List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                    SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                    SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                    schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : STORAGE_1_AVAILABLE_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), STORAGE_NAME);
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                // Create two storage files.
                for (int i = 0; i < 2; i++)
                {
                    storageFileDaoTestHelper
                        .createStorageFileEntity(storageUnitEntity, String.format("%s/data%d.dat", s3KeyPrefix, i), FILE_SIZE_1_KB, ROW_COUNT_1000);
                }

                herdDao.saveAndRefresh(storageUnitEntity);
                herdDao.saveAndRefresh(businessObjectDataEntity);
            }

            // Retrieve business object data ddl.
            BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

            // Validate the results.
            String expectedHiveFileFormat = businessObjectFormatFileTypeMap.get(businessObjectFormatFileType);
            String expectedDdl = businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                    null, expectedHiveFileFormat, businessObjectFormatFileType, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                    STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlStorageFilesWithoutPrefix()
    {
        // Create an S3 storage entity with the relative attributes.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Expected business object format file type to Hive file format mapping.
        Map<String, String> businessObjectFormatFileTypeMap = new HashMap<>();
        businessObjectFormatFileTypeMap.put(FileTypeEntity.BZ_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);

        for (String businessObjectFormatFileType : businessObjectFormatFileTypeMap.keySet())
        {
            // Prepare test data for the respective business object format file type.
            List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                    SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                    SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                    schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : STORAGE_1_AVAILABLE_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), STORAGE_NAME);
                // add s3KeyPrefix as storage directory
                StorageUnitEntity storageUnitEntity =
                    storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

                // Create two storage files without prefix
                for (int i = 0; i < 2; i++)
                {
                    storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, String.format("data%d.dat", i), FILE_SIZE_1_KB, ROW_COUNT_1000);
                }

                herdDao.saveAndRefresh(storageUnitEntity);
                herdDao.saveAndRefresh(businessObjectDataEntity);
            }

            // Retrieve business object data ddl.
            BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

            // Validate the results.
            String expectedHiveFileFormat = businessObjectFormatFileTypeMap.get(businessObjectFormatFileType);
            String expectedDdl = businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                    null, expectedHiveFileFormat, businessObjectFormatFileType, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                    STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlEmptyFolderMarkers()
    {
        // Create an S3 storage entity with the relative attributes.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Prepare test data for the respective business object format file type.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                partitionColumns);

        for (String partitionValue : STORAGE_1_AVAILABLE_PARTITION_VALUES)
        {
            // Create business object data.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, partitionValue,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

            // Get expected S3 key prefix for this business object data.
            String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), STORAGE_NAME);

            // Add s3KeyPrefix as storage directory.
            StorageUnitEntity storageUnitEntity =
                storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

            // Create storage files that represent empty folders - two with prefix and one without prefix.
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntity, String.format("%s%s", s3KeyPrefix, StorageFileEntity.S3_EMPTY_PARTITION), FILE_SIZE_1_KB,
                    ROW_COUNT_1000);
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, String.format("%s/", s3KeyPrefix), FILE_SIZE_1_KB, ROW_COUNT_1000);
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, StorageFileEntity.S3_EMPTY_PARTITION, FILE_SIZE_1_KB, ROW_COUNT_1000);

            // Persist the entities.
            herdDao.saveAndRefresh(storageUnitEntity);
            herdDao.saveAndRefresh(businessObjectDataEntity);
        }

        // Retrieve business object data ddl.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoCustomDdlNotSupportedFileType()
    {
        // Prepare test data without custom ddl.
        String businessObjectFileType = "UNKNOWN";
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(businessObjectFileType, PARTITION_KEY, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, null,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false, null,
            true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl for the format without custom ddl and that uses unsupported file type.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatFileType(businessObjectFileType);
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(request);
            fail("Should throw an IllegalArgumentException when business object format has an unsupported file type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Unsupported format file type for business object format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", request.getNamespace(),
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoDropTable()
    {
        // Prepare test data without custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request without drop table statement.
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        request.setIncludeDropTableStatement(false);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl =
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
                ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, false, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlFilterOnSubPartitionValues()
    {
        List<SchemaColumn> columns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();

        // Test generate business object data ddl using primary partition and each of the available subpartition columns.
        for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, PARTITION_COLUMNS.length); i++)
        {
            // Prepare test data without custom ddl.
            businessObjectDataServiceTestHelper
                .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, i + 1,
                    STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
                    SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

            // Retrieve business object data ddl request without drop table statement.
            BusinessObjectDataDdlRequest request =
                businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(STORAGE_1_AVAILABLE_PARTITION_VALUES);
            request.getPartitionValueFilters().get(0).setPartitionKey(partitionColumns.get(i).getName());
            request.setIncludeDropPartitions(INCLUDE_DROP_PARTITIONS);
            request.setAllowMissingData(NO_ALLOW_MISSING_DATA);
            BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

            // Validate the results.
            String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
                FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, i + 1, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true, INCLUDE_DROP_PARTITIONS,
                NO_INCLUDE_SINGLE_LOCATION);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlUsingStorageDirectoriesNoAutoDiscovery()
    {
        // Prepare test data with storage units having no storage files, but only the relative storage directory path values.
        // For auto-discovery not to occur, number of partition columns is equal to the number of partition values.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1 + SUBPARTITION_VALUES.size());
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                partitionColumns, false, CUSTOM_DDL_NAME, false, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl =
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
                CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlStorageDirectoryMismatchesS3KeyPrefix()
    {
        // Prepare test data with a storage unit having no storage files and storage directory path value not matching the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE, invalidS3KeyPrefix);

        // Try to retrieve business object data ddl when storage unit has no storage
        // files and storage directory path not matching the expected S3 key prefix.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Should throw an IllegalArgumentException when storage directory path does not match the expected S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage directory path \"%s\" registered with business object data {%s} " +
                    "in \"%s\" storage does not match the expected S3 key prefix \"%s\".", invalidS3KeyPrefix, businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                        PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME,
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, PARTITION_VALUE, null, null, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlStorageDirectoryIsNull()
    {
        // Prepare test data with a storage unit having no storage files and storage directory path is null.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE, null);

        // Try to retrieve business object data ddl when storage unit has no storage files and storage directory path is null.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Should throw an IllegalArgumentException when storage directory path is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage directory path \"%s\" registered with business object data {%s} " +
                    "in \"%s\" storage does not match the expected S3 key prefix \"%s\".", null, businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                        PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME,
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, PARTITION_VALUE, null, null, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestFormatVersionDataNotAvailableInStorage()
    {
        // Create database entities for two versions of a business object format.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1 + SUBPARTITION_VALUES.size());
        BusinessObjectFormatEntity businessObjectFormatV0Entity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);
        BusinessObjectFormatEntity businessObjectFormatV1Entity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

        // Create two storage entities.
        StorageEntity storage1Entity = storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE);
        StorageEntity storage2Entity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2);

        // Register initial version of the business object data for initial format version in both storages.
        BusinessObjectDataEntity businessObjectDataV0V0Entity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatV0Entity,
            businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataV0V0Entity), storage1Entity.getName());

        for (StorageEntity storageEntity : Arrays.asList(storage1Entity, storage2Entity))
        {
            StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                .createStorageUnitEntity(storageEntity, businessObjectDataV0V0Entity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
            storageUnitEntity.setDirectoryPath(s3KeyPrefix);
            herdDao.saveAndRefresh(storageUnitEntity);
        }
        herdDao.saveAndRefresh(businessObjectDataV0V0Entity);

        // Register initial version of the business object data for second format version, but only in the second storage.
        BusinessObjectDataEntity businessObjectDataV1V0Entity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatV1Entity,
            businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataV1V0Entity), storage2Entity.getName());

        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storage2Entity, businessObjectDataV1V0Entity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageUnitEntity.setDirectoryPath(s3KeyPrefix);
        herdDao.saveAndRefresh(storageUnitEntity);
        herdDao.saveAndRefresh(businessObjectDataV1V0Entity);

        // Retrieve business object data ddl for the first storage without specifying business object format version.
        BusinessObjectDataDdlRequest request = new BusinessObjectDataDdlRequest();

        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(null);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilter(partitionValueFilter);
        partitionValueFilter.setPartitionKey(FIRST_PARTITION_COLUMN_NAME);
        partitionValueFilter.setPartitionValues(Arrays.asList(PARTITION_VALUE));

        request.setBusinessObjectDataVersion(INITIAL_DATA_VERSION);
        request.setStorageName(StorageEntity.MANAGED_STORAGE);
        request.setOutputFormat(BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL);
        request.setTableName(TABLE_NAME);
        request.setCustomDdlName(null);
        request.setIncludeDropTableStatement(true);

        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        assertNotNull(resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLargePartitionValueListPrimaryPartitionOnly()
    {
        final int PRIMARY_PARTITION_VALUES_SIZE = 10000;

        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl by passing a large set of partition values.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < PRIMARY_PARTITION_VALUES_SIZE; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(partitionValues, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);

        // Retrieve business object data ddl when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataDdl(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(), resultDdl);
    }

    @Test
    @Ignore
    public void testGenerateBusinessObjectDataDdlLargePartitionValueListWithAutoDiscovery()
    {
        final int PRIMARY_PARTITION_VALUE_LIST_SIZE = 10000;
        final int SECOND_LEVEL_PARTITION_VALUES_PER_BUSINESS_OBJECT_DATA = 1;
        final int STORAGE_FILES_PER_PARTITION = 1;

        // Prepare test data and build a list of partition values to generate business object data DDL for.

        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(new SchemaColumn(PARTITION_KEY, "DATE", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME, "NUMBER", COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME_2, "STRING", NO_COLUMN_SIZE, NO_COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));

        // Use the first two columns as partition columns.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 2);

        // Create a business object format entity with the schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                    null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

        // Create an S3 storage entity.
        StorageEntity storageEntity = storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                S3_BUCKET_NAME);

        // Create relative business object data, storage unit, and storage file entities.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < PRIMARY_PARTITION_VALUE_LIST_SIZE; i++)
        {
            String partitionValue = String.format("%s-%03d", PARTITION_VALUE, i);
            partitionValues.add(partitionValue);

            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, partitionValue, NO_SUBPARTITION_VALUES, DATA_VERSION, true,
                    BusinessObjectDataStatusEntity.VALID);

            // Build an S3 key prefix according to the herd S3 naming convention.
            String s3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_KEY,
                    partitionValue, null, null, DATA_VERSION);

            // Create a storage unit with a storage directory path.
            StorageUnitEntity storageUnitEntity =
                storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

            // Create storage file entities.
            for (int j = 0; j < SECOND_LEVEL_PARTITION_VALUES_PER_BUSINESS_OBJECT_DATA; j++)
            {
                // Build a storage file directory path that includes the relative second level partition value - needed for auto discovery.
                String storageFileDirectoryPath = String.format("%s/%s=%s-%03d", s3KeyPrefix, COLUMN_NAME, PARTITION_VALUE_2, j);

                for (int k = 0; k < STORAGE_FILES_PER_PARTITION; k++)
                {
                    String storageFilePath = String.format("%s/%03d.data", storageFileDirectoryPath, k);
                    storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, storageFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
                }
            }

            herdDao.saveAndRefresh(storageUnitEntity);
        }

        // Retrieve business object data ddl for the entire list of partition values.
        BusinessObjectDataDdl businessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, partitionValues, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION,
                NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the results.
        assertNotNull(businessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestBeforePartitionValueNoExists()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Try to retrieve business object data ddl using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(PARTITION_VALUE), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when failed to find the latest before partition value.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Failed to find partition value which is the latest before partition value = \"%s\" " +
                    "for partition key = \"%s\" due to no available business object data " +
                    "in \"%s\" storage that satisfies the search criteria. Business object data {namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", PARTITION_VALUE, FIRST_PARTITION_COLUMN_NAME, STORAGE_NAME, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, DATA_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlLatestAfterPartitionValueNoExists()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Try retrieve business object data ddl using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(PARTITION_VALUE_2))), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS,
                    NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when failed to find the latest after partition value.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Failed to find partition value which is the latest after partition value = \"%s\" " +
                    "for partition key = \"%s\" due to no available business object data " +
                    "in \"%s\" storage that satisfies the search criteria. Business object data {namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", PARTITION_VALUE_2, FIRST_PARTITION_COLUMN_NAME, STORAGE_NAME, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, DATA_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitMultipleStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified multiple storage with request having:
        // - business object data version set to a valid value (this forces to select business object data regardless of business object data status)
        // - allowMissingData is set to true
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, STORAGE_NAMES, NO_STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION,
                NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl =
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
                ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false, true,
                true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, STORAGE_NAMES, NO_STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl, AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitMultipleStorageWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified multiple storage with request having:
        // - business object data version is not set
        // - business object data status for available business object data is set to UPLOADING (one of the pre-registration statuses)
        // - allowMissingData is set to true
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.UPLOADING,
                STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, MULTI_STORAGE_AVAILABLE_AS_UPLOADING_PARTITION_VALUES_UNION, SUBPARTITION_VALUES,
                false, true, true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.UPLOADING,
                STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl,
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitMultipleStorageWithBusinessObjectDataStatusSetToValid()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified multiple storage with request having:
        // - business object data version is not set
        // - business object data status for available business object data is set to VALID
        // - allowMissingData is set to true
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID,
                STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, MULTI_STORAGE_AVAILABLE_AS_VALID_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false,
                true, true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID,
                STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl,
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitSingleStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified single storage with request having:
        // - business object data version set to a valid value (this forces to select business object data regardless of business object data status)
        // - allowMissingData is set to true
        // - storage is set to the first storage
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME),
                NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME),
                NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl,
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitSingleStorageWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified single storage with request having:
        // - business object data version is not set
        // - business object data status for available business object data is set to UPLOADING (one of the pre-registration statuses)
        // - allowMissingData is set to true
        // - storage is set to the first storage
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.UPLOADING,
                Arrays.asList(STORAGE_NAME), NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_AS_UPLOADING_PARTITION_VALUES, SUBPARTITION_VALUES, false, true,
                true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.UPLOADING,
                Arrays.asList(STORAGE_NAME), NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl,
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlExplicitSingleStorageWithBusinessObjectDataStatusSetToValid()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified single storage with request having:
        // - business object data version is not set
        // - business object data status for available business object data is set to VALID
        // - allowMissingData is set to true
        // - storage is set to the first storage
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID,
                Arrays.asList(STORAGE_NAME), NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES, SUBPARTITION_VALUES, false, true,
                true);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID,
                Arrays.asList(STORAGE_NAME), NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl,
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoStorageNamesAndSameBusinessObjectDataInMultipleStorageEntities()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false,
                CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data ddl when storage names are not specified and the same business object data is registered
        // in multiple storage entities and with both business object format and business object data versions are specified in the request.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Collections
                    .singletonList(new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an IllegalArgumentException when business object data registered in more than one storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found business object data registered in more than one storage. " +
                "Please specify storage(s) in the request to resolve this. Business object data {%s}", businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_INTERSECTION.get(0), SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }

        // For functional coverage of StorageUnitDaoImpl.getStorageUnitsByPartitionFilters() method in DAO layer,
        // repeat the same request, but without specifying business object format and business object data versions.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, NO_FORMAT_VERSION, Collections
                    .singletonList(new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an IllegalArgumentException when business object data registered in more than one storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found business object data registered in more than one storage. " +
                "Please specify storage(s) in the request to resolve this. Business object data {%s}", businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_INTERSECTION.get(0), SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    /**
     * This test case reproduces an error when business object data has more or equal sub-partition values then the latest business object format version.
     */
    @Test
    public void testGenerateBusinessObjectDataDdlLatestFormatHasLessPartitionColumnsThenBusinessObjectData()
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(new SchemaColumn(PARTITION_KEY, "DATE", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME, "NUMBER", COLUMN_SIZE, NO_COLUMN_REQUIRED, COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION));

        // Build two list of partition columns, so the second format version would have one less partition column defined.
        List<SchemaColumn> partitionColumns1 = schemaColumns.subList(0, 2);
        List<SchemaColumn> partitionColumns2 = schemaColumns.subList(0, 1);

        // Create an initial version of business object format with the schema having two partition columns.
        BusinessObjectFormatEntity businessObjectFormatEntity1 = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns1);

        // Create a second version of business object format with the schema having only one partition column.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns2);

        // Provide sub-partition column name and value.
        List<SchemaColumn> subPartitionColumns = partitionColumns1.subList(1, 2);
        List<String> subPartitionValues = Arrays.asList(SUBPARTITION_VALUES.get(0));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity1, PARTITION_VALUE, subPartitionValues, DATA_VERSION, true,
                BusinessObjectDataStatusEntity.VALID);

        // Create an S3 storage entity.
        // Add the "bucket name" attribute to the storage along with the key prefix velocity template.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Build an S3 key prefix according to the Data Management S3 naming convention.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, subPartitionColumns.toArray(new SchemaColumn[subPartitionColumns.size()]),
                subPartitionValues.toArray(new String[subPartitionValues.size()]), DATA_VERSION);

        // Create a storage unit with a storage directory path.
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

        // Try to retrieve business object data DDL when business object data has more
        // or equal sub-partition values then the latest business object format version.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, null, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES,
                    STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                    INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an IllegalArgumentException when business object data has more or " +
                "equal sub-partition values then the latest business object format version.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Number of subpartition values specified for the business object data is greater than or equal to " +
                    "the number of partition columns defined in the schema of the business object format selected for DDL/Partitions generation. " +
                    "Business object data: {%s},  business object format: {%s}", businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION,
                        PARTITION_VALUE, subPartitionValues, DATA_VERSION), businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION)),
                e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionValid()
    {
        //
        List<List<String>> testPartitions =
            Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2));

        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(testPartitions);

        // Retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA, INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlTwoPartitionLevels(testPartitions), AbstractServiceTest.NO_AS_OF_TIME),
            resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionDeleted()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to DELETED.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.DELETED));

        // Retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA, INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Only the first sub-partition should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdlTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1))),
            AbstractServiceTest.NO_AS_OF_TIME), resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalid()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to INVALID.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.INVALID));

        // Try to retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when second sub-partition has an INVALID status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUE_2, DATA_VERSION, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionValidNonAvailableStorageUnit()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition storage unit status to DISABLED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.DISABLED));

        // Try to retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when second sub-partition has a non-available storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUE_2, DATA_VERSION, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionValidNonAvailableStorageUnitBdataArchived()
    {
        // Create two VALID sub-partitions both with "available" storage units.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition storage unit status to ARCHIVED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ARCHIVED));

        // Try to retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when second sub-partition has a non-available storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUE_2, DATA_VERSION, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalidBdataArchived()
    {
        // Create two VALID sub-partitions both with "available" storage units.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to INVALID.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.INVALID));

        // Update the second sub-partition storage unit status to ARCHIVED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ARCHIVED));

        // Try to retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail("Suppose to throw an ObjectNotFoundException when second sub-partition has a non-available storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE, SUB_PARTITION_VALUE_2, DATA_VERSION, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeAllRegisteredSubPartitionsSecondSubPartitionDeletedBdataArchived()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to DELETED.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.DELETED));

        // Update the second sub-partition storage unit status to ARCHIVED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ARCHIVED));

        // Retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA, INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE,
                AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should. Only the first sub-partition should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdlTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1))),
            AbstractServiceTest.NO_AS_OF_TIME), resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlDoNotIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalid()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to INVALID.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.INVALID));

        // Retrieve business object data DDL with "IncludeAllRegisteredSubPartitions" option disabled.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Only the first sub-partition should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdlTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1))),
            AbstractServiceTest.NO_AS_OF_TIME), resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSuppressScanForUnregisteredSubPartitions()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
            Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdlTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2))),
            AbstractServiceTest.NO_AS_OF_TIME), resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSuppressScanForUnregisteredSubPartitionsNoDirectoryPath()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update both storage units to remove the directory path values.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            storageUnitEntity.setDirectoryPath(NO_STORAGE_DIRECTORY_PATH);
        }

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdlTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2))),
            AbstractServiceTest.NO_AS_OF_TIME), resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSuppressScanForUnregisteredSubPartitionsDirectoryPathMismatchS3KeyPrefix()
    {
        // Create one VALID sub-partition with an "available" storage unit a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)));

        // Save the original S3 key prefix (directory path) and update the storage unit with a directory path that does not match the S3 key prefix.
        String originalS3KeyPrefix = storageUnitEntities.get(0).getDirectoryPath();
        storageUnitEntities.get(0).setDirectoryPath(INVALID_VALUE);

        // Try to retrieve business object data DDL with flag set to suppress scan for unregistered
        // sub-partitions when its storage unit directory path does not match the expected S3 key prefix.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage directory path \"%s\" registered with business object data {%s} in \"%s\" storage does not match " +
                "the expected S3 key prefix \"%s\".", INVALID_VALUE, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUB_PARTITION_VALUE_1), DATA_VERSION)), STORAGE_NAME, originalS3KeyPrefix), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSuppressScanForUnregisteredSubPartitionsLatestFormatHasLessPartitionColumnsThenBusinessObjectData()
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(new SchemaColumn(PARTITION_KEY, "DATE", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME, "NUMBER", COLUMN_SIZE, NO_COLUMN_REQUIRED, COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION));

        // Build two list of partition columns, so the second format version would have one less partition column defined.
        List<SchemaColumn> partitionColumns1 = schemaColumns.subList(0, 2);
        List<SchemaColumn> partitionColumns2 = schemaColumns.subList(0, 1);

        // Create an initial version of business object format with the schema having two partition columns.
        BusinessObjectFormatEntity businessObjectFormatEntity1 = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns1);

        // Create a second version of business object format with the schema having only one partition column.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null,
                null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns2);

        // Provide sub-partition column name and value.
        List<SchemaColumn> subPartitionColumns = partitionColumns1.subList(1, 2);
        List<String> subPartitionValues = Arrays.asList(SUBPARTITION_VALUES.get(0));

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity1, PARTITION_VALUE, subPartitionValues, DATA_VERSION, LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID);

        // Create an S3 storage entity.
        // Add the "bucket name" attribute to the storage along with the key prefix velocity template.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Build an S3 key prefix according to the Data Management S3 naming convention.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, subPartitionColumns.toArray(new SchemaColumn[subPartitionColumns.size()]),
                subPartitionValues.toArray(new String[subPartitionValues.size()]), DATA_VERSION);

        // Create a storage unit with a storage directory path.
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

        // Try to retrieve business object data DDL when flag is set to suppress scan for unregistered sub-partitions
        // and latest format has less partition columns then the business object data is registered with.
        try
        {
            businessObjectDataService.generateBusinessObjectDataDdl(
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, null, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES,
                    STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                    INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Number of primary and sub-partition values (2) specified for the business object data is not equal to " +
                    "the number of partition columns (1) defined in the schema of the business object format selected for DDL/Partitions generation. " +
                    "Business object data: {%s},  business object format: {%s}", businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION,
                        PARTITION_VALUE, subPartitionValues, DATA_VERSION), businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatKeyAsString(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION)),
                e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithoutCombinedAlterTableMaxPartitionsAndPartitionCountEqualsTo1()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(Arrays.asList(PARTITION_VALUE),
                        Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)), 1), AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithoutCombinedAlterTableMaxPartitionsAndPartitionCountGreaterThan1()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays
            .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(
                    Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), Arrays
                        .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                            Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)), null),
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithCombinedAlterTableMaxPartitionsEqualTo1()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays
            .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, 1, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(
                    Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), Arrays
                        .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                            Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)), 1),
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithCombinedAlterTableMaxPartitionsEqualTo2AndEqualChunks()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays
            .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4),
                Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_3),
                Arrays.asList(PARTITION_VALUE_4, SUB_PARTITION_VALUE_2), Arrays.asList(PARTITION_VALUE_4, SUB_PARTITION_VALUE_4)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4),
                    NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION,
                NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, 2, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4),
                    NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(
                    Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4), Arrays
                        .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                            Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4),
                            Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_3),
                            Arrays.asList(PARTITION_VALUE_4, SUB_PARTITION_VALUE_2), Arrays.asList(PARTITION_VALUE_4, SUB_PARTITION_VALUE_4)), 2),
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithCombinedAlterTableMaxPartitionsEqualTo2AndUnequalChunks()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays
            .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4),
                Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_1)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3),
                    NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION,
                NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, 2, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3),
                    NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                NO_DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(
                    Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2, PARTITION_VALUE_3), Arrays
                        .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                            Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4),
                            Arrays.asList(PARTITION_VALUE_3, SUB_PARTITION_VALUE_1)), 2), AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCombineMultiplePartitionsInSingleAlterTableWithCombinedAlterTableMaxPartitionsGreaterThanPartitionsCount()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays
            .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)));

        // Retrieve business object data DDL with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                AbstractServiceTest.COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, 1000, AbstractServiceTest.NO_AS_OF_TIME,
                AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object. Both sub-partitions should be present in the generated DDL.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION, NO_BDATA_STATUS,
                NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlTwoPartitionLevelsWithMultiplePartitionsInSingleAlterTableStatement(
                    Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2), Arrays
                        .asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2),
                            Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_3), Arrays.asList(PARTITION_VALUE_2, SUB_PARTITION_VALUE_4)), null),
                AbstractServiceTest.NO_AS_OF_TIME);
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWithAsOfTimeNoValidDataFound()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data ddl request with all parameter values in upper case (except for case-sensitive partition values).
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        request.setCustomDdlName(request.getCustomDdlName().toUpperCase());

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);

        BusinessObjectDataDdl resultDdl = null;

        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(cal.getTime()));
        resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);
        assertEquals(resultDdl.getAsOfTime(), request.getAsOfTime());

        String expectedDdlStr = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.NO_PARTITION_VALUES, AbstractServiceTest.NO_SUBPARTITION_VALUES, false, true, true);

        // Validate the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWithAsOfTime()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, null, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request with all parameter values in upper case (except for case-sensitive partition values).
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, null);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        request.setCustomDdlName(null);
        request.setBusinessObjectDataVersion(null);

        Calendar cal = Calendar.getInstance();
        java.util.Date dateTime = cal.getTime();
        request.setAsOfTime(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        String expectedDdlStr = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, "ROW FORMAT " + AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES,
                AbstractServiceTest.SUBPARTITION_VALUES, false, true, true);

        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl);
        assertEquals(resultDdl.getAsOfTime(), request.getAsOfTime());

        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, NO_CUSTOM_DDL_NAME, NO_INCLUDE_SINGLE_LOCATION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, AbstractServiceTest.DATA_VERSION + 99);

        // Validate the results.
        String expectedDllStrVersion2 =
            expectedDdlStr.replaceAll(AbstractServiceTest.DATA_VERSION.toString(), Integer.toString((AbstractServiceTest.DATA_VERSION + 99)));

        // NO AS of Time param set, the latest version is version 2
        request.setAsOfTime(null);
        BusinessObjectDataDdl resultDdl2 = businessObjectDataService.generateBusinessObjectDataDdl(request);

        assertEquals(resultDdl2.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDllStrVersion2, resultDdl2);
        // Version 1 should be returned
        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime));
        BusinessObjectDataDdl resultDdl3 = businessObjectDataService.generateBusinessObjectDataDdl(request);

        assertEquals(resultDdl3.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl3);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWithAsOfTimeNoValidStorageUnitFound()
    {
        // Prepare database entities required for testing.
        StorageUnitEntity storageUnitEntity = businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Update the storage unit status to ARCHIVED.
        storageUnitEntity.setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ARCHIVED));

        try
        {
            BusinessObjectDataDdlRequest ddlRequest =
                new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, NO_DATA_VERSION,
                    NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                    AbstractServiceTest.NO_AS_OF_TIME, AbstractServiceTest.NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES);

            Calendar cal = Calendar.getInstance();
            java.util.Date dateTime = cal.getTime();
            ddlRequest.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime));
            businessObjectDataService.generateBusinessObjectDataDdl(ddlRequest);

            fail("Suppose to throw an ObjectNotFoundException when business object data has a non-available storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, PARTITION_VALUE, ",,,", null, STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWithAsOfTimeWithBusinessObjectDataStatusSetToUploading()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, null, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request with all parameter values in upper case (except for case-sensitive partition values).
        // The request will have business object data status set to UPLOADING (one of the pre-registration statuses).
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, null);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.setBusinessObjectDataStatus(BusinessObjectDataStatusEntity.UPLOADING);
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        request.setCustomDdlName(null);
        request.setBusinessObjectDataVersion(null);

        Calendar cal = Calendar.getInstance();
        java.util.Date dateTime = cal.getTime();
        request.setAsOfTime(null);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        String expectedDdlStr = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, "ROW FORMAT " + AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.STORAGE_1_AVAILABLE_AS_UPLOADING_PARTITION_VALUES,
                AbstractServiceTest.SUBPARTITION_VALUES, false, true, true);

        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl);
        assertEquals(resultDdl.getAsOfTime(), request.getAsOfTime());

        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, NO_CUSTOM_DDL_NAME, NO_INCLUDE_SINGLE_LOCATION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, AbstractServiceTest.DATA_VERSION + 99);

        // Validate the results.
        String expectedDllStrVersion2 =
            expectedDdlStr.replaceAll(AbstractServiceTest.DATA_VERSION.toString(), Integer.toString((AbstractServiceTest.DATA_VERSION + 99)));

        // NO AS of Time param set, the latest version is version 2
        request.setAsOfTime(null);
        BusinessObjectDataDdl resultDdl2 = businessObjectDataService.generateBusinessObjectDataDdl(request);

        assertEquals(resultDdl2.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDllStrVersion2, resultDdl2);
        // Version 1 should be returned
        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime));
        BusinessObjectDataDdl resultDdl3 = businessObjectDataService.generateBusinessObjectDataDdl(request);

        assertEquals(resultDdl3.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl3);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWithAsOfTimeWithMultipleStatusHistoryEntries()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, null, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl request with all parameter values in upper case (except for case-sensitive partition values).
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, null);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        request.setCustomDdlName(null);
        request.setBusinessObjectDataVersion(null);

        String expectedDdlStr = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, "ROW FORMAT " + AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.STORAGE_1_AVAILABLE_AS_VALID_PARTITION_VALUES,
                AbstractServiceTest.SUBPARTITION_VALUES, false, true, true);

        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, NO_CUSTOM_DDL_NAME, NO_INCLUDE_SINGLE_LOCATION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, AbstractServiceTest.DATA_VERSION + 99);

        Calendar cal = Calendar.getInstance();
        java.util.Date dateTime = cal.getTime();
        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime));
        for (String partitionValue : UNSORTED_PARTITION_VALUES)
        {
            BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
            businessObjectDataKey.setNamespace(AbstractServiceTest.NAMESPACE);
            businessObjectDataKey.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
            businessObjectDataKey.setBusinessObjectDefinitionName(AbstractServiceTest.BDEF_NAME);
            businessObjectDataKey.setBusinessObjectFormatUsage(AbstractServiceTest.FORMAT_USAGE_CODE);
            businessObjectDataKey.setPartitionValue(partitionValue);
            businessObjectDataKey.setSubPartitionValues(AbstractServiceTest.SUBPARTITION_VALUES);
            businessObjectDataKey.setBusinessObjectDataVersion(AbstractServiceTest.DATA_VERSION + 99);

            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
            businessObjectDataDaoTestHelper.updateBusinessObjectDataEntityStatus(businessObjectDataEntity, BusinessObjectDataStatusEntity.INVALID);
        }
        // create dateTime2, which is before version 3 is created and version 2 last status history status is INVALID
        java.util.Date dateTime2 = Calendar.getInstance().getTime();

        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                AbstractServiceTest.UNSORTED_PARTITION_VALUES, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, AbstractServiceTest.SCHEMA_MAP_KEYS_DELIMITER_HASH,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT,
                AbstractServiceTest.SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, NO_CUSTOM_DDL_NAME, NO_INCLUDE_SINGLE_LOCATION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, AbstractServiceTest.DATA_VERSION + 199);

        String expectedDllStrVersion2 =
            expectedDdlStr.replaceAll(AbstractServiceTest.DATA_VERSION.toString(), Integer.toString((AbstractServiceTest.DATA_VERSION + 99)));

        BusinessObjectDataDdl resultDdl2 = businessObjectDataService.generateBusinessObjectDataDdl(request);
        // Validate the results.
        assertEquals(resultDdl2.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDllStrVersion2, resultDdl2);

        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime2));
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);
        // Validate the results.
        assertEquals(resultDdl.getAsOfTime(), request.getAsOfTime());
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdlStr, resultDdl);

        // Request data version 2 specifically
        request.setBusinessObjectDataVersion(AbstractServiceTest.DATA_VERSION + 99);
        request.setAsOfTime(HerdDateUtils.getXMLGregorianCalendarValue(dateTime2));
        BusinessObjectDataDdl resultDdl3 = businessObjectDataService.generateBusinessObjectDataDdl(request);
        // Validate the results.
        assertEquals(resultDdl3.getAsOfTime(), request.getAsOfTime());
        String expectedDdlStr2 = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, "ROW FORMAT " + AbstractServiceTest.SCHEMA_CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.STORAGE_1_AVAILABLE_PARTITION_VALUES,
                AbstractServiceTest.SUBPARTITION_VALUES, false, true, true);
        expectedDllStrVersion2 =
            expectedDdlStr2.replaceAll(AbstractServiceTest.DATA_VERSION.toString(), Integer.toString((AbstractServiceTest.DATA_VERSION + 99)));
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDllStrVersion2, resultDdl3);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationNonPartitionedTable()
    {
        // Prepare non-partitioned test business object data with custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                null, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a non-partitioned table by
        // - specifying custom ddl name
        // - including single location (no impact is expected for non-partitioned tables since their DDL contains location statement regardless of this flag)
        // - not allowing missing data
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        assertNotNull(request.getCustomDdlName());
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(false);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
            CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, STORAGE_1_AVAILABLE_PARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationNonPartitionedTableNoAvailableData()
    {
        // Prepare non-partitioned test business object data with custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                null, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a new storage, so we can specify it in the request and result in no data being available for non-partitioned table.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Retrieve business object data ddl for a non-partitioned table by
        // - specifying custom ddl name
        // - including single location (no impact is expected for non-partitioned tables since their DDL contains location statement regardless of this flag)
        // - allowing missing data and specifying storage that has no available data
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        assertNotNull(request.getCustomDdlName());
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(true);
        request.setStorageName(STORAGE_NAME_3);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT,
            CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, NO_PARTITION_VALUES, STORAGE_1_AVAILABLE_PARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationNonPartitionedTableNoCustomDdl()
    {
        // Prepare non-partitioned test business object data without custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a non-partitioned table by
        // - not specifying custom ddl name
        // - including single location (no impact is expected for non-partitioned tables since their DDL contains location statement regardless of this flag)
        // - not allowing missing data
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        assertNull(request.getCustomDdlName());
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(false);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE,
            CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationNonPartitionedTableNoCustomDdlNoAvailableData()
    {
        // Prepare non-partitioned test business object data without custom ddl.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a new storage, so we can specify it in the request and result in no data being available for non-partitioned table.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Retrieve business object data ddl for a non-partitioned table by
        // - not specifying custom ddl name
        // - including single location (no impact is expected for non-partitioned tables since their DDL contains location statement regardless of this flag)
        // - allowing missing data and specifying storage that has no available data
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        assertNull(request.getCustomDdlName());
        request.getPartitionValueFilters().get(0).setPartitionKey(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY);
        request.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(true);
        request.setStorageName(STORAGE_NAME_3);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(0, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null,
                Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                NO_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationPartitionedTable()
    {
        // Prepare test data with custom ddl.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
                schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, INCLUDE_SINGLE_LOCATION, LATEST_VERSION_FLAG_SET,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, DATA_VERSION);

        // Retrieve business object data ddl for partitioned table by
        // - specifying custom ddl name
        // - including single location
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        assertNotNull(request.getCustomDdlName());
        request.setIncludeSingleLocation(true);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true, NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationPartitionedTableNoAvailableData()
    {
        // Prepare test data with custom ddl.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME,
            PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES,
            SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME,
            INCLUDE_SINGLE_LOCATION, LATEST_VERSION_FLAG_SET, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, DATA_VERSION);

        // Create a new storage, so we can specify it in the request and result in no data being available for non-partitioned table.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Retrieve business object data ddl for partitioned table by
        // - specifying custom ddl name
        // - including single location
        // - allowing missing data and specifying storage that has no available data
        BusinessObjectDataDdlRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        assertNotNull(request.getCustomDdlName());
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(true);
        request.setStorageName(STORAGE_NAME_3);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, NO_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true,
            true, NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationPartitionedTableNoCustomDdl()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for a partitioned table by
        // - not specifying custom ddl name
        // - including single location
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        assertNull(request.getCustomDdlName());
        request.setIncludeSingleLocation(true);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false, true, true, NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationPartitionedTableNoCustomDdlMultipleStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME,
            PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES,
            SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data ddl for data located in explicitly specified multiple storage with request having:
        // - business object data version set to a valid value (this forces to select business object data regardless of business object data status)
        // - not specifying custom ddl name
        // - including single location
        // - allowMissingData is set to true
        BusinessObjectDataDdlRequest request =
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, STORAGE_NAMES, NO_STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION,
                NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION, ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS, NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                NO_AS_OF_TIME, NO_SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES);
        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the response object.
        String expectedDdl = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE,
                null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false, true,
                true, NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_BDATA_STATUS, STORAGE_NAMES, NO_STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl, NO_AS_OF_TIME);
        assertEquals(expectedDdl, resultBusinessObjectDataDdl.getDdl());
        assertEquals(expectedBusinessObjectDataDdl, resultBusinessObjectDataDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlIncludeSingleLocationPartitionedTableNoCustomDdlNoAvailableData()
    {
        // Prepare test data without custom ddl.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
            SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a new storage, so we can specify it in the request and result in no data being available for non-partitioned table.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Retrieve business object data ddl for partitioned table by
        // - not specifying custom ddl name
        // - including single location
        // - allowing missing data and specifying storage that has no available data
        BusinessObjectDataDdlRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES);
        assertNull(request.getCustomDdlName());
        request.setIncludeSingleLocation(true);
        request.setAllowMissingData(true);
        request.setStorageName(STORAGE_NAME_3);
        BusinessObjectDataDdl resultDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        // Validate the results.
        String expectedDdl = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdl(partitionColumns.size(), FIRST_COLUMN_NAME,
            FIRST_COLUMN_DATA_TYPE, CUSTOM_ROW_FORMAT, CUSTOM_CLUSTERED_BY_VALUE, null, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, NO_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false, true,
            true, NO_INCLUDE_DROP_PARTITIONS, INCLUDE_SINGLE_LOCATION);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlSuppressQuotesInNumericTypePartitionValues()
    {
        String numericPartitionType = "TINYINT";

        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesByPartitionValueTypeForBusinessObjectDataDdlTesting("1", numericPartitionType);

        BusinessObjectDataDdl resultBusinessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue("1"), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_INCLUDE_SINGLE_LOCATION,
                NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, NO_COMBINED_ALTER_TABLE_MAX_PARTITIONS,
                NO_AS_OF_TIME, SUPPRESS_QUOTES_IN_NUMERIC_TYPE_PARTITION_VALUES));

        // Validate the response object.
        assertEquals(new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue("1"), NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER,
                DATA_VERSION, NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME,
                NO_CUSTOM_DDL_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlWithSpecifiedType("1", "1", numericPartitionType),
                NO_AS_OF_TIME),
            resultBusinessObjectDataDdl);
    }
}
