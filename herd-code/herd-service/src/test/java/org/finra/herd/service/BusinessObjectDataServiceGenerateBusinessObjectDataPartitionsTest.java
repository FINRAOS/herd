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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitions;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.Partition;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataDdlPartitionsHelper;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests generateBusinessObjectDataPartitions functionality within the business object data service.
 */
public class BusinessObjectDataServiceGenerateBusinessObjectDataPartitionsTest extends AbstractServiceTest
{
    @Test
    public void testGenerateBusinessObjectDataPartitionsPartitionValueList()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataPartitionsRequest request;
        BusinessObjectDataPartitions resultPartitions;

        // Retrieve business object data partitions.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);

        // Retrieve business object data partitions when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsPartitionValueRange()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        BusinessObjectDataPartitionsRequest request;
        BusinessObjectDataPartitions resultPartitions;
        List<Partition> expectedPartitions;

        // Retrieve business object data partitions when start partition value is less than the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        expectedPartitions = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);

        businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);

        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);

        // Retrieve business object data partitions when start partition value is equal to the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, START_PARTITION_VALUE);
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        expectedPartitions = businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(START_PARTITION_VALUE), SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestBeforePartitionValue()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Check an availability using a latest before partition value filter option.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
                    NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

            // Validate the response object.
            assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_VALUE)), resultBusinessObjectDataPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestAfterPartitionValue()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Check an availability using a latest after partition value filter option.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

            // Validate the response object.
            assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    new LatestAfterPartitionValue(lowerBoundPartitionValue))), DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_VALUE_2)), resultBusinessObjectDataPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingOptionalParametersPartitionValueList()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false,
                CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions request without optional parameters.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(STORAGE_1_AVAILABLE_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setAllowMissingData(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingOptionalParametersPartitionValueRange()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false,
                CUSTOM_DDL_NAME, true, NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data partitions request without optional parameters.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        request.setIncludeAllRegisteredSubPartitions(null);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestBeforePartitionValueNoStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Check an availability using a latest before partition value filter option and without specifying any storage.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
                    NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

            // Validate the response object.
            assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                    new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_VALUE)), resultBusinessObjectDataPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestAfterPartitionValueNoStorage()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Check an availability using a latest after partition value filter option and without specifying any storage.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

            // Validate the response object.
            assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    new LatestAfterPartitionValue(lowerBoundPartitionValue))), DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_VALUE_2)), resultBusinessObjectDataPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsTrimParametersPartitionValueList()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data partitions request with all string values requiring trimming.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        for (int i = 0; i < request.getPartitionValueFilters().get(0).getPartitionValues().size(); i++)
        {
            request.getPartitionValueFilters().get(0).getPartitionValues()
                .set(i, addWhitespace(request.getPartitionValueFilters().get(0).getPartitionValues().get(i)));
        }

        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsTrimParametersPartitionValueRange()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data partitions request with all string values requiring trimming.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataPartitionsRequest(addWhitespace(START_PARTITION_VALUE), addWhitespace(END_PARTITION_VALUE));
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsUpperCaseParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data partitions request with all parameter values in upper case (except for case-sensitive partition values).
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());

        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLowerCaseParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Retrieve business object data partitions request with all parameter values in lower case (except for case-sensitive partition values).
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toLowerCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toLowerCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toLowerCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toLowerCase());
        for (int i = 0; i < request.getStorageNames().size(); i++)
        {
            request.getStorageNames().set(i, request.getStorageNames().get(i).toLowerCase());
        }
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsInvalidParameters()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataPartitionsRequest request;

        // Try to retrieve business object data partitions using non-existing format.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(request.getNamespace(), request.getBusinessObjectDefinitionName(),
                        request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()),
                e.getMessage());
        }

        // Try to retrieve business object data partitions using non-existing partition key (partition column).
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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

        // Try to retrieve business object data partitions when partition value filter has none or more than one partition value filter option specified.
        for (PartitionValueFilter partitionValueFilter : businessObjectDataServiceTestHelper.getInvalidPartitionValueFilters())
        {
            request = new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                Arrays.asList(partitionValueFilter), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS);

            try
            {
                businessObjectDataService.generateBusinessObjectDataPartitions(request);
                fail("Should throw an IllegalArgumentException when partition value filter has more than one partition value filter option.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Exactly one partition value filter option must be specified.", e.getMessage());
            }
        }

        // Try to retrieve business object data partitions when partition value token is specified with a partition value range.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataPartitionsRequest(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value token is specified with a partition value range.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataPartitionsRequest(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value token is specified with a partition value range.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value token is specified with a partition value range.
        request = businessObjectDataServiceTestHelper
            .getTestBusinessObjectDataPartitionsRequest(END_PARTITION_VALUE, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value filter has start partition value which is greater than the end partition value.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(END_PARTITION_VALUE, START_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when start partition value which is greater than the end partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                    .format("The start partition value \"%s\" cannot be greater than the end partition value \"%s\".", END_PARTITION_VALUE, START_PARTITION_VALUE),
                e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value filter has specifies a range that results in no valid partition values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest("I_DO_NOT_EXIST_1", "I_DO_NOT_EXIST_2", null);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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

        // Try to retrieve business object data partitions when partition value filter has specifies a range that results in no valid partition values.
        String invalidPartitionValue = "INVALID_PARTITION_VALUE_/";
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(Arrays.asList(invalidPartitionValue));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value contains a '/' character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition value \"%s\" can not contain a '/' character.", request.getPartitionValueFilters().get(0).getPartitionValues().get(0)),
                e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value token is specified as a partition value.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(Arrays.asList(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified as a partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of partition values.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value token is specified as a partition value.
        request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(Arrays.asList(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified as a partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of partition values.", e.getMessage());
        }

        // Try to retrieve business object data partitions passing a non-existing storage in the list of storage names.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageNames(Arrays.asList("I_DO_NOT_EXIST"));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an ObjectNotFoundException when non-existing storage is used in the list of storage names.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageNames().get(0)), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsTwoPartitionValueRanges()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data partitions when two partition value ranges are specified.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
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
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when more than one partition value range is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot specify more than one partition value range.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsDuplicatePartitionColumns()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data partitions using partition value filters with duplicate partition columns.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
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
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value filters use duplicate partition columns.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition value filters specify duplicate partition columns.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsNoSchemaExists()
    {
        // Prepare test data without schema.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, null, null, null, null, null, null,
            null, null, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data partitions when the business object format has no schema.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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
    public void testGenerateBusinessObjectDataPartitionsPartitionColumnIsAlsoRegularColumn()
    {
        // Prepare test data.
        List<SchemaColumn> schemaColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        // Override the first schema column to be a partition column.
        schemaColumns.set(0, partitionColumns.get(0));
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(partitionColumns.size(), FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsPartitionValueRangeFormatWithoutPartitionKeyGroup()
    {
        // Prepare test data with business object format having no partition key group value specified.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false,
                CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to generate business object data partitions using partition value range when business object format has no partition key group.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when generating partitions for a partition value range and " +
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
    public void testGenerateBusinessObjectDataPartitionsPartitionValueRangeExpectedPartitionValueMatchesMaxPartitionValueToken()
    {
        // Prepare test data with expected partition value set to the maximum partition value token.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        String startPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "a");
        String endPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "z");
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to generate business object data partitions when expected partition value matches to the maximum partition value token.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(startPartitionValue, endPartitionValue);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the maximum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsPartitionValueRangeExpectedPartitionValueMatchesMinPartitionValueToken()
    {
        // Prepare test data with expected partition value set to minimum partition value token.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        String startPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "a");
        String endPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "z");
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to generate business object data partitions when expected partition value matches to the maximum partition value token.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(startPartitionValue, endPartitionValue);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the minimum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSingleLevelPartitioningPartitionValueList()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
            SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(partitionColumns.size(), FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);

        // Retrieve business object data partitions when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSingleLevelPartitioningPartitionValueRange()
    {
        // Prepare test data with partition key using NO_PARTITIONING_PARTITION_KEY.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
        String partitionKey = Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY;
        partitionColumns.get(0).setName(partitionKey);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        request.getPartitionValueFilters().get(0).setPartitionKey(partitionKey);
        businessObjectDataService.generateBusinessObjectDataPartitions(request);

        businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(partitionColumns.size(), FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_PARTITION_VALUES, NO_SUBPARTITION_VALUES, false);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsNoPartitioningInvalidPartitionKey()
    {
        // Prepare non-partitioned test business object data.
        List<String> partitionValues = Arrays.asList(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null, false, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a business object data generate partitions request for a non-partitioned table with an invalid partition key value.
        BusinessObjectDataPartitionsRequest businessObjectDataPartitionsRequest =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        businessObjectDataPartitionsRequest.getPartitionValueFilters().get(0).setPartitionKey(INVALID_VALUE);
        businessObjectDataPartitionsRequest.getPartitionValueFilters().get(0).setPartitionValues(partitionValues);

        // Try to retrieve business object data partitions.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(businessObjectDataPartitionsRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The partition key \"%s\" does not exist in first %d partition columns in the schema for business object format {" +
                    "namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.", INVALID_VALUE, BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1,
                businessObjectDataPartitionsRequest.getNamespace(), businessObjectDataPartitionsRequest.getBusinessObjectDefinitionName(),
                businessObjectDataPartitionsRequest.getBusinessObjectFormatUsage(), businessObjectDataPartitionsRequest.getBusinessObjectFormatFileType(),
                businessObjectDataPartitionsRequest.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSubpartitionKeysHaveHyphens()
    {
        // Prepare test data with subpartition using key values with hyphens instead of underscores.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, true, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.  Please note that we expect hyphens in subpartition key values.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, true);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingSchemaDelimiterCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, null, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false,
            null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        String expectedRowFormat = "ROW FORMAT DELIMITED NULL DEFINED AS '\\N'";
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingSchemaEscapeCharacter()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null, null,
            null, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingSchemaNullValue()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, null, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsEscapeSingleQuoteInRowFormat()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SINGLE_QUOTE, SINGLE_QUOTE, SINGLE_QUOTE,
            SINGLE_QUOTE, null, SINGLE_QUOTE, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsEscapeBackslashInRowFormat()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, BACKSLASH, BACKSLASH, BACKSLASH,
            BACKSLASH, null, BACKSLASH, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true,
            ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results - please note that we do not escape single backslash in null value.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsUnprintableCharactersInRowFormat()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        // Set schemaDelimiterCharacter to char(1), schemaEscapeCharacter to char(10), and schemaNullValue to char(128).
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, String.valueOf((char) 1),
            String.valueOf((char) 2), String.valueOf((char) 3), String.valueOf((char) 10), null, String.valueOf((char) 128),
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions request without business object format and data versions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results - please note that 1 decimal = 1 octal, 10 decimal = 12 octal, and 128 decimal = 200 octal.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsWithCustomRowFormat()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, partitionKey, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, null, null,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingBusinessObjectDataDoNotAllowMissingData()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data partitions for the non-existing business object data with "allow missing data" flag set to "false".
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionValues(Arrays.asList("I_DO_NOT_EXIST"));
        request.setAllowMissingData(false);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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
                request.getBusinessObjectDataVersion(), request.getStorageNames().get(0)), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsNotAvailableStorageUnitDoNotAllowMissingData()
    {
        // Prepare database entities required for testing.
        StorageUnitEntity storageUnitEntity = businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Generate partitions for a collection of business object data.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(Arrays.asList(PARTITION_VALUE));
        request.setAllowMissingData(false);
        BusinessObjectDataPartitions result = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the response object.
        assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(PARTITION_VALUE)), result);

        // Update the storage unit status to a non-available one.
        storageUnitEntity.setStatus(
            storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS, DESCRIPTION, NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET));
        herdDao.saveAndRefresh(storageUnitEntity);

        // Try to retrieve business object data partitions when storage unit has a non-available status and with "allow missing data" flag set to "false".
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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
                request.getBusinessObjectDataVersion(), request.getStorageNames().get(0)), e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingBusinessObjectDataAllowMissingDataSomeDataNoExists()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions when some of the business object data is not available and "allow missing data" flag is set to "true".
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add("I_DO_NOT_EXIST");
        assertTrue(request.isAllowMissingData());
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsMissingBusinessObjectDataAllowMissingDataAllDataNoExists()
    {
        // Prepare test data.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions when all of the business object data is not available and "allow missing data" flag is set to "true".
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(Arrays.asList("I_DO_NOT_EXIST"));
        assertTrue(request.isAllowMissingData());
        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsForwardSlashInPartitionColumnName()
    {
        // Prepare test data.
        String invalidPartitionColumnName = "INVALID_/_PRTN_CLMN";
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        partitionColumns.get(0).setName(invalidPartitionColumnName);
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, PARTITION_KEY, null,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data partitions for the format that uses unsupported schema column data type.
        BusinessObjectDataPartitionsRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(invalidPartitionColumnName);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
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
    public void testGenerateBusinessObjectDataPartitionsAllKnownFileTypes()
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
                    SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

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

            // Retrieve business object data partitions.
            BusinessObjectDataPartitionsRequest request =
                businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

            // Validate the results.
            List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataPartitions(partitionColumns.size(), businessObjectFormatFileType,
                    BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsStorageFilesWithoutPrefix()
    {
        // Create an S3 storage entity with the relative attributes.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Expected business object format file type to Hive file format mapping.
        Map<String, String> businessObjectFormatFileTypeMap = new HashMap<>();
        businessObjectFormatFileTypeMap.put(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT);

        for (String businessObjectFormatFileType : businessObjectFormatFileTypeMap.keySet())
        {
            // Prepare test data for the respective business object format file type.
            List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1);
            String partitionKey = partitionColumns.get(0).getName();
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                    SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

            for (String partitionValue : STORAGE_1_AVAILABLE_PARTITION_VALUES)
            {
                BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
                String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), STORAGE_NAME);

                // add s3KeyPrefix as storage directory
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);

                // Create storage files without prefix
                for (int i = 0; i < 2; i++)
                {
                    storageFileDaoTestHelper
                    .createStorageFileEntity(storageUnitEntity, String.format("data%d.dat", i), FILE_SIZE_1_KB, ROW_COUNT_1000);

                }

                herdDao.saveAndRefresh(storageUnitEntity);
                herdDao.saveAndRefresh(businessObjectDataEntity);
            }

            // Retrieve business object data partitions.
            BusinessObjectDataPartitionsRequest request =
                businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
            request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
            BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

            // Validate the results.
            List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataPartitions(partitionColumns.size(), businessObjectFormatFileType,
                    BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsFilterOnSubPartitionValues()
    {
        List<SchemaColumn> columns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();

        // Test generate business object data partitions using primary partition and each of the available subpartition columns.
        for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, PARTITION_COLUMNS.length); i++)
        {
            // Prepare test data.
            businessObjectDataServiceTestHelper
                .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, i + 1,
                    STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA,
                    SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns, false,
                    null, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

            // Retrieve business object data partitions request without drop table statement.
            BusinessObjectDataPartitionsRequest request =
                businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(STORAGE_1_AVAILABLE_PARTITION_VALUES);
            request.getPartitionValueFilters().get(0).setPartitionKey(partitionColumns.get(i).getName());
            request.setAllowMissingData(NO_ALLOW_MISSING_DATA);
            BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

            // Validate the results.
            List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE, i + 1, STORAGE_1_AVAILABLE_PARTITION_VALUES,
                    SUBPARTITION_VALUES, false);
            businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsUsingStorageDirectoriesNoAutoDiscovery()
    {
        // Prepare test data with storage units having no storage files, but only the relative storage directory path values.
        // For auto-discovery not to occur, number of partition columns is equal to the number of partition values.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1 + SUBPARTITION_VALUES.size());
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns, false, CUSTOM_DDL_NAME, false, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        BusinessObjectDataPartitionsRequest request;
        BusinessObjectDataPartitions resultPartitions;

        // Retrieve business object data partitions.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(partitionColumns.size(), FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES, false);
        businessObjectDataServiceTestHelper.validateBusinessObjectDataPartitions(request, expectedPartitions, resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsStorageDirectoryMismatchesS3KeyPrefix()
    {
        // Prepare test data with a storage unit having no storage files and storage directory path value not matching the expected S3 key prefix.
        String invalidS3KeyPrefix = "INVALID_S3_KEY_PREFIX";
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE, invalidS3KeyPrefix);

        // Try to retrieve business object data partitions when storage unit has no storage
        // files and storage directory path not matching the expected S3 key prefix.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsStorageDirectoryIsNull()
    {
        // Prepare test data with a storage unit having no storage files and storage directory path is null.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE, null);

        // Try to retrieve business object data partitions when storage unit has no storage files and storage directory path is null.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsLatestFormatVersionDataNotAvailableInStorage()
    {
        // Create database entities for two versions of a business object format.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns().subList(0, 1 + SUBPARTITION_VALUES.size());
        BusinessObjectFormatEntity businessObjectFormatV0Entity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);
        BusinessObjectFormatEntity businessObjectFormatV1Entity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

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

        // Retrieve business object data partitions for the first storage without specifying business object format version.
        BusinessObjectDataPartitionsRequest request = new BusinessObjectDataPartitionsRequest();

        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(null);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));
        partitionValueFilter.setPartitionKey(FIRST_PARTITION_COLUMN_NAME);
        partitionValueFilter.setPartitionValues(Arrays.asList(PARTITION_VALUE));

        request.setBusinessObjectDataVersion(INITIAL_DATA_VERSION);
        request.setStorageNames(Arrays.asList(StorageEntity.MANAGED_STORAGE));

        BusinessObjectDataPartitions resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        assertNotNull(resultPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLargePartitionValueListPrimaryPartitionOnly()
    {
        final int PRIMARY_PARTITION_VALUES_SIZE = 10000;

        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataPartitionsRequest request;
        BusinessObjectDataPartitions resultPartitions;

        // Retrieve business object data partitions by passing a large set of partition values.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < PRIMARY_PARTITION_VALUES_SIZE; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(partitionValues);
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);

        // Retrieve business object data partitions when request partition value list has duplicate values.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(request);

        // Validate the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataPartitions(request, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitions(), resultPartitions);
    }

    @Test
    @Ignore
    public void testGenerateBusinessObjectDataPartitionsLargePartitionValueListWithAutoDiscovery()
    {
        final int PRIMARY_PARTITION_VALUE_LIST_SIZE = 10000;
        final int SECOND_LEVEL_PARTITION_VALUES_PER_BUSINESS_OBJECT_DATA = 1;
        final int STORAGE_FILES_PER_PARTITION = 1;

        // Prepare test data and build a list of partition values to generate business object data partitions for.

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
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

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

        // Retrieve business object data partitions for the entire list of partition values.
        BusinessObjectDataPartitions BusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, partitionValues, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the results.
        assertNotNull(BusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestBeforePartitionValueNoExists()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE_2);

        // Try to retrieve business object data partitions using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(PARTITION_VALUE), NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, SINGLE_STORAGE_NAMES,
                    NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsLatestAfterPartitionValueNoExists()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);

        // Try retrieve business object data partitions using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(PARTITION_VALUE_2))), DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsMultipleStorages()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Retrieve business object data partitions for data located in multiple storages.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, STORAGE_NAMES, ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object.
        List<Partition> expectedPartitions = businessObjectDataServiceTestHelper
            .getExpectedBusinessObjectDataPartitions(PARTITION_COLUMNS.length, FileTypeEntity.TXT_FILE_TYPE,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_UNION, SUBPARTITION_VALUES, false);
        assertEquals(new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, STORAGE_NAMES, expectedPartitions), resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsNoStorageNamesAndSameBusinessObjectDataInMultipleStorageEntities()
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null, SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data partitions when storage names are not specified and the same business object data is registered
        // in multiple storage entities and with both business object format and business object data versions are specified in the request.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Collections
                    .singletonList(new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), DATA_VERSION, NO_STORAGE_NAMES, ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
                    NO_STORAGE_NAMES, NO_STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME,
                    INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION, NO_INCLUDE_DROP_PARTITIONS, ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS,
                    AbstractServiceTest.NO_COMBINE_MULTIPLE_PARTITIONS_IN_SINGLE_ALTER_TABLE, AbstractServiceTest.NO_AS_OF_TIME));
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


    //This test case reproduces an error when business object data has more or equal sub-partition values then the latest business object format version.

    @Test
    public void testGenerateBusinessObjectDataPartitionsLatestFormatHasLessPartitionColumnsThenBusinessObjectData()
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
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns1);

        // Create a second version of business object format with the schema having only one partition column.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns2);

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

        // Try to retrieve business object data partitions when business object data has more
        // or equal sub-partition values then the latest business object format version.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, null, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        NO_LATEST_AFTER_PARTITION_VALUE)), null, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                    NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionValid()
    {
        //
        List<List<String>> testPartitions =
            Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2));

        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(testPartitions);

        // Retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Both sub-partitions should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(testPartitions)),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionDeleted()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to DELETED.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.DELETED));

        // Retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Only the first sub-partition should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)))),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalid()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to INVALID.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.INVALID));

        // Try to retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionValidNonAvailableStorageUnit()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition storage unit status to DISABLED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.DISABLED));

        // Try to retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionValidNonAvailableStorageUnitBdataArchived()
    {
        // Create two VALID sub-partitions both with "available" storage units.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition storage unit status to ARCHIVED.
        storageUnitEntities.get(1).setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ARCHIVED));

        // Try to retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalidBdataArchived()
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

        // Try to retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsIncludeAllRegisteredSubPartitionsSecondSubPartitionDeletedBdataArchived()
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

        // Retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option enabled.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Both sub-partitions should. Only the first sub-partition should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)))),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsDoNotIncludeAllRegisteredSubPartitionsSecondSubPartitionInvalid()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
                Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Update the second sub-partition business object data status to INVALID.
        storageUnitEntities.get(1).getBusinessObjectData()
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.INVALID));

        // Retrieve business object data partitions with "IncludeAllRegisteredSubPartitions" option disabled.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Only the first sub-partition should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)))),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSuppressScanForUnregisteredSubPartitions()
    {
        // Create two VALID sub-partitions both with "available" storage units in a non-Glacier storage.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(
            Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)));

        // Retrieve business object data partitions with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Both sub-partitions should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(
                    Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)))),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSuppressScanForUnregisteredSubPartitionsNoDirectoryPath()
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

        // Retrieve business object data partitions with flag set to suppress scan for unregistered sub-partitions.
        BusinessObjectDataPartitions resultBusinessObjectDataPartitions = businessObjectDataService.generateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));

        // Validate the response object. Both sub-partitions should be present in the generated partitions.
        validateBusinessObjectDataPartitions(
            new BusinessObjectDataPartitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataPartitionsTwoPartitionLevels(
                    Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1), Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_2)))),
            resultBusinessObjectDataPartitions);
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsSuppressScanForUnregisteredSubPartitionsDirectoryPathMismatchS3KeyPrefix()
    {
        // Create one VALID sub-partition with an "available" storage unit a non-Glacier storage.
        List<StorageUnitEntity> storageUnitEntities = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(Arrays.asList(Arrays.asList(PARTITION_VALUE, SUB_PARTITION_VALUE_1)));

        // Save the original S3 key prefix (directory path) and update the storage unit with a directory path that does not match the S3 key prefix.
        String originalS3KeyPrefix = storageUnitEntities.get(0).getDirectoryPath();
        storageUnitEntities.get(0).setDirectoryPath(INVALID_VALUE);

        // Try to retrieve business object data partitions with flag set to suppress scan for unregistered
        // sub-partitions when its storage unit directory path does not match the expected S3 key prefix.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                        NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_DATA_VERSION, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA,
                    NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS, SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionsSuppressScanForUnregisteredSubPartitionsLatestFormatHasLessPartitionColumnsThanBusinessObjectData()
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
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns1);

        // Create a second version of business object format with the schema having only one partition column.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES,
                SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, null,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns2);

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

        // Try to retrieve business object data partitions when flag is set to suppress scan for unregistered sub-partitions
        // and latest format has less partition columns then the business object data is registered with.
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(
                new BusinessObjectDataPartitionsRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, null, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        NO_LATEST_AFTER_PARTITION_VALUE)), null, SINGLE_STORAGE_NAMES, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                    SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS));
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
    public void testGenerateBusinessObjectDataPartitionslMissingRequiredParameters()
    {
        BusinessObjectDataPartitionsRequest request;

        // Try to retrieve business object data partitions when business object definition name parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when business object format usage parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when business object format file type parameter is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to retrieve business object data partitions when partition value list is null.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilters(Collections.EMPTY_LIST);
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when partition value list is null.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one partition value filter must be specified.", e.getMessage());
        }

        // Try to check business object data availability when standalone storage name parameter value is not specified.
        request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        request.setStorageNames(Collections.singletonList(BLANK_TEXT));
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when storage name parameter in the list of storage names is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsTrimParametersStorageNames()
    {
        // Prepare test data.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        // Try to retrieve business object data partitions when business object definition name parameter is not specified.
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        List<String> storageNames = Arrays.asList(addWhitespace(AbstractServiceTest.STORAGE_NAME));
        request.setStorageNames(storageNames);

        // Verify storageNames in request are trimmed
        businessObjectDataService.generateBusinessObjectDataPartitions(request);
        assertEquals(AbstractServiceTest.STORAGE_NAME, request.getStorageNames().get(0));
    }

    @Test
    public void testGenerateBusinessObjectDataPartitionsNoPartitioningSingletonObject()
    {
        // Prepare test data.
        List<String> partitionValues = Arrays.asList(BusinessObjectDataDdlPartitionsHelper.NO_PARTITIONING_PARTITION_VALUE);
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, Hive13DdlGenerator.NO_PARTITIONING_PARTITION_KEY, null,
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, partitionValues, NO_SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), null, false, CUSTOM_DDL_NAME, true,
                ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Try to retrieve business object data partitions with partitionKey="partition" and partitionValue="none"
        BusinessObjectDataPartitionsRequest request =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataPartitionsRequest(null, null, UNSORTED_PARTITION_VALUES);
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilter.setPartitionKey(BusinessObjectDataDdlPartitionsHelper.NO_PARTITIONING_PARTITION_KEY);
        partitionValueFilter.setPartitionValues(partitionValues);
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));

        // Verify Exception is throw for singleton object
        try
        {
            businessObjectDataService.generateBusinessObjectDataPartitions(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Generate-partitions request does not support singleton partitions.", e.getMessage());
        }
    }

    private void validateBusinessObjectDataPartitions(BusinessObjectDataPartitions expectedBusinessObjectDataPartitions,
        BusinessObjectDataPartitions actualBusinessObjectDataPartitions)
    {

        assertNotNull(actualBusinessObjectDataPartitions);
        assertEquals(expectedBusinessObjectDataPartitions.getNamespace(), actualBusinessObjectDataPartitions.getNamespace());
        assertEquals(expectedBusinessObjectDataPartitions.getBusinessObjectDefinitionName(),
            actualBusinessObjectDataPartitions.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectDataPartitions.getBusinessObjectFormatUsage(), actualBusinessObjectDataPartitions.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectDataPartitions.getBusinessObjectFormatFileType(),
            actualBusinessObjectDataPartitions.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectDataPartitions.getBusinessObjectFormatVersion(),
            actualBusinessObjectDataPartitions.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataPartitions.getPartitionValueFilters(), actualBusinessObjectDataPartitions.getPartitionValueFilters());
        assertEquals(expectedBusinessObjectDataPartitions.getBusinessObjectDataVersion(), actualBusinessObjectDataPartitions.getBusinessObjectDataVersion());
        assertEquals(expectedBusinessObjectDataPartitions.getStorageNames(), actualBusinessObjectDataPartitions.getStorageNames());

        List<Partition> expectedPartitions = expectedBusinessObjectDataPartitions.getPartitions();
        List<Partition> actualPartitions = actualBusinessObjectDataPartitions.getPartitions();
        if (expectedPartitions == null && actualPartitions != null || expectedPartitions != null && actualPartitions == null)
        {
            fail("Only one of the expected/actual partitions list is null");
        }
        else
        {
            assertTrue(expectedPartitions == actualPartitions ||
                expectedPartitions.size() == actualPartitions.size() && new HashSet(expectedPartitions).equals(new HashSet(actualPartitions)));
        }
    }
}
