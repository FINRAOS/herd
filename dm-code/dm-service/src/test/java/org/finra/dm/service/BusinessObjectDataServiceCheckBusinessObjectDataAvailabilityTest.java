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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailability;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStatus;
import org.finra.dm.model.api.xml.LatestAfterPartitionValue;
import org.finra.dm.model.api.xml.LatestBeforePartitionValue;
import org.finra.dm.model.api.xml.PartitionValueFilter;
import org.finra.dm.model.api.xml.PartitionValueRange;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.service.impl.BusinessObjectDataServiceImpl;

/**
 * This class tests checkBusinessObjectDataAvailability functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceCheckBusinessObjectDataAvailabilityTest extends AbstractServiceTest
{
    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueList()
    {
        // Prepare test data and execute the check business object data availability request.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListStandalonePartitionValueFilter()
    {
        // Prepare test data and execute the check business object data availability request with a standalone partition value filter.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, true);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRange()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        BusinessObjectDataAvailabilityRequest request;
        BusinessObjectDataAvailability resultAvailability;
        List<BusinessObjectDataStatus> expectedAvailableStatuses;
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses;

        // Execute the check business object data availability request when start partition value is less than the end partition value.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        expectedNotAvailableStatuses = getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            PROCESS_DATE_PARTITION_VALUES_NOT_AVAILABLE, null, DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);

        // Execute the check business object data availability request when start partition value is equal to the end partition value.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, START_PARTITION_VALUE);
        resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(START_PARTITION_VALUE),
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, DATA_VERSION,
                BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLatestBeforePartitionValue()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Check an availability using a latest before partition value filter option.
        for (String upperBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataAvailability resultBusinessObjectDataAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(
                new BusinessObjectDataAvailabilityRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE,
                        new LatestBeforePartitionValue(upperBoundPartitionValue), NO_LATEST_AFTER_PARTITION_VALUE)), null, DATA_VERSION, STORAGE_NAME));

            // Validate the response object.
            assertEquals(new BusinessObjectDataAvailability(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(upperBoundPartitionValue),
                    NO_LATEST_AFTER_PARTITION_VALUE)), null, DATA_VERSION, STORAGE_NAME, Arrays
                .asList(new BusinessObjectDataStatus(FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID)),
                new ArrayList<BusinessObjectDataStatus>()), resultBusinessObjectDataAvailability);
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLatestAfterPartitionValue()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
            DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Check an availability using a latest after partition value filter option.
        for (String lowerBoundPartitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            BusinessObjectDataAvailability resultBusinessObjectDataAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(
                new BusinessObjectDataAvailabilityRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(lowerBoundPartitionValue))), null, DATA_VERSION, STORAGE_NAME));

            // Validate the response object.
            assertEquals(new BusinessObjectDataAvailability(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    new LatestAfterPartitionValue(lowerBoundPartitionValue))), null, DATA_VERSION, STORAGE_NAME, Arrays.asList(
                new BusinessObjectDataStatus(FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID)),
                new ArrayList<BusinessObjectDataStatus>()), resultBusinessObjectDataAvailability);
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMissingRequiredParameters()
    {
        BusinessObjectDataAvailabilityRequest request;

        // Try to check business object data availability when business object definition name parameter is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when business object definition name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to check business object data availability when business object format usage parameter is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when business object format usage parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage name must be specified.", e.getMessage());
        }

        // Try to check business object data availability when business object format file type parameter is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when business object format file type parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to check business object data availability when partition key is not specified in one of the partition value filters.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition key is not specified in one of the partition value filters.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key must be specified.", e.getMessage());
        }

        // Try to check business object data availability when start partition value is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(BLANK_TEXT, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when start partition values is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A start partition value for the partition value range must be specified.", e.getMessage());
        }

        // Try to check business object data availability when end partition value is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when end partition values is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An end partition value for the partition value range must be specified.", e.getMessage());
        }

        // Try to check business object data availability when partition value list has no partition values specified.
        request = getTestBusinessObjectDataAvailabilityRequest(new ArrayList<String>());
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value list has no partition values specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one partition value must be specified.", e.getMessage());
        }

        // Try to check business object data availability when one of the partition values in the partition value list is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when one of the partition values in the partition value list is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to check business object data availability when the latest before partition value filter option has no partition value specified.
        request = getTestBusinessObjectDataAvailabilityRequest(NO_PARTITION_VALUES);
        for (String partitionValue : Arrays.asList(null, BLANK_TEXT))
        {
            request.getPartitionValueFilters().get(0).setLatestBeforePartitionValue(new LatestBeforePartitionValue(partitionValue));
            try
            {
                businessObjectDataService.checkBusinessObjectDataAvailability(request);
                fail("Should throw an IllegalArgumentException when the latest before partition value filter option has no partition value specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("A partition value must be specified.", e.getMessage());
            }
        }

        // Try to check business object data availability when the latest after partition value filter option has no partition value specified.
        request = getTestBusinessObjectDataAvailabilityRequest(NO_PARTITION_VALUES);
        for (String partitionValue : Arrays.asList(null, BLANK_TEXT))
        {
            request.getPartitionValueFilters().get(0).setLatestAfterPartitionValue(new LatestAfterPartitionValue(partitionValue));
            try
            {
                businessObjectDataService.checkBusinessObjectDataAvailability(request);
                fail("Should throw an IllegalArgumentException when the latest after partition value filter option has no partition value specified.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("A partition value must be specified.", e.getMessage());
            }
        }

        // Try to check business object data availability when storage name parameter is not specified.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName(BLANK_TEXT);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when storage name parameter is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMissingOptionalParameters()
    {
        // Prepare test data and execute the check business object data availability request without optional parameters.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null, null,
                BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMissingOptionalParametersStandalonePartitionValueFilter()
    {
        // Prepare test data and execute the check business object data availability request
        // with a standalone partition value filter and without optional parameters.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        request.setBusinessObjectFormatVersion(null);
        request.getPartitionValueFilter().setPartitionKey(BLANK_TEXT);
        request.setBusinessObjectDataVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null, null,
                BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, true);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMissingFormatVersion()
    {
        // Prepare test data and execute the check business object data availability request with only business object format version missing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectFormatVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityTrimParametersPartitionValueList()
    {
        // Prepare test data and execute the check business object data availability request with all string values requiring trimming.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        for (int i = 0; i < request.getPartitionValueFilters().get(0).getPartitionValues().size(); i++)
        {
            request.getPartitionValueFilters().get(0).getPartitionValues()
                .set(i, addWhitespace(request.getPartitionValueFilters().get(0).getPartitionValues().get(i)));
        }
        request.setStorageName(addWhitespace(request.getStorageName()));
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityTrimParametersPartitionValueRange()
    {
        // Prepare test data and execute the check business object data availability request with all string values requiring trimming.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);
        BusinessObjectDataAvailabilityRequest request =
            getTestBusinessObjectDataAvailabilityRequest(addWhitespace(START_PARTITION_VALUE), addWhitespace(END_PARTITION_VALUE));
        request.setBusinessObjectDefinitionName(addWhitespace(request.getBusinessObjectDefinitionName()));
        request.setBusinessObjectFormatUsage(addWhitespace(request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(addWhitespace(request.getBusinessObjectFormatFileType()));
        request.getPartitionValueFilters().get(0).setPartitionKey(addWhitespace(request.getPartitionValueFilters().get(0).getPartitionKey()));
        request.setStorageName(addWhitespace(request.getStorageName()));
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                PROCESS_DATE_PARTITION_VALUES_NOT_AVAILABLE, null, DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityUpperCaseParameters()
    {
        // Prepare test data and execute the check business object data availability request
        // with all parameter values in upper case (except for case-sensitive partition values).
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toUpperCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toUpperCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toUpperCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toUpperCase());
        request.setStorageName(request.getStorageName().toUpperCase());
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLowerCaseParameters()
    {
        // Prepare test data and execute the check business object data availability request
        // with all parameter values in lower case (except for case-sensitive partition values).
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().toLowerCase());
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().toLowerCase());
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().toLowerCase());
        request.getPartitionValueFilters().get(0).setPartitionKey(request.getPartitionValueFilters().get(0).getPartitionKey().toLowerCase());
        request.setStorageName(request.getStorageName().toLowerCase());
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityInvalidParameters()
    {
        BusinessObjectDataAvailabilityRequest request;

        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability using non-existing format.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setBusinessObjectDefinitionName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an ObjectNotFoundException when non-existing business object format is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectFormatNotFoundErrorMessage(request.getNamespace(), request.getBusinessObjectDefinitionName(),
                request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }

        // Try to check business object data availability using non-existing partition key (partition column).
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
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

        // Try to check business object data availability when partition value filter has none or more than one partition value filter option specified.
        for (PartitionValueFilter partitionValueFilter : getInvalidPartitionValueFilters())
        {
            request = new BusinessObjectDataAvailabilityRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                Arrays.asList(partitionValueFilter), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, STORAGE_NAME);
            try
            {
                businessObjectDataService.checkBusinessObjectDataAvailability(request);
                fail("Should throw an IllegalArgumentException when partition value filter has more than one partition value filter option.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Exactly one partition value filter option must be specified.", e.getMessage());
            }
        }

        // Try to check business object data availability when partition value token is specified with a partition value range.
        request = getTestBusinessObjectDataAvailabilityRequest(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to check business object data availability when partition value token is specified with a partition value range.
        request = getTestBusinessObjectDataAvailabilityRequest(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to check business object data availability when partition value token is specified with a partition value range.
        request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to check business object data availability when partition value token is specified with a partition value range.
        request = getTestBusinessObjectDataAvailabilityRequest(END_PARTITION_VALUE, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value token is specified with a partition value range.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified with a partition value range.", e.getMessage());
        }

        // Try to check business object data availability when partition value filter has start partition value which is greater than the end partition value.
        request = getTestBusinessObjectDataAvailabilityRequest(END_PARTITION_VALUE, START_PARTITION_VALUE);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when start partition value which is greater than the end partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("The start partition value \"%s\" cannot be greater than the end partition value \"%s\".", END_PARTITION_VALUE, START_PARTITION_VALUE),
                e.getMessage());
        }

        // Try to check business object data availability using non-existing storage.
        request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setStorageName("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an ObjectNotFoundException when non-existing storage is used.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageName()), e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueFilterListAndStandalonePartitionValueFilter()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability when both partition value filter and partition value filter list are specified.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.setPartitionValueFilter(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME.toUpperCase(), new ArrayList<>(UNSORTED_PARTITION_VALUES), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when both a list of partition value filters and a standalone partition value filter are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A list of partition value filters and a standalone partition value filter cannot be both specified.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityTwoPartitionValueRanges()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability when two partition value ranges are specified.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
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
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when more than one partition value range is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot specify more than one partition value range.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityInvalidPartitionKeyAndNoSchemaExists()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<SchemaColumn>(), new ArrayList<SchemaColumn>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, NO_SUBPARTITION_VALUES);

        // Try to check business object data availability using non-existing partition key when the business object format has no schema.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).setPartitionKey("I_DO_NOT_EXIST");
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when non-existing partition key is used.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\" and there is " +
                "no schema defined to check subpartition columns for business object format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d}.",
                request.getPartitionValueFilters().get(0).getPartitionKey(), PARTITION_KEY, request.getNamespace(), request.getBusinessObjectDefinitionName(),
                request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()), e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityDuplicatePartitionColumns()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability using partition value filters with duplicate partition columns.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
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
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when partition value filters use duplicate partition columns.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition value filters specify duplicate partition columns.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityNoBusinessObjectDataFound()
    {
        // Prepare test data and execute the check business object data availability request.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(PARTITION_VALUES_NOT_AVAILABLE);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses = getTestBusinessObjectDataStatuses(FORMAT_VERSION, 0, null, null, DATA_VERSION, null, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null,
                DATA_VERSION, BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityBusinessObjectDataNotAvailableNoValidStatus()
    {
        // Prepare test data with business object data not having the VALID status.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        // Create and execute a business object data availability request without a business object data version.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(PARTITION_KEY, Arrays.asList(PARTITION_VALUE));
        request.setBusinessObjectDataVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        BusinessObjectDataStatus expectedNotAvailableStatus =
            new BusinessObjectDataStatus(FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BDATA_STATUS);
        validateBusinessObjectDataAvailability(request, NO_BUSINESS_OBJECT_DATA_STATUSES, Arrays.asList(expectedNotAvailableStatus), resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRangeMaxValuesExceeded() throws Exception
    {
        final Integer testMaxAllowedPartitionValues = 1;

        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Create a business object data availability request with the range of values that would contain 5 partition values.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);

        // Override configuration to add max partition values.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AVAILABILITY_DDL_MAX_PARTITION_VALUES.getKey(), testMaxAllowedPartitionValues.toString());
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to check business object data availability when the maximum allowed number of partition values would be exceeded.
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when the maximum allowed number of partition values have been exceeded.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The number of partition values (5) exceeds the system limit of %d.", testMaxAllowedPartitionValues), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRangeFormatWithoutPartitionKeyGroup()
    {
        // Prepare test data with business object format having no partition key group value specified.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability using partition value range when business object format has no partition key group.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(START_PARTITION_VALUE, END_PARTITION_VALUE);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when checking availability using partition value range and " +
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
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRangeExpectedPartitionValueMatchesMaxPartitionValueToken()
    {
        // Prepare test data with expected partition value set to the maximum partition value token.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        String startPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "a");
        String endPartitionValue = BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN.replace("maximum", "z");
        createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to check business object data availability when expected partition value matches to the maximum partition value token.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(startPartitionValue, endPartitionValue);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the maximum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueRangeExpectedPartitionValueMatchesMinPartitionValueToken()
    {
        // Prepare test data with expected partition value set to minimum partition value token.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(PARTITION_KEY_GROUP);
        String startPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "a");
        String endPartitionValue = BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN.replace("minimum", "z");
        createExpectedPartitionValueEntities(PARTITION_KEY_GROUP,
            Arrays.asList(startPartitionValue, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN, endPartitionValue));

        // Try to check business object data availability when expected partition value matches to the maximum partition value token.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(startPartitionValue, endPartitionValue);
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(request);
            fail("Should throw an IllegalArgumentException when expected partition value matches to the minimum partition value token.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value token cannot be specified as one of the expected partition values.", e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityFilterOnSubPartitionValues()
    {
        List<SchemaColumn> columns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();

        // Test data availability using primary partition and each of the available subpartition columns.
        for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, PARTITION_COLUMNS.length); i++)
        {
            // Prepare test data and execute the check business object data availability request.
            createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, columns, partitionColumns, i + 1, SUBPARTITION_VALUES);
            BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
            request.getPartitionValueFilters().get(0).setPartitionKey(partitionColumns.get(i).getName());
            BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

            // Validate the results.
            List<BusinessObjectDataStatus> expectedAvailableStatuses =
                getTestBusinessObjectDataStatuses(FORMAT_VERSION, i + 1, PARTITION_VALUES_AVAILABLE, SUBPARTITION_VALUES, DATA_VERSION,
                    BusinessObjectDataStatusEntity.VALID, false);
            List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
                getTestBusinessObjectDataStatuses(FORMAT_VERSION, i + 1, PARTITION_VALUES_NOT_AVAILABLE, null, DATA_VERSION,
                    BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
            validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityFilterOnSubPartitionValuesStandalonePartitionValueFilter()
    {
        List<SchemaColumn> columns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();

        // Test data availability using primary partition and each of the available subpartition columns.
        for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, PARTITION_COLUMNS.length); i++)
        {
            // Prepare test data and execute the check business object data availability request.
            createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, columns, partitionColumns, i + 1, SUBPARTITION_VALUES);
            BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
            request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
            request.setPartitionValueFilters(null);
            request.getPartitionValueFilter().setPartitionKey(partitionColumns.get(i).getName());
            BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

            // Validate the results.
            List<BusinessObjectDataStatus> expectedAvailableStatuses =
                getTestBusinessObjectDataStatuses(FORMAT_VERSION, i + 1, PARTITION_VALUES_AVAILABLE, SUBPARTITION_VALUES, DATA_VERSION,
                    BusinessObjectDataStatusEntity.VALID, false);
            List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
                getTestBusinessObjectDataStatuses(FORMAT_VERSION, i + 1, PARTITION_VALUES_NOT_AVAILABLE, null, DATA_VERSION,
                    BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, true);
            validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLargePartitionValueListPrimaryPartitionOnly()
    {
        final int PRIMARY_PARTITION_VALUES_SIZE = 10000;

        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Build and execute a business object data availability request with a large list of partition values.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < PRIMARY_PARTITION_VALUES_SIZE; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(partitionValues);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        assertNotNull(resultAvailability);
        assertEquals(PARTITION_VALUES_AVAILABLE.size(), resultAvailability.getAvailableStatuses().size());
        assertEquals(partitionValues.size() - PARTITION_VALUES_AVAILABLE.size(), resultAvailability.getNotAvailableStatuses().size());
    }

    /**
     * This test validates the getBusinessObjectDataEntities() functionality when request contains multiple partition value filters and when business object
     * data entities actually exist.
     */
    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionValueFilters()
    {
        final int PRIMARY_PARTITION_VALUES_SIZE = 5;

        String[][] subPartitionValues =
            new String[][] {{null, null, null, null}, {"A", null, null, null}, {"A", "X", null, null}, {"A", "X", "0", null}, {"A", "X", "0", "0"},
                {"A", "X", "0", "1"}, {"A", "X", "1", "0"}, {"A", "X", "1", "1"}, {"A", "Y", "0", "0"}, {"A", "Y", "0", "1"}, {"A", "Y", "1", "0"},
                {"A", "Y", "1", "1"}};

        String[] secondStorageSubPartitionValues = new String[] {"A", "X", "2", "2"};

        // Create the relative database entities.
        List<SchemaColumn> columns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();

        // Create a business object format entity with the schema.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            partitionKey, null, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns);

        // Create two storage entities if they do not exist.
        List<StorageEntity> storageEntities = Arrays.asList(createStorageEntity(STORAGE_NAME), createStorageEntity(STORAGE_NAME_2));

        // Create the relative business object data entities with the relative storage units.
        // Please note that we will create some extra business object data entities that
        // will not be selected as available per partition value mismatch or a different storage.
        List<String> primaryPartitionValues = new ArrayList<>();
        for (int i = 0; i < PRIMARY_PARTITION_VALUES_SIZE; i++)
        {
            BusinessObjectDataEntity businessObjectDataEntity;
            String primaryPartitionValue = String.format("%s-%s", PARTITION_VALUE, i);
            primaryPartitionValues.add(primaryPartitionValue);

            for (String[] subPartitionValuesSet : subPartitionValues)
            {
                businessObjectDataEntity =
                    createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                        Arrays.asList(subPartitionValuesSet), DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);

                createStorageUnitEntity(storageEntities.get(0), businessObjectDataEntity);
            }

            // Add an extra business object data that is only present in the second storage.
            businessObjectDataEntity =
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                    Arrays.asList(secondStorageSubPartitionValues), DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);

            createStorageUnitEntity(storageEntities.get(1), businessObjectDataEntity);
        }

        // Check business object data availability request with a large list of partition values using multiple partition value filters.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(null);
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        request.setPartitionValueFilters(partitionValueFilters);
        partitionValueFilters.add(
            new PartitionValueFilter(partitionColumns.get(0).getName(), primaryPartitionValues, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(partitionColumns.get(1).getName(), Arrays.asList("A"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(partitionColumns.get(2).getName(), Arrays.asList("X", "Y"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(partitionColumns.get(3).getName(), Arrays.asList("0", "1"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter(partitionColumns.get(4).getName(), Arrays.asList("0", "1"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        assertNotNull(resultAvailability);
        assertEquals(PRIMARY_PARTITION_VALUES_SIZE * 8, resultAvailability.getAvailableStatuses().size());
        assertEquals(0, resultAvailability.getNotAvailableStatuses().size());
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListMaxPartitionValueToken()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Check business object data availability using maximum partition value token.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(null);
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        request.setPartitionValueFilters(partitionValueFilters);
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(GREATEST_PARTITION_VALUE),
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses = new ArrayList<>();
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListMinPartitionValueToken()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Check business object data availability using minimum partition value token.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(null);
        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        request.setPartitionValueFilters(partitionValueFilters);
        partitionValueFilters.add(
            new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(LEAST_PARTITION_VALUE),
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses = new ArrayList<>();
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListPartitionValueTokensMissingOptionalParameters()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Check business object data availability using multiple maximum and minimum partition value tokens along with other partition values.
        BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
        request.setBusinessObjectFormatVersion(null);
        request.setBusinessObjectDataVersion(null);
        BusinessObjectDataAvailability resultAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Validate the results.
        List<BusinessObjectDataStatus> expectedAvailableStatuses =
            getTestBusinessObjectDataStatuses(FORMAT_VERSION, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_AVAILABLE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID, false);
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses =
            getTestBusinessObjectDataStatuses(null, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PARTITION_VALUES_NOT_AVAILABLE, null, null,
                BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED, false);
        validateBusinessObjectDataAvailability(request, expectedAvailableStatuses, expectedNotAvailableStatuses, resultAvailability);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityPartitionValueListPartitionValueTokensNoPartitionValueExists()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Try to check business object data availability using partition value tokens on a sub-partition
        // column when all business object data is registered using only primary partition.
        for (String partitionValueToken : Arrays
            .asList(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN, BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN))
        {
            BusinessObjectDataAvailabilityRequest request = getTestBusinessObjectDataAvailabilityRequest(null);
            List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
            request.setPartitionValueFilters(partitionValueFilters);
            partitionValueFilters.add(new PartitionValueFilter(SECOND_PARTITION_COLUMN_NAME, Arrays.asList(partitionValueToken), NO_PARTITION_VALUE_RANGE,
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE));
            try
            {
                businessObjectDataService.checkBusinessObjectDataAvailability(request);
                fail("Suppose to throw an ObjectNotFoundException when failed to find a maximum partition value.");
            }
            catch (ObjectNotFoundException e)
            {

                assertEquals(String.format("Failed to find %s partition value for partition key = \"%s\" due to no available business object data " +
                    "in \"%s\" storage that is registered using that partition. Business object data {namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataVersion: %d}",
                    partitionValueToken.equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) ? "maximum" : "minimum", SECOND_PARTITION_COLUMN_NAME,
                    STORAGE_NAME, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, DATA_VERSION), e.getMessage());
            }
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLatestBeforePartitionValueNoExists()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
            DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Try to check an availability using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(
                new BusinessObjectDataAvailabilityRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(PARTITION_VALUE),
                        NO_LATEST_AFTER_PARTITION_VALUE)), null, DATA_VERSION, STORAGE_NAME));
            fail("Suppose to throw an ObjectNotFoundException when failed to find the latest before partition value.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Failed to find partition value which is the latest before partition value = \"%s\" " +
                "for partition key = \"%s\" due to no available business object data " +
                "in \"%s\" storage that satisfies the search criteria. Business object data {namespace: \"%s\", " +
                "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", PARTITION_VALUE, PARTITION_KEY, STORAGE_NAME, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, DATA_VERSION), e.getMessage());
        }
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityLatestAfterPartitionValueNoExists()
    {
        // Create database entities required for testing.
        createStorageUnitEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Try to check an availability using a latest before partition value filter option when the latest partition value does not exist.
        try
        {
            businessObjectDataService.checkBusinessObjectDataAvailability(
                new BusinessObjectDataAvailabilityRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                    new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                        new LatestAfterPartitionValue(PARTITION_VALUE_2))), null, DATA_VERSION, STORAGE_NAME));
            fail("Suppose to throw an ObjectNotFoundException when failed to find the latest after partition value.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Failed to find partition value which is the latest after partition value = \"%s\" " +
                "for partition key = \"%s\" due to no available business object data " +
                "in \"%s\" storage that satisfies the search criteria. Business object data {namespace: \"%s\", " +
                "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", PARTITION_VALUE_2, PARTITION_KEY, STORAGE_NAME, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, DATA_VERSION), e.getMessage());
        }
    }
}
