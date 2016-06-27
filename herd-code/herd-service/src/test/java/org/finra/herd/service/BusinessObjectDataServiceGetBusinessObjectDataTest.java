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
import java.util.List;

import org.apache.commons.collections4.IterableUtils;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusChangeEvent;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataKeyComparator;

/**
 * This class tests getBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceGetBusinessObjectDataTest extends AbstractServiceTest
{
    @Test
    public void testGetBusinessObjectDataUsingCreateRequest()
    {
        BusinessObjectDataCreateRequest request = getNewBusinessObjectDataCreateRequest();
        StorageUnitCreateRequest storageUnit = request.getStorageUnits().get(0);

        // Create the business object data.
        businessObjectDataService.createBusinessObjectData(request);

        // Retrieve the business object data.
        BusinessObjectData businessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion(), request.getPartitionValue(),
                request.getSubPartitionValues(), INITIAL_DATA_VERSION), request.getPartitionKey(), NO_BDATA_STATUS,
            NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Verify the results.
        assertNotNull(businessObjectData);
        assertNotNull(businessObjectData.getId());
        assertTrue(businessObjectData.getBusinessObjectDefinitionName().equals(request.getBusinessObjectDefinitionName()));
        assertTrue(businessObjectData.getBusinessObjectFormatUsage().equals(request.getBusinessObjectFormatUsage()));
        assertTrue(businessObjectData.getBusinessObjectFormatFileType().equals(request.getBusinessObjectFormatFileType()));
        assertTrue(businessObjectData.getBusinessObjectFormatVersion() == request.getBusinessObjectFormatVersion());
        assertTrue(businessObjectData.getPartitionValue().equals(request.getPartitionValue()));
        assertTrue(businessObjectData.getSubPartitionValues().equals(request.getSubPartitionValues()));
        assertTrue(businessObjectData.getVersion() == INITIAL_DATA_VERSION);
        assertTrue(businessObjectData.isLatestVersion());

        List<StorageUnit> storageUnits = businessObjectData.getStorageUnits();
        assertTrue(storageUnits.size() == 1);
        StorageUnit resultStorageUnit = storageUnits.get(0);
        assertTrue(resultStorageUnit.getStorage().getName().equals(storageUnit.getStorageName()));

        // Check if result storage directory path matches to the directory path from the request.
        assertEquals(storageUnit.getStorageDirectory(), resultStorageUnit.getStorageDirectory());

        // Check if result list of storage files matches to the list from the create request.
        validateStorageFiles(resultStorageUnit.getStorageFiles(), storageUnit.getStorageFiles());

        // Check if result list of attributes matches to the list from the create request.
        validateAttributes(request.getAttributes(), businessObjectData.getAttributes());

        // Validate the parents.
        assertTrue(businessObjectData.getBusinessObjectDataParents().size() == 2);
        Collections.sort(request.getBusinessObjectDataParents(), new BusinessObjectDataKeyComparator());
        assertEquals(request.getBusinessObjectDataParents(), businessObjectData.getBusinessObjectDataParents());

        // Ensure no children exist since the entity is new.
        assertTrue(businessObjectData.getBusinessObjectDataChildren().size() == 0);

        // Get one of the parents.
        BusinessObjectDataKey businessObjectDataParentKey = request.getBusinessObjectDataParents().get(0);

        // Retrieve the business object data for the parent.
        BusinessObjectData businessObjectDataParent = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(businessObjectDataParentKey.getNamespace(), businessObjectDataParentKey.getBusinessObjectDefinitionName(),
                businessObjectDataParentKey.getBusinessObjectFormatUsage(), businessObjectDataParentKey.getBusinessObjectFormatFileType(),
                businessObjectDataParentKey.getBusinessObjectFormatVersion(), businessObjectDataParentKey.getPartitionValue(),
                businessObjectDataParentKey.getSubPartitionValues(), businessObjectDataParentKey.getBusinessObjectDataVersion()), request.getPartitionKey(),
            NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Ensure that the parent contains a single child record and that it is equal to our original business object data we created.
        assertTrue(businessObjectDataParent.getBusinessObjectDataChildren().size() == 1);
        assertEquals(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectData), businessObjectDataParent.getBusinessObjectDataChildren().get(0));
    }

    /**
     * This test validates business object data get when both business object format version and business object data version are specified.
     */
    @Test
    public void testGetBusinessObjectData()
    {
        final int NUMBER_OF_FORMAT_VERSIONS = 3;
        final int NUMBER_OF_DATA_VERSIONS_PER_FORMAT = 3;

        // Create business object data status entities.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Create business object format and data entities.
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, FORMAT_DESCRIPTION,
                businessObjectFormatVersion == SECOND_FORMAT_VERSION, PARTITION_KEY);

            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT;
                businessObjectDataVersion++)
            {
                createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS);
            }
        }

        BusinessObjectData resultBusinessObjectData;

        // Retrieve business object data entity by specifying values for all alternate key fields.
        for (int businessObjectFormatVersion = INITIAL_FORMAT_VERSION; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            for (int businessObjectDataVersion = INITIAL_DATA_VERSION; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT;
                businessObjectDataVersion++)
            {
                resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
                    new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                        SUBPARTITION_VALUES, businessObjectDataVersion), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

                // Validate the returned object.
                validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS, resultBusinessObjectData);
            }
        }

        // Validate that business object data status is ignored when business object data version is specified
        // by retrieving business object data using an incorrect business object data status.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION), PARTITION_KEY, BDATA_STATUS_2, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataMissingRequiredParameters()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to get a business object data instance when business object definition name is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get a business object data instance when business object format usage is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get a business object data instance when business object format file type is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get a business object data instance when partition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get a business object data instance when subpartition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataMissingOptionalParameters()
    {
        // Create and persist a set of business object data instances without subpartition values.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS));

        // Retrieve business object data entity by not specifying any of the optional parameters including the subpartition values.
        for (String partitionKey : Arrays.asList(null, BLANK_TEXT))
        {
            BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, NO_SUBPARTITION_VALUES, null),
                partitionKey, NO_BDATA_STATUS, null);

            // Validate the returned object.
            validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID,
                resultBusinessObjectData);
        }
    }

    @Test
    public void testGetBusinessObjectDataTrimParameters()
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, true, BDATA_STATUS);

        // Retrieve the business object data using input parameters with leading and trailing empty spaces.
        // Please note that we do not specify business object data version in order for the business object data status to have an effect.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), NO_DATA_VERSION),
            addWhitespace(PARTITION_KEY), addWhitespace(BDATA_STATUS), NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataUpperCaseParameters()
    {
        // Create and persist a business object data entity using lower case values.
        createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            FORMAT_VERSION, FORMAT_DESCRIPTION, false, PARTITION_KEY.toLowerCase());
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toLowerCase());

        // Retrieve business object data entity using upper case input parameters (except for case-sensitive partition values).
        // Please note that we do not specify business object data version in order for the business object data status to have an effect.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), NO_DATA_VERSION), PARTITION_KEY.toUpperCase(),
            BDATA_STATUS.toUpperCase(), NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, true,
            BDATA_STATUS.toLowerCase(), resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataLowerCaseParameters()
    {
        // Create and persist a business object data entity using upper case values.
        createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            FORMAT_VERSION, FORMAT_DESCRIPTION, false, PARTITION_KEY.toUpperCase());
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toUpperCase());

        // Retrieve business object data entity using lower case input parameters (except for case-sensitive partition values).
        // Please note that we do not specify business object data version in order for the business object data status to have an effect.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), NO_DATA_VERSION), PARTITION_KEY.toLowerCase(),
            BDATA_STATUS.toLowerCase(), NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, true,
            BDATA_STATUS.toUpperCase(), resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataInvalidParameters()
    {
        // Create and persist a valid business object data.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, true, BDATA_STATUS);

        // Validate that we can perform a get on our business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);

        // Try to perform a get using invalid business object definition name.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid format usage.        
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid format file type.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid partition key.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), "I_DO_NOT_EXIST", NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when using an invalid partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", "I_DO_NOT_EXIST", PARTITION_KEY),
                e.getMessage());
        }

        // Try to perform a get using invalid partition value.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST", SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid subpartition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>();
            try
            {
                testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
                testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
                businessObjectDataService.getBusinessObjectData(
                    new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
            }
        }

        // Try to perform a get using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an IllegalArgumentException when passing too many subpartition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }

        // Try to perform a get using invalid business object format version.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid business object data version.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    INVALID_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using an invalid business object data status.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS_2, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", BDATA_STATUS_2), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataBusinessObjectDataNoExists()
    {
        // Create a business object data status entity.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Try to get a non-existing business object data without specifying business object data status.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to get a non-existing business object data by specifying a business object data status.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, NO_DATA_VERSION, BDATA_STATUS), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataParentChildPresent()
    {
        // Create a parent business object data entity.
        BusinessObjectDataEntity businessObjectDataParentEntity =
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, true, BDATA_STATUS);

        // Create an object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create a child business object data entity.
        BusinessObjectDataEntity businessObjectDataChildEntity =
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION, true, BDATA_STATUS);

        // Associate our business object data entity with the child and parent business object data entities.
        businessObjectDataParentEntity.getBusinessObjectDataChildren().add(businessObjectDataEntity);
        businessObjectDataEntity.getBusinessObjectDataParents().add(businessObjectDataParentEntity);
        businessObjectDataEntity.getBusinessObjectDataChildren().add(businessObjectDataChildEntity);
        businessObjectDataChildEntity.getBusinessObjectDataParents().add(businessObjectDataEntity);

        // Retrieve the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE_2, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);

        // Make sure that the parent data's key is listed.
        assertEquals(1, resultBusinessObjectData.getBusinessObjectDataParents().size());
        assertEquals(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataParentEntity),
            resultBusinessObjectData.getBusinessObjectDataParents().get(0));

        // Make sure that the child data's key is listed.
        assertEquals(1, resultBusinessObjectData.getBusinessObjectDataChildren().size());
        assertEquals(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataChildEntity),
            resultBusinessObjectData.getBusinessObjectDataChildren().get(0));
    }

    @Test
    public void testGetBusinessObjectDataIncludeBusinessObjectDataStatusHistory()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Retrieve the business object data with enabled include business object data status history flag.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY, BusinessObjectDataStatusEntity.VALID, INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Build the expected response object.
        BusinessObjectData expectedBusinessObjectData =
            new BusinessObjectData(businessObjectDataEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                NO_STORAGE_UNITS, NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, Arrays.asList(
                new BusinessObjectDataStatusChangeEvent(BusinessObjectDataStatusEntity.VALID,
                    HerdDateUtils.getXMLGregorianCalendarValue(IterableUtils.get(businessObjectDataEntity.getHistoricalStatuses(), 0).getCreatedOn()))));

        // Validate the returned response object.
        assertEquals(expectedBusinessObjectData, resultBusinessObjectData);
    }

    /**
     * This test validates business object data get when business object format version is specified, but business object data version is not.
     */
    @Test
    public void testGetBusinessObjectDataNoDataVersionSpecified()
    {
        // Create business object data status entities.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Create business object data entities with subpartition values.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS),
            createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS));

        BusinessObjectData resultBusinessObjectData;

        // Retrieve a business object data for the initial business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the result for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(0).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET,
            BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);

        // Retrieve a business object data for the second business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the result for the second business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(2).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            SECOND_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            resultBusinessObjectData);

        // Try to retrieve business object data for the third business object format version.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when latest valid business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Retrieve a business object data by explicitly specifying VALID business object data status.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, BusinessObjectDataStatusEntity.VALID, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest VALID business object data version available for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(0).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET,
            BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);

        // Retrieve a business object data by explicitly specifying business object data status testing value.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest business object data version with
        // the test business object data status available for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);

        // Try to retrieve a business object data by explicitly specifying an incorrect business object data status value.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS_2, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, NO_DATA_VERSION, BDATA_STATUS_2), e.getMessage());
        }
    }

    /**
     * This test validates business object data get when business object data version is specified, but business object format version is not.
     */
    @Test
    public void testGetBusinessObjectDataNoFormatVersionSpecified()
    {
        // Create business object data status entities.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Create business object format entities.
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
            NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, FORMAT_DESCRIPTION,
            NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FOURTH_FORMAT_VERSION, FORMAT_DESCRIPTION,
            NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FIFTH_FORMAT_VERSION, FORMAT_DESCRIPTION,
            NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create business object data entities for the initial format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, THIRD_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create business object data entities for the second format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, THIRD_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create business object data entities for the third format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create business object data entities for the fourth format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FOURTH_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        BusinessObjectData resultBusinessObjectData;

        // Retrieve an initial business object data version without specifying business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);

        // Retrieve a second business object data version without specifying business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                SECOND_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FOURTH_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);

        // Retrieve a third business object data version without specifying business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                THIRD_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, THIRD_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);

        // Validate that business object data status is ignored when business object data version is specified
        // by retrieving business object data using no business object format version and an incorrect business object data status.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION), PARTITION_KEY, BDATA_STATUS_2, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);
    }

    /**
     * This test validates business object data get when both business object format and data versions are not specified.
     */
    @Test
    public void testGetBusinessObjectDataBothFormatAndDataVersionsNotSpecified()
    {
        // Create business object data status entities.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Create business object format entities.
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
            NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
            LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create business object data entities for the initial format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, THIRD_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create business object data entities for the second format version.
        createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        BusinessObjectData resultBusinessObjectData;

        // Retrieve a business object data without specifying business object format version, business object data version, and business object data status.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                NO_DATA_VERSION), PARTITION_KEY, NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest VALID business object format and data version available.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);

        // Retrieve a business object data by explicitly specifying VALID business object data status.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                NO_DATA_VERSION), PARTITION_KEY, BusinessObjectDataStatusEntity.VALID, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest VALID business object format and data version available.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);

        // Retrieve a business object data by explicitly specifying business object data status testing value.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest business object format and data version with the test business object data status.
        validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);

        // Try to retrieve a business object data by explicitly specifying an incorrect business object data status value.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION), PARTITION_KEY, BDATA_STATUS_2, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, NO_DATA_VERSION, BDATA_STATUS_2), e.getMessage());
        }
    }
}
