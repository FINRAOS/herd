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

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.service.helper.BusinessObjectDataKeyComparator;
import org.finra.herd.service.helper.HerdDaoHelper;

/**
 * This class tests getBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceGetBusinessObjectDataTest extends AbstractServiceTest
{
    @Autowired
    HerdDaoHelper herdDaoHelper;

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
                request.getSubPartitionValues(), INITIAL_DATA_VERSION), request.getPartitionKey());

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
                businessObjectDataParentKey.getSubPartitionValues(), businessObjectDataParentKey.getBusinessObjectDataVersion()), request.getPartitionKey());

        // Ensure that the parent contains a single child record and that it is equal to our original business object data we created.
        assertTrue(businessObjectDataParent.getBusinessObjectDataChildren().size() == 1);
        assertEquals(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectData), businessObjectDataParent.getBusinessObjectDataChildren().get(0));
    }

    // Testing Business Object Data Get...
    // Case 1: Both Format Version and Data Version are specified.

    @Test
    public void testGetBusinessObjectData()
    {
        final int NUMBER_OF_FORMAT_VERSIONS = 3;
        final int NUMBER_OF_DATA_VERSIONS_PER_FORMAT = 3;

        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, FORMAT_DESCRIPTION,
                businessObjectFormatVersion == SECOND_FORMAT_VERSION, PARTITION_KEY);

            for (int businessObjectDataVersion = 0; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT; businessObjectDataVersion++)
            {
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS);
            }
        }

        // Retrieve business object data entity by specifying values for all alternate key fields.
        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            for (int businessObjectDataVersion = 0; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT; businessObjectDataVersion++)
            {
                BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
                    new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                        SUBPARTITION_VALUES, businessObjectDataVersion), PARTITION_KEY);

                // Validate the returned object.
                validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS, resultBusinessObjectData);
            }
        }
    }

    @Test
    public void testGetBusinessObjectDataMissingRequiredParameters()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to get a business object data instance when business object definition name is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION), PARTITION_KEY);
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
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS));

        // Retrieve business object data entity by not specifying any of the optional parameters including the subpartition values.
        for (String partitionKey : Arrays.asList(null, BLANK_TEXT))
        {
            BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                    null), partitionKey);

            // Validate the returned object.
            validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID,
                resultBusinessObjectData);
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Retrieve the business object data using input parameters with leading and trailing empty spaces.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            addWhitespace(PARTITION_KEY));

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataUpperCaseParameters()
    {
        // Create and persist a business object data entity using lower case values.
        createBusinessObjectFormatEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, FORMAT_DESCRIPTION, false, PARTITION_KEY.toLowerCase());
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toLowerCase());

        // Retrieve business object data entity using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION), PARTITION_KEY.toUpperCase());

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, true,
            BDATA_STATUS.toLowerCase(), resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataLowerCaseParameters()
    {
        // Create and persist a business object data entity using upper case values.
        createBusinessObjectFormatEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, FORMAT_DESCRIPTION, false, PARTITION_KEY.toUpperCase());
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toUpperCase());

        // Retrieve business object data entity using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION), PARTITION_KEY.toLowerCase());

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, true,
            BDATA_STATUS.toUpperCase(), resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataInvalidParameters()
    {
        // Create and persist a valid business object data.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Validate that we can perform a get on our business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);

        // Try to perform a get using invalid business object definition name.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid format usage.        
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid format file type.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid partition key.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), "I_DO_NOT_EXIST");
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
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
                    new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), PARTITION_KEY);
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
            }
        }

        // Try to perform a get using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, DATA_VERSION), PARTITION_KEY);
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
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Try to perform a get using invalid business object data version.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataBusinessObjectDataNoExists()
    {
        // Try to get a non-existing business object data.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataParentChildPresent()
    {
        // Create a parent business object data entity.
        BusinessObjectDataEntity businessObjectDataParentEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create an object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create a child business object data entity.
        BusinessObjectDataEntity businessObjectDataChildEntity =
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION, true, BDATA_STATUS);

        // Associate our business object data entity with the child and parent business object data entities.
        businessObjectDataParentEntity.getBusinessObjectDataChildren().add(businessObjectDataEntity);
        businessObjectDataEntity.getBusinessObjectDataParents().add(businessObjectDataParentEntity);
        businessObjectDataEntity.getBusinessObjectDataChildren().add(businessObjectDataChildEntity);
        businessObjectDataChildEntity.getBusinessObjectDataParents().add(businessObjectDataEntity);

        // Retrieve the business object data.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE_2, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);

        // Make sure that the child data's key is listed.
        assertEquals(1, resultBusinessObjectData.getBusinessObjectDataParents().size());
        assertEquals(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataParentEntity), resultBusinessObjectData.getBusinessObjectDataParents().get(0));

        // Make sure that the child data's key is listed.
        assertEquals(1, resultBusinessObjectData.getBusinessObjectDataChildren().size());
        assertEquals(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataChildEntity), resultBusinessObjectData.getBusinessObjectDataChildren().get(0));
    }

    // Testing Business Object Data Get...
    // Case 2: Format Version is specified, but Data Version is not.

    @Test
    public void testGetBusinessObjectDataNoDataVersionSpecified()
    {
        // Create and persist a set of business object data instances with subpartition values.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS),
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS));

        // Retrieve business object data and validate the result for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(0).getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID,
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null), PARTITION_KEY));

        // Retrieve business object data and validate the result for the second business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            SECOND_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID,
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null), PARTITION_KEY));

        // Try to retrieve business object data for the third business object format version.
        try
        {
            businessObjectDataService.getBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when latest valid business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, THIRD_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, null, BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }
    }

    // Testing Business Object Data Get...
    // Case 3: Format Version is not specified, but Data Version is.

    @Test
    public void testGetBusinessObjectDataNoFormatVersionSpecified()
    {
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, FORMAT_DESCRIPTION, false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, FORMAT_DESCRIPTION, true, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, FORMAT_DESCRIPTION, false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, FORMAT_DESCRIPTION, false, PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 4, FORMAT_DESCRIPTION, false, PARTITION_KEY);

        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, PARTITION_VALUE, SUBPARTITION_VALUES,
            INITIAL_DATA_VERSION, false, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, false, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, PARTITION_VALUE, SUBPARTITION_VALUES,
            THIRD_DATA_VERSION, true, BDATA_STATUS);

        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, PARTITION_VALUE, SUBPARTITION_VALUES,
            INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, PARTITION_VALUE, SUBPARTITION_VALUES,
            THIRD_DATA_VERSION, false, BDATA_STATUS);

        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, PARTITION_VALUE, SUBPARTITION_VALUES,
            INITIAL_DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, false, BDATA_STATUS);

        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, false, BDATA_STATUS);

        // Retrieve an initial business object data version without specifying business object format version.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 2, PARTITION_VALUE, SUBPARTITION_VALUES,
            INITIAL_DATA_VERSION, true, BDATA_STATUS, resultBusinessObjectData);

        // Retrieve a second business object data version without specifying business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                SECOND_DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 3, PARTITION_VALUE, SUBPARTITION_VALUES,
            SECOND_DATA_VERSION, false, BDATA_STATUS, resultBusinessObjectData);

        // Retrieve a third business object data version without specifying business object format version.
        resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                THIRD_DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, PARTITION_VALUE, SUBPARTITION_VALUES,
            THIRD_DATA_VERSION, false, BDATA_STATUS, resultBusinessObjectData);
    }

    // Testing Business Object Data Get...
    // Case 4: Both Format Version and Data Version are not specified:

    @Test
    public void testGetBusinessObjectDataBothFormatAndDataVersionsNotSpecified()
    {
        // Create database entities required for testing.
        // Create business object format entities.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION, false,
            PARTITION_KEY);
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY);
        // Create business object data entities for the initial format version.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID);
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, THIRD_DATA_VERSION, true, BDATA_STATUS);
        // Create business object data entities for the second format version.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Retrieve a business object data without specifying both business object format and business object data versions.
        BusinessObjectData resultBusinessObjectData = businessObjectDataService.getBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES, null),
            PARTITION_KEY);

        // Validate the returned object, it should be the latest VALID business object data version available.
        validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, SECOND_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);
    }
}
