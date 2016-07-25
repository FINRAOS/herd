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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataKeyComparator;

/**
 * This class tests getBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGetBusinessObjectDataTest extends AbstractRestTest
{
    @Test
    public void testGetBusinessObjectDataUsingCreateRequest()
    {
        BusinessObjectDataCreateRequest request = getNewBusinessObjectDataCreateRequest();
        StorageUnitCreateRequest storageUnit = request.getStorageUnits().get(0);

        // Create the business object data.
        businessObjectDataRestController.createBusinessObjectData(request);

        // Retrieve the business object data.
        BusinessObjectData businessObjectData = businessObjectDataRestController
            .getBusinessObjectData(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getPartitionKey(), request.getPartitionValue(),
                getDelimitedFieldValues(request.getSubPartitionValues()), request.getBusinessObjectFormatVersion(), INITIAL_DATA_VERSION, NO_BDATA_STATUS,
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
        BusinessObjectData businessObjectDataParent = businessObjectDataRestController
            .getBusinessObjectData(businessObjectDataParentKey.getNamespace(), businessObjectDataParentKey.getBusinessObjectDefinitionName(),
                businessObjectDataParentKey.getBusinessObjectFormatUsage(), businessObjectDataParentKey.getBusinessObjectFormatFileType(),
                request.getPartitionKey(), businessObjectDataParentKey.getPartitionValue(),
                getDelimitedFieldValues(businessObjectDataParentKey.getSubPartitionValues()), businessObjectDataParentKey.getBusinessObjectFormatVersion(),
                businessObjectDataParentKey.getBusinessObjectDataVersion(), NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Ensure that the parent contains a single child record and that it is equal to our original business object data we created.
        assertTrue(businessObjectDataParent.getBusinessObjectDataChildren().size() == 1);
        assertEquals(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectData), businessObjectDataParent.getBusinessObjectDataChildren().get(0));
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectDataVersion()
    {
        final int NUMBER_OF_FORMAT_VERSIONS = 3;
        final int NUMBER_OF_DATA_VERSIONS_PER_FORMAT = 3;

        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                    FORMAT_DESCRIPTION, businessObjectFormatVersion == SECOND_FORMAT_VERSION, PARTITION_KEY);

            for (int businessObjectDataVersion = 0; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT; businessObjectDataVersion++)
            {
                businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion,
                        PARTITION_VALUE, SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS);
            }
        }

        // Retrieve business object data entity by specifying values for all alternate key fields except for the business object data status.
        for (int businessObjectFormatVersion = 0; businessObjectFormatVersion < NUMBER_OF_FORMAT_VERSIONS; businessObjectFormatVersion++)
        {
            for (int businessObjectDataVersion = 0; businessObjectDataVersion < NUMBER_OF_DATA_VERSIONS_PER_FORMAT; businessObjectDataVersion++)
            {
                BusinessObjectData resultBusinessObjectData = businessObjectDataRestController
                    .getBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                        getDelimitedFieldValues(SUBPARTITION_VALUES), businessObjectFormatVersion, businessObjectDataVersion, NO_BDATA_STATUS,
                        NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

                // Validate the returned object.
                validateBusinessObjectData(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS, resultBusinessObjectData);
            }
        }
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectDataStatus()
    {
        // Create business object data entities with subpartition values.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS));

        BusinessObjectData resultBusinessObjectData;

        // Retrieve a business object data by explicitly specifying VALID business object data status.
        // Please note that we do not specify business object data version in order for the business object data status to have an effect.
        resultBusinessObjectData = businessObjectDataRestController
            .getBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                getDelimitedFieldValues(SUBPARTITION_VALUES), INITIAL_FORMAT_VERSION, NO_DATA_VERSION, BusinessObjectDataStatusEntity.VALID,
                NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest VALID business object data version available for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(0).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET,
            BusinessObjectDataStatusEntity.VALID, resultBusinessObjectData);

        // Retrieve a business object data by explicitly specifying business object data status testing value.
        // Please note that we do not specify business object data version in order for the business object data status to have an effect.
        resultBusinessObjectData = businessObjectDataRestController
            .getBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                getDelimitedFieldValues(SUBPARTITION_VALUES), INITIAL_FORMAT_VERSION, NO_DATA_VERSION, BDATA_STATUS,
                NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

        // Validate the returned object, it should be the latest business object data version with
        // the test business object data status available for the initial business object format version.
        validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, resultBusinessObjectData);
    }

    @Test
    public void testGetBusinessObjectDataMissingOptionalParameters()
    {
        // Create and persist a set of business object data instances without subpartition values.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS));

        // Retrieve business object data entity by not specifying any of the optional parameters including the subpartition values.
        for (String partitionKey : Arrays.asList(null, BLANK_TEXT))
        {
            BusinessObjectData resultBusinessObjectData = businessObjectDataRestController
                .getBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, partitionKey, PARTITION_VALUE, null, null, null,
                    NO_BDATA_STATUS, NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY);

            // Validate the returned object.
            validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID,
                resultBusinessObjectData);
        }
    }
}
