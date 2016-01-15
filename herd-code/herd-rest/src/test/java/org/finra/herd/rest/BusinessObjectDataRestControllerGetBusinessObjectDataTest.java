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
import org.springframework.beans.factory.annotation.Autowired;

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
public class BusinessObjectDataRestControllerGetBusinessObjectDataTest extends AbstractRestTest
{
    @Autowired
    HerdDaoHelper herdDaoHelper;

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
                getDelimitedFieldValues(request.getSubPartitionValues()), request.getBusinessObjectFormatVersion(), INITIAL_DATA_VERSION);

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
                businessObjectDataParentKey.getBusinessObjectDataVersion());

        // Ensure that the parent contains a single child record and that it is equal to our original business object data we created.
        assertTrue(businessObjectDataParent.getBusinessObjectDataChildren().size() == 1);
        assertEquals(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectData), businessObjectDataParent.getBusinessObjectDataChildren().get(0));
    }

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
                BusinessObjectData resultBusinessObjectData = businessObjectDataRestController
                    .getBusinessObjectData(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                        getDelimitedFieldValues(SUBPARTITION_VALUES), businessObjectFormatVersion, businessObjectDataVersion);

                // Validate the returned object.
                validateBusinessObjectData(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, businessObjectFormatVersion, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataVersion == SECOND_DATA_VERSION, BDATA_STATUS, resultBusinessObjectData);
            }
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
            BusinessObjectData resultBusinessObjectData = businessObjectDataRestController
                .getBusinessObjectData(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, partitionKey, PARTITION_VALUE, null, null, null);

            // Validate the returned object.
            validateBusinessObjectData(businessObjectDataEntities.get(1).getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                SECOND_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BusinessObjectDataStatusEntity.VALID,
                resultBusinessObjectData);
        }
    }
}
