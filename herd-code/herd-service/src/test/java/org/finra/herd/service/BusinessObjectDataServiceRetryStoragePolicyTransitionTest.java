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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * This class tests retryStoragePolicyTransition functionality within the business object data service.
 */
public class BusinessObjectDataServiceRetryStoragePolicyTransitionTest extends AbstractServiceTest
{
    @Test
    public void testRetryStoragePolicyTransition()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition for the business object data.
        BusinessObjectData businessObjectData = businessObjectDataService
            .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }
}
