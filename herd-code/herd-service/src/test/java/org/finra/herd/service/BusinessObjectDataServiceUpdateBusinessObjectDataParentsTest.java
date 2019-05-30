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

import java.util.Collections;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataParentsUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;

/**
 * This class tests updateBusinessObjectDataParents functionality within the business object data service.
 */
public class BusinessObjectDataServiceUpdateBusinessObjectDataParentsTest extends AbstractServiceTest
{
    @Test
    public void testUpdateBusinessObjectDataParents()
    {
        // Create a business object data key for the first parent.
        BusinessObjectDataKey businessObjectDataParentOneKey =
            new BusinessObjectDataKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION);

        // Create a business object data key for the second parent.
        BusinessObjectDataKey businessObjectDataParentTwoKey =
            new BusinessObjectDataKey(NAMESPACE_3, BDEF_NAME_3, FORMAT_USAGE_CODE_3, FORMAT_FILE_TYPE_CODE_3, FORMAT_VERSION, PARTITION_VALUE_3,
                SUBPARTITION_VALUES_3, DATA_VERSION);

        // Create a business object data key for the child.
        BusinessObjectDataKey businessObjectDataChildKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create the first business object data parent.
        BusinessObjectDataEntity businessObjectDataParentOneEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataParentOneKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create the first business object data parent.
        BusinessObjectDataEntity businessObjectDataParentTwoEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataParentTwoKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create a child business object data entity having a pre-registration status.
        BusinessObjectDataEntity businessObjectDataChildEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataChildKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);

        // Associate child and first parent business object data entities.
        businessObjectDataParentOneEntity.getBusinessObjectDataChildren().add(businessObjectDataChildEntity);
        businessObjectDataChildEntity.getBusinessObjectDataParents().add(businessObjectDataParentOneEntity);

        // Update parents for business object data.
        BusinessObjectData businessObjectData = businessObjectDataService.updateBusinessObjectDataParents(businessObjectDataChildKey,
            new BusinessObjectDataParentsUpdateRequest(Collections.singletonList(businessObjectDataParentTwoKey)));

        // Validate the response.
        assertNotNull(businessObjectData);
        assertEquals(1, CollectionUtils.size(businessObjectData.getBusinessObjectDataParents()));
        assertEquals(businessObjectDataParentTwoKey, businessObjectData.getBusinessObjectDataParents().get(0));

        // Validate the parent entities.
        assertTrue(CollectionUtils.isEmpty(businessObjectDataParentOneEntity.getBusinessObjectDataChildren()));
        assertEquals(1, CollectionUtils.size(businessObjectDataParentTwoEntity.getBusinessObjectDataChildren()));
    }
}
