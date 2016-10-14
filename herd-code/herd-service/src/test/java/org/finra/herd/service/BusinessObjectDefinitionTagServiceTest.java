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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;

/**
 * This class tests functionality within the business object definition tag service.
 */
public class BusinessObjectDefinitionTagServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDefinitionTag()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a business object definition tag.
        BusinessObjectDefinitionTag result =
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(businessObjectDefinitionTagKey));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(result.getId(), businessObjectDefinitionTagKey), result);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTag()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Validate that this business object definition tag exists.
        assertNotNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Delete this business object definition tag.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(businessObjectDefinitionTagKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), businessObjectDefinitionTagKey), result);

        // Ensure that this business object definition tag is no longer there.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));
    }
}
