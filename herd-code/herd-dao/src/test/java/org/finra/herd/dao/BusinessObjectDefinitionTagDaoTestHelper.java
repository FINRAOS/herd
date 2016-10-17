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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.TagEntity;

@Component
public class BusinessObjectDefinitionTagDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagDaoTestHelper tagDaoTestHelper;

    /**
     * Creates and persists a new business object definition tag entity.
     *
     * @param businessObjectDefinitionTagKey the business object definition tag key
     *
     * @return the newly created business object definition tag entity
     */
    public BusinessObjectDefinitionTagEntity createBusinessObjectDefinitionTagEntity(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey)
    {
        return createBusinessObjectDefinitionTagEntity(businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey(),
            businessObjectDefinitionTagKey.getTagKey());
    }

    /**
     * Creates and persists a new business object definition tag entity.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param tagKey the tag key
     *
     * @return the newly created business object definition tag entity
     */
    public BusinessObjectDefinitionTagEntity createBusinessObjectDefinitionTagEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey, TagKey tagKey)
    {
        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, AbstractDaoTest.DATA_PROVIDER_NAME, AbstractDaoTest.DESCRIPTION);
        }

        // Create a tag entity if needed.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        if (tagEntity == null)
        {
            tagEntity = tagDaoTestHelper
                .createTagEntity(tagKey.getTagTypeCode(), tagKey.getTagCode(), AbstractDaoTest.TAG_DISPLAY_NAME, AbstractDaoTest.TAG_DESCRIPTION);
        }

        // Create a business object definition entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = new BusinessObjectDefinitionTagEntity();
        businessObjectDefinitionTagEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionTagEntity.setTag(tagEntity);

        // Persist and return the newly created entity.
        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionTagEntity);
    }
}
