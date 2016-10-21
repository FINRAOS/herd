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

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.TagEntity;

public interface BusinessObjectDefinitionTagDao extends BaseJpaDao
{
    /**
     * Gets a business object definition tag by it's key.
     *
     * @param businessObjectDefinitionTagKey the business object definition tag key (case-insensitive)
     *
     * @return the business object definition tag entity
     */
    public BusinessObjectDefinitionTagEntity getBusinessObjectDefinitionTagByKey(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey);

    /**
     * Gets a business object definition tag by business object definition and tag entities.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param tagEntity the tag entity
     *
     * @return the business object definition tag entity
     */
    public BusinessObjectDefinitionTagEntity getBusinessObjectDefinitionTagByParentEntities(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        TagEntity tagEntity);

    /**
     * Gets a list of keys for all business object definition tags associated with the specified business object definition entity. The list of keys is ordered
     * by tag display name.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     *
     * @return the list of business object definition tag keys
     */
    public List<BusinessObjectDefinitionTagKey> getBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity);

    /**
     * Gets a list of keys for all business object definition tags associated with the specified tag entity. The list of keys is ordered by business object
     * definition display name and business object definition name.
     *
     * @param tagEntity the tag entity
     *
     * @return the list of business object definition tag keys
     */
    public List<BusinessObjectDefinitionTagKey> getBusinessObjectDefinitionTagsByTagEntity(TagEntity tagEntity);
}
