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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKeys;
import org.finra.herd.model.api.xml.TagKey;

/**
 * The business object definition tag service.
 */
public interface BusinessObjectDefinitionTagService
{
    /**
     * Creates a new business object definition tag.
     *
     * @param businessObjectDefinitionTagCreateRequest the information needed to create a business object definition tag
     *
     * @return the newly created business object definition tag
     */
    public BusinessObjectDefinitionTag createBusinessObjectDefinitionTag(BusinessObjectDefinitionTagCreateRequest businessObjectDefinitionTagCreateRequest);

    /**
     * Deletes an existing business object definition tag by key.
     *
     * @param businessObjectDefinitionTagKey the business object definition tag key
     *
     * @return the deleted business object definition tag
     */
    public BusinessObjectDefinitionTag deleteBusinessObjectDefinitionTag(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey);

    /**
     * Gets a list of keys for all existing business object definition tags that are associated with the specified business object definition.
     *
     * @param businessObjectDefinitionKey the key of the business object definition
     *
     * @return the list of business object definition tag keys
     */
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Gets a list of keys for all existing business object definition tags that are associated with the specified tag.
     *
     * @param tagKey the tag key
     *
     * @return the list of business object definition tag keys
     */
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByTag(TagKey tagKey);
}
