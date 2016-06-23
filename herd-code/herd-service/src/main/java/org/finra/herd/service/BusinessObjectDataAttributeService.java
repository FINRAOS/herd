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

import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * The business object data attribute service.
 */
public interface BusinessObjectDataAttributeService
{
    /**
     * Creates a new business object data attribute.
     *
     * @param businessObjectDataAttributeCreateRequest the information needed to create a business object data attribute
     *
     * @return the newly created business object data attribute
     */
    public BusinessObjectDataAttribute createBusinessObjectDataAttribute(BusinessObjectDataAttributeCreateRequest businessObjectDataAttributeCreateRequest);

    /**
     * Gets an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute information
     */
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey);

    /**
     * Updates an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     * @param businessObjectDataAttributeUpdateRequest the information needed to update a business object data attribute
     *
     * @return the business object data attribute information
     */
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey,
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest);

    /**
     * Deletes an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute information for the attribute that got deleted
     */
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey);

    /**
     * Gets a list of keys for all existing business object data attributes.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the list of business object data attribute keys
     */
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(BusinessObjectDataKey businessObjectDataKey);
}
