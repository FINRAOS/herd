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

import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;


/**
 * The attribute value list service.
 */
public interface AttributeValueListService
{

    /**
     * Creates a new attribute value list.
     *
     * @param request the information needed to create a attribute value list
     *
     * @return the newly created attribute value list
     */
    AttributeValueList createAttributeValueList(AttributeValueListCreateRequest request);

    /**
     * Deletes an existing attribute value list by key.
     *
     * @param attributeValueListKey the attribute value list key
     *
     * @return the attribute value list key information for the attribute that got deleted
     */
    AttributeValueListKey deleteAttributeValueList(AttributeValueListKey attributeValueListKey);

    /**
     * Gets an existing attribute value list by key.
     *
     * @param attributeValueListKey the attribute value list key
     *
     * @return the attribute value list information
     */
    AttributeValueList getAttributeValueList(AttributeValueListKey attributeValueListKey);

    /**
     * Gets a list of keys for all existing attribute value lists.
     *
     * @return the list of attribute value list keys
     */
    AttributeValueListKeys getAttributeValueListKeys();
}
