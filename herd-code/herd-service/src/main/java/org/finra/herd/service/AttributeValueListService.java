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
import org.finra.herd.model.jpa.AttributeValueListEntity;


/**
 * The attribute value list service.
 */
public interface AttributeValueListService
{

    /**
     * Creates a new attribute value list.
     *
     * @param attributeValueListCreateRequest the information needed to create a attribute value list
     *
     * @return the newly created attribute value list
     */
    public AttributeValueListEntity createAttributeValueList(AttributeValueListCreateRequest attributeValueListCreateRequest);

    /**
     * Gets an existing attribute value list by key.
     *
     * @param attributeValueListKey the attribute value list key
     *
     * @return the attribute value list information
     */
    public AttributeValueListEntity getAttributeValueList(AttributeValueListKey attributeValueListKey);

    /**
     * Deletes an existing attribute value list by key.
     *
     * @param attributeValueListKey the attribute value list key
     *
     * @return the attribute value list information for the attribute that got deleted
     */
    public AttributeValueListEntity deleteAttributeValueList(AttributeValueListKey attributeValueListKey);

    /**
     * Gets a list of keys for all existing attribute value lists.
     *
     * @param namespace the attribute value list key
     *
     * @return the list of attribute value list keys
     */
    public AttributeValueListKeys getAttributeValueLists(String namespace);

    /**
     * Gets a list of keys for all existing attribute value lists.
     *
     * @return the list of attribute value list keys
     */
    public AttributeValueListKeys getAttributeValueLists();

}
