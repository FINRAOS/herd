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

import java.util.Collection;
import java.util.List;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AttributeValueListEntity;

public interface AttributeValueListDao extends BaseJpaDao
{
    /**
     * Gets an attribute value list entity by its key.
     *
     * @param attributeValueListKey the attribute value list key (case-insensitive)
     *
     * @return the attribute value list entity
     */
    AttributeValueListEntity getAttributeValueListByKey(AttributeValueListKey attributeValueListKey);

    /**
     * Gets a list of attribute value list keys per specified list of namespaces. When an empty list of namespaces is passed, the method returns keys for all
     * attribute value lists registered in the system.
     *
     * @param namespaces the list of namespaces (case-sensitive)
     *
     * @return the list of attribute value list keys
     */
    List<AttributeValueListKey> getAttributeValueLists(Collection<String> namespaces);
}
