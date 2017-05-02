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

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;

@Component
public class AttributeValueListDaoTestHelper
{
    @Autowired
    private AttributeValueListDao attributeValueListDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    /**
     * Create attribute value list entity and save it in the database
     *
     * @param namespace namespace of the attribute value list.
     * @param attributeValueListName name of the attribute value list
     *
     * @return Attribute value list entity
     */

    public AttributeValueListEntity createAttributeValueListEntity(String namespace, String attributeValueListName)
    {
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceDaoTestHelper.createNamespaceEntity(namespace));
        attributeValueListEntity.setAttributeValueListName(attributeValueListName);
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }

    /**
     * @param namespace
     * @param attributeValueListName
     *
     * @return
     */
    public AttributeValueListKeys getTestAttributeValueListKeys(String namespace, String attributeValueListName)
    {
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(namespace, attributeValueListName);
        AttributeValueListKey attributeValueListKeyDuplicate = new AttributeValueListKey(namespace, attributeValueListName);

        return new AttributeValueListKeys(Arrays.asList(attributeValueListKey, attributeValueListKeyDuplicate));
    }
}
