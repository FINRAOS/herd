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

import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class AttributeValueListDaoTestHelper
{
    @Autowired
    private AttributeValueListDao attributeValueListDao;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    /**
     * Creates attribute value list entity and save it in the database.
     *
     * @param namespace the namespace of the attribute value list
     * @param attributeValueListName the name of the attribute value list
     *
     * @return the attribute value list entity
     */
    public AttributeValueListEntity createAttributeValueListEntity(String namespace, String attributeValueListName)
    {
        // Create a namespace entity if not exists.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(namespace);
        }

        // Create an attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attributeValueListName);

        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }
}
