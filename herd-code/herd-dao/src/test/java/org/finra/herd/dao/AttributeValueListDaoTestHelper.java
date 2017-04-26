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

import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE_LIST_NAME;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class AttributeValueListDaoTestHelper
{

    @Autowired
    private AttributeValueListDao attributeValueListDao;

    public AttributeValueListEntity createAttributeValueListEntity(NamespaceEntity namespaceEntity, String attributValueListName)
    {
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attributValueListName);
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }

    public AttributeValueListKeys getTestAttributeValueListKeys() {

        AttributeValueListKey attributeValueListKey = new AttributeValueListKey("NamespaceTest" + AbstractDaoTest.getRandomSuffix(), ATTRIBUTE_VALUE_LIST_NAME);
        AttributeValueListKey attributeValueListKey1 = new AttributeValueListKey("NamespaceTest" + AbstractDaoTest.getRandomSuffix(), ATTRIBUTE_VALUE_LIST_NAME);

        return new AttributeValueListKeys(Arrays.asList(attributeValueListKey,attributeValueListKey1));
    }

}
