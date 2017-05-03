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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AttributeValueListEntity;

public class AttributeValueListDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetAttributeValueListByKey()
    {
        // Create and persist a attribute value list entity.
        AttributeValueListEntity attributeValueListEntity =
            attributeValueListDaoTestHelper.createAttributeValueListEntity(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Retrieve a attribute value list entity.
        assertEquals(attributeValueListEntity,
            attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)));

        // Test case insensitivity.
        assertEquals(attributeValueListEntity, attributeValueListDao
            .getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE.toUpperCase(), ATTRIBUTE_VALUE_LIST_NAME.toUpperCase())));
        assertEquals(attributeValueListEntity, attributeValueListDao
            .getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE.toLowerCase(), ATTRIBUTE_VALUE_LIST_NAME.toLowerCase())));

        // Confirm negative results when using invalid values.
        assertNull(attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(I_DO_NOT_EXIST, ATTRIBUTE_VALUE_LIST_NAME)));
        assertNull(attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, I_DO_NOT_EXIST)));
    }

    @Test
    public void testGetAttributeValueListKeys()
    {
        // Create several attribute value list keys in random order.
        List<AttributeValueListKey> attributeValueListKeys = Arrays
            .asList(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME_2),
                new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE_2, ATTRIBUTE_VALUE_LIST_NAME_2),
                new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE_2, ATTRIBUTE_VALUE_LIST_NAME),
                new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME));

        // Create and persist attribute value list entities.
        for (AttributeValueListKey attributeValueListKey : attributeValueListKeys)
        {
            attributeValueListDaoTestHelper
                .createAttributeValueListEntity(attributeValueListKey.getNamespace(), attributeValueListKey.getAttributeValueListName());
        }

        // Retrieve a list of attribute value list keys.
        assertEquals(Arrays.asList(attributeValueListKeys.get(3), attributeValueListKeys.get(0)),
            attributeValueListDao.getAttributeValueLists(Arrays.asList(ATTRIBUTE_VALUE_LIST_NAMESPACE)));

        // Test case sensitivity.
        assertEquals(new ArrayList<>(), attributeValueListDao.getAttributeValueLists(Arrays.asList(ATTRIBUTE_VALUE_LIST_NAMESPACE.toUpperCase())));
        assertEquals(new ArrayList<>(), attributeValueListDao.getAttributeValueLists(Arrays.asList(ATTRIBUTE_VALUE_LIST_NAMESPACE.toLowerCase())));

        // Retrieve the list of keys for all attribute value lists registered in the system.
        assertEquals(Arrays.asList(attributeValueListKeys.get(3), attributeValueListKeys.get(0), attributeValueListKeys.get(2), attributeValueListKeys.get(1)),
            attributeValueListDao.getAttributeValueLists(null));
    }
}
