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

package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.Namespace;

/**
 * This class tests various functionality within the attribute value list REST controller.
 */
public class AttributeValueListRestControllerTest extends AbstractRestTest
{


    @Test
    public void testCreateAttributeValueList() throws Exception
    {
        // Create a attribute value list.
        AttributeValueList resultAttributeValueList = attributeValueListRestController
            .createAttributeValueList(new AttributeValueListCreateRequest(
                new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)));

        // Validate the returned object.
        assertEquals(new AttributeValueList(1220, new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)), resultAttributeValueList);
    }

    @Test
    public void testDeleteAttributeValueList() throws Exception
    {
        Namespace namespace = namespaceService.createNamespace(namespaceServiceTestHelper.createNamespaceCreateRequest(NAMESPACE));

        // Create and persist a attribute value list entity.
        attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);

        // Validate that this attribute value list exists.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);
        assertNotNull(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey));

        // Delete this attribute value list.
        AttributeValueList deletedAttributeValueList = attributeValueListRestController.deleteAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Validate the returned object.
        assertEquals(new AttributeValueList(1002, new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)), deletedAttributeValueList);

        // Ensure that this attribute value list is no longer there.
        assertNull(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey));
    }

    @Test
    public void testGetAttributeValueList() throws Exception
    {
        // Create and persist a attribute value list entity.
        attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);

        // Retrieve the attribute value list.
        AttributeValueList resultAttributeValueList = attributeValueListRestController.getAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Validate the returned object.
        assertEquals(new AttributeValueList(1002, new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)), resultAttributeValueList);
    }

    @Test
    public void testGetAttributeValueLists() throws Exception
    {
        // Create and persist attribute value list entities.
        attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);
        attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);

        // Retrieve a list of attribute value list keys.
        AttributeValueListKeys resultAttributeValueListKeys = attributeValueListRestController.getAttributeValueLists();

        // Validate the returned object.
        assertNotNull(resultAttributeValueListKeys);
        assertEquals(attributeValueListDaoTestHelper.getTestAttributeValueListKeys(), resultAttributeValueListKeys.getAttributeValueListKeys());
    }

}

