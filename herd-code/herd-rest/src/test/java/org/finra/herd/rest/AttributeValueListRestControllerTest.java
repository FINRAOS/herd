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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.service.AttributeValueListService;

/**
 * This class tests various functionality within the attribute value list REST controller.
 */
public class AttributeValueListRestControllerTest extends AbstractRestTest
{

    private static final int ONE_TIME = 1;

    @InjectMocks
    private AttributeValueListRestController attributeValueListRestController;

    @Mock
    private AttributeValueListService attributeValueListService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void testCreateAttributeValueListWithInvalidNamespace() throws Exception
    {
        // Create a attribute value list.
        AttributeValueList resultAttributeValueList =
            new AttributeValueList(1, new AttributeValueListKey(null, ATTRIBUTE_VALUE_LIST_NAME));

        AttributeValueListCreateRequest attributeValueListCreateRequest = new AttributeValueListCreateRequest(
            new AttributeValueListKey(null, ATTRIBUTE_VALUE_LIST_NAME));

        when(attributeValueListService.createAttributeValueList(attributeValueListCreateRequest)).thenReturn(resultAttributeValueList);

        // calling the rest method under test
        AttributeValueList resultAttributeValueListRest = attributeValueListRestController.createAttributeValueList(attributeValueListCreateRequest);

        // Validate the returned object.
        verify(attributeValueListService, times(ONE_TIME)).createAttributeValueList(attributeValueListCreateRequest);
        verifyNoMoreInteractions(attributeValueListService);

        assertEquals(resultAttributeValueListRest, resultAttributeValueList);
    }

    @Test
    public void testDeleteAttributeValueList() throws Exception
    {
        Namespace namespace = namespaceService.createNamespace(namespaceServiceTestHelper.createNamespaceCreateRequest(NAMESPACE));

        // Create and persist a attribute value list entity.
        AttributeValueListEntity attributeValueListEntity =
            attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);

        // Validate that this attribute value list exists.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);
        assertNotNull(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey));

        when(attributeValueListService.deleteAttributeValueList(attributeValueListKey)).thenReturn(attributeValueListKey);

        // Delete this attribute value list.
        AttributeValueListKey deletedAttributeValueListKey = attributeValueListRestController.deleteAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Validate the returned object.
        verify(attributeValueListService, times(ONE_TIME)).deleteAttributeValueList(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListService);

        assertEquals(deletedAttributeValueListKey, attributeValueListKey);

        // Ensure that this attribute value list is no longer there.
        assertNotNull(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey));
    }

    @Test
    public void testGetAttributeValueList() throws Exception
    {
        // Create and persist a attribute value list entity.
        attributeValueListDaoTestHelper.createAttributeValueListEntity(namespaceDaoTestHelper.createNamespaceEntity(), ATTRIBUTE_VALUE_LIST_NAME);
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);
        // Create a attribute value list.
        AttributeValueList attributeValueList =
            new AttributeValueList(1, new AttributeValueListKey(null, ATTRIBUTE_VALUE_LIST_NAME));

        when(attributeValueListService.getAttributeValueList(attributeValueListKey)).thenReturn(attributeValueList);


        // Retrieve the attribute value list.
        AttributeValueList resultAttributeValueList = attributeValueListRestController.getAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        verify(attributeValueListService, times(ONE_TIME)).getAttributeValueList(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the returned object.
        assertEquals(attributeValueList, resultAttributeValueList);
    }

    @Test
    public void testGetAttributeValueLists() throws Exception
    {
        // Create and persist attribute value list entities.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);
        AttributeValueListKey attributeValueListKey1 = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        AttributeValueListKeys attributeValueListKeys = new AttributeValueListKeys(Arrays.asList(attributeValueListKey, attributeValueListKey1));

        when(attributeValueListService.getAttributeValueListKeys()).thenReturn(attributeValueListKeys);

        // Retrieve a list of attribute value list keys.
        AttributeValueListKeys resultAttributeValueListKeys = attributeValueListRestController.getAttributeValueLists();

        verify(attributeValueListService, times(ONE_TIME)).getAttributeValueListKeys();
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the returned object.
        assertNotNull(resultAttributeValueListKeys);
        assertEquals(resultAttributeValueListKeys, attributeValueListKeys);
    }

}

