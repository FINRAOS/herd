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
import org.finra.herd.service.AttributeValueListService;

/**
 * This class tests various functionality within the attribute value list REST controller.
 */
public class AttributeValueListRestControllerTest extends AbstractRestTest
{
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
    public void testCreateAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list.
        AttributeValueList attributeValueList = new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey);

        // Create an attribute value list create request.
        AttributeValueListCreateRequest request = new AttributeValueListCreateRequest(attributeValueListKey);

        // Mock calls to external methods.
        when(attributeValueListService.createAttributeValueList(request)).thenReturn(attributeValueList);

        // Call the method under test.
        AttributeValueList result = attributeValueListRestController.createAttributeValueList(request);

        // Verify the external calls.
        verify(attributeValueListService).createAttributeValueList(request);
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the result.
        assertEquals(attributeValueList, result);
    }

    @Test
    public void testDeleteAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list.
        AttributeValueList attributeValueList = new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey);

        // Mock calls to external methods.
        when(attributeValueListService.deleteAttributeValueList(attributeValueListKey)).thenReturn(attributeValueList);

        // Call the method under test.
        AttributeValueList result = attributeValueListRestController.deleteAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Verify the external calls.
        verify(attributeValueListService).deleteAttributeValueList(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the result.
        assertEquals(attributeValueList, result);
    }

    @Test
    public void testGetAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list.
        AttributeValueList attributeValueList = new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey);

        // Mock calls to external methods.
        when(attributeValueListService.getAttributeValueList(attributeValueListKey)).thenReturn(attributeValueList);

        // Call the method under test.
        AttributeValueList result = attributeValueListRestController.getAttributeValueList(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Verify the external calls.
        verify(attributeValueListService).getAttributeValueList(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the result.
        assertEquals(attributeValueList, result);
    }

    @Test
    public void testGetAttributeValueLists()
    {
        // Create an attribute value list key.
        AttributeValueListKeys attributeValueListKeys =
            new AttributeValueListKeys(Arrays.asList(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)));

        // Mock calls to external methods.
        when(attributeValueListService.getAttributeValueLists()).thenReturn(attributeValueListKeys);

        // Call the method under test.
        AttributeValueListKeys result = attributeValueListRestController.getAttributeValueLists();

        // Verify the external calls.
        verify(attributeValueListService).getAttributeValueLists();
        verifyNoMoreInteractions(attributeValueListService);

        // Validate the result.
        assertEquals(attributeValueListKeys, result);
    }
}
