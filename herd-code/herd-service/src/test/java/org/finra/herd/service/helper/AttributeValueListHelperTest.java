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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.service.AbstractServiceTest;

public class AttributeValueListHelperTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private AttributeValueListHelper attributeValueListHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createValidateAttributeValueListCreateRequest()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list create request.
        AttributeValueListCreateRequest request = new AttributeValueListCreateRequest(attributeValueListKey);

        // Mock calls to external methods.
        when(alternateKeyHelper.validateStringParameter("namespace", ATTRIBUTE_VALUE_LIST_NAMESPACE)).thenReturn(ATTRIBUTE_VALUE_LIST_NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("An", "attribute value list name", ATTRIBUTE_VALUE_LIST_NAME)).thenReturn(ATTRIBUTE_VALUE_LIST_NAME);

        // Call the method under test.
        attributeValueListHelper.validateAttributeValueListCreateRequest(request);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", ATTRIBUTE_VALUE_LIST_NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("An", "attribute value list name", ATTRIBUTE_VALUE_LIST_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);

        // Validate the result.
        assertEquals(new AttributeValueListCreateRequest(attributeValueListKey), request);
    }

    @Test
    public void createValidateAttributeValueListCreateRequestMissingRequiredParameters()
    {
        // Try to call the method under test.
        try
        {
            attributeValueListHelper.validateAttributeValueListCreateRequest(null);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute value list create request must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void createValidateAttributeValueListKey()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Mock calls to external methods.
        when(alternateKeyHelper.validateStringParameter("namespace", ATTRIBUTE_VALUE_LIST_NAMESPACE)).thenReturn(ATTRIBUTE_VALUE_LIST_NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("An", "attribute value list name", ATTRIBUTE_VALUE_LIST_NAME)).thenReturn(ATTRIBUTE_VALUE_LIST_NAME);

        // Call the method under test.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", ATTRIBUTE_VALUE_LIST_NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("An", "attribute value list name", ATTRIBUTE_VALUE_LIST_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);

        // Validate the result.
        assertEquals(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME), attributeValueListKey);
    }

    @Test
    public void createValidateAttributeValueListKeyMissingRequiredParameters()
    {
        // Try to call the method under test.
        try
        {
            attributeValueListHelper.validateAttributeValueListKey(null);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute value list key must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
