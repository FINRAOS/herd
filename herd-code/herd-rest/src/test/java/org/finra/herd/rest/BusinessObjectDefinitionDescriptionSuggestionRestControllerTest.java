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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.DescriptionSuggestion;
import org.finra.herd.service.BusinessObjectDefinitionDescriptionSuggestionService;

/**
 * This class tests various functionality within the business object definition description suggestion REST controller.
 */
public class BusinessObjectDefinitionDescriptionSuggestionRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionDescriptionSuggestionRestController businessObjectDefinitionDescriptionSuggestionRestController;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionService businessObjectDefinitionDescriptionSuggestionService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a new description suggestion.
        DescriptionSuggestion descriptionSuggestion = new DescriptionSuggestion(BDEF_DESCRIPTION);

        // Create the business object definition description suggestion create request.
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(key, descriptionSuggestion);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, descriptionSuggestion);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request))
            .thenReturn(businessObjectDefinitionDescriptionSuggestion);

        // Create a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion response =
            businessObjectDefinitionDescriptionSuggestionRestController.createBusinessObjectDefinitionDescriptionSuggestion(request);

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).createBusinessObjectDefinitionDescriptionSuggestion(request);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, response);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a new description suggestion.
        DescriptionSuggestion descriptionSuggestion = new DescriptionSuggestion(BDEF_DESCRIPTION);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, descriptionSuggestion);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.deleteBusinessObjectDefinitionDescriptionSuggestion(key))
            .thenReturn(businessObjectDefinitionDescriptionSuggestion);

        // Delete this business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion deletedBusinessObjectDefinitionDescriptionSuggestion =
            businessObjectDefinitionDescriptionSuggestionRestController
                .deleteBusinessObjectDefinitionDescriptionSuggestion(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getUserId());

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).deleteBusinessObjectDefinitionDescriptionSuggestion(key);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, deletedBusinessObjectDefinitionDescriptionSuggestion);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestions()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a list of business object definition description suggestion keys.
        BusinessObjectDefinitionDescriptionSuggestionKeys businessObjectDefinitionDescriptionSuggestionKeys =
            new BusinessObjectDefinitionDescriptionSuggestionKeys(Lists.newArrayList(key));

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestions())
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKeys);

        // Get all business object definition description suggestions.
        BusinessObjectDefinitionDescriptionSuggestionKeys resultBusinessObjectDefinitionDescriptionSuggestionKeys =
            businessObjectDefinitionDescriptionSuggestionRestController.getBusinessObjectDefinitionDescriptionSuggestions();

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).getBusinessObjectDefinitionDescriptionSuggestions();
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestionKeys, resultBusinessObjectDefinitionDescriptionSuggestionKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByKey()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a new description suggestion.
        DescriptionSuggestion descriptionSuggestion = new DescriptionSuggestion(BDEF_DESCRIPTION);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, descriptionSuggestion);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestionByKey(key))
            .thenReturn(businessObjectDefinitionDescriptionSuggestion);

        // Get the business object definition description suggestion for the specified key.
        BusinessObjectDefinitionDescriptionSuggestion resultBusinessObjectDefinitionDescriptionSuggestion =
            businessObjectDefinitionDescriptionSuggestionRestController
                .getBusinessObjectDefinitionDescriptionSuggestionByKey(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getUserId());

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).getBusinessObjectDefinitionDescriptionSuggestionByKey(key);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(resultBusinessObjectDefinitionDescriptionSuggestion, businessObjectDefinitionDescriptionSuggestion);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a new description suggestion.
        DescriptionSuggestion descriptionSuggestion = new DescriptionSuggestion(BDEF_DESCRIPTION);

        // Create the business object definition description suggestion update request.
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(descriptionSuggestion);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, descriptionSuggestion);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.updateBusinessObjectDefinitionDescriptionSuggestion(key, request))
            .thenReturn(businessObjectDefinitionDescriptionSuggestion);

        // Update a business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion response = businessObjectDefinitionDescriptionSuggestionRestController
            .updateBusinessObjectDefinitionDescriptionSuggestion(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getUserId(), request);

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).updateBusinessObjectDefinitionDescriptionSuggestion(key, request);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, response);
    }
}
