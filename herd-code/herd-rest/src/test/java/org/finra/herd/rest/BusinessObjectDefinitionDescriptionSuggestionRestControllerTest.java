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

import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.CREATED_BY_USER_ID_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.DESCRIPTION_SUGGESTION_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.STATUS_FIELD;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
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

        // Create the business object definition description suggestion create request.
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(key, DESCRIPTION_SUGGESTION);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

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

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

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
        // Create the business object definition key used to get the business object definition description suggestions
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Create business object definition description suggestion keys.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionKey key2 = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID_2);

        // Create a list of business object definition description suggestion keys.
        BusinessObjectDefinitionDescriptionSuggestionKeys businessObjectDefinitionDescriptionSuggestionKeys =
            new BusinessObjectDefinitionDescriptionSuggestionKeys(Lists.newArrayList(key, key2));

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestions(businessObjectDefinitionKey))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKeys);

        // Get all business object definition description suggestions.
        BusinessObjectDefinitionDescriptionSuggestionKeys resultBusinessObjectDefinitionDescriptionSuggestionKeys =
            businessObjectDefinitionDescriptionSuggestionRestController.getBusinessObjectDefinitionDescriptionSuggestions(NAMESPACE, BDEF_NAME);

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).getBusinessObjectDefinitionDescriptionSuggestions(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestionKeys, resultBusinessObjectDefinitionDescriptionSuggestionKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByKey()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

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
    public void testSearchBusinessObjectDefinitionDescriptionSuggestions()
    {
        // Create the business object definition description suggestion search request.
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey =
            new BusinessObjectDefinitionDescriptionSuggestionSearchKey(NAMESPACE, BDEF_NAME, BDEF_DESCRIPTION_SUGGESTION_STATUS);
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchKey));
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter));

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

        // Create the business object definition description suggestion search response.
        BusinessObjectDefinitionDescriptionSuggestionSearchResponse businessObjectDefinitionDescriptionSuggestionSearchResponse =
            new BusinessObjectDefinitionDescriptionSuggestionSearchResponse(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestion));

        // Build the fields set
        Set<String> fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);
        fields.add(DESCRIPTION_SUGGESTION_FIELD);
        fields.add(STATUS_FIELD);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, fields))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionSearchResponse);

        // Search business object definition description suggestions.
        BusinessObjectDefinitionDescriptionSuggestionSearchResponse response =
            businessObjectDefinitionDescriptionSuggestionRestController.searchBusinessObjectDefinitionDescriptionSuggestions(fields, request);

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).searchBusinessObjectDefinitionDescriptionSuggestions(request, fields);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestionSearchResponse, response);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create the business object definition description suggestion update request.
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(DESCRIPTION_SUGGESTION);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

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

    @Test
    public void testAcceptBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey key = new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create the business object definition description suggestion acceptance request.
        BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request = new BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest(key);

        // Create the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, key, DESCRIPTION_SUGGESTION, BDEF_DESCRIPTION_SUGGESTION_STATUS, USER_ID, CREATED_ON);

        // Mock calls to external method.
        when(businessObjectDefinitionDescriptionSuggestionService.acceptBusinessObjectDefinitionDescriptionSuggestion(request))
            .thenReturn(businessObjectDefinitionDescriptionSuggestion);

        // Call business object definition description suggestion acceptance.
        BusinessObjectDefinitionDescriptionSuggestion response =
            businessObjectDefinitionDescriptionSuggestionRestController.acceptBusinessObjectDefinitionDescriptionSuggestion(request);

        // Verify the external calls.
        verify(businessObjectDefinitionDescriptionSuggestionService).acceptBusinessObjectDefinitionDescriptionSuggestion(request);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionService);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, response);
    }
}
