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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexValidation;
import org.finra.herd.model.api.xml.SearchIndexValidationCreateRequest;
import org.finra.herd.service.SearchIndexValidationService;

/**
 * This class tests search index validation functionality within the search index validation REST controller.
 */
public class SearchIndexValidationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private SearchIndexValidationRestController searchIndexValidationRestController;

    @Mock
    private SearchIndexValidationService searchIndexValidationService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    private void searchIndexValidation(SearchIndexValidationCreateRequest searchIndexValidationCreateRequest)
    {
        // Create a search index validation response.
        SearchIndexValidation searchIndexValidation =
            new SearchIndexValidation(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_STATISTICS_CREATION_DATE, false, false, false);

        // Mock the call to the search index validation service.
        when(searchIndexValidationService.createSearchIndexValidation(searchIndexValidationCreateRequest)).thenReturn(searchIndexValidation);

        // Create a search index validation.
        SearchIndexValidation response = searchIndexValidationRestController.createSearchIndexValidation(searchIndexValidationCreateRequest);

        // Verify the calls.
        verify(searchIndexValidationService, times(1)).createSearchIndexValidation(searchIndexValidationCreateRequest);

        response.setValidateStartTime(SEARCH_INDEX_STATISTICS_CREATION_DATE);
        // Validate the returned object.
        assertEquals(searchIndexValidation, response);
    }

    @Test
    public void testCreateSearchIndexValidationFull()
    {
        // Create a search index validation create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_VALIDATION_ENABLED);

        searchIndexValidation(searchIndexValidationCreateRequest);
    }

    @Test
    public void testCreateSearchIndexValidation()
    {
        // Create a search index validation create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_VALIDATION_DISABLED);

        searchIndexValidation(searchIndexValidationCreateRequest);
    }
}
