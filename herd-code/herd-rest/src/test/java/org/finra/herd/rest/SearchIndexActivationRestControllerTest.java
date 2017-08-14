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

import org.finra.herd.model.api.xml.SearchIndexActivation;
import org.finra.herd.model.api.xml.SearchIndexActivationCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.service.SearchIndexActivationService;

/**
 * This class tests search index activation functionality within the search index activation REST controller.
 */
public class SearchIndexActivationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private SearchIndexActivationRestController searchIndexActivationRestController;

    @Mock
    private SearchIndexActivationService searchIndexActivationService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSearchIndexActivation()
    {
        // Create a search index activation create request.
        SearchIndexActivationCreateRequest searchIndexActivationCreateRequest = new SearchIndexActivationCreateRequest(new SearchIndexKey(SEARCH_INDEX_NAME));

        // Create a search index activation response.
        SearchIndexActivation searchIndexActivation =
            new SearchIndexActivation(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, SEARCH_INDEX_ACTIVE_FLAG, CREATED_BY,
                CREATED_ON, UPDATED_ON);

        // Mock the call to the search index activation service.
        when(searchIndexActivationService.createSearchIndexActivation(searchIndexActivationCreateRequest)).thenReturn(searchIndexActivation);

        // Create a search index activation.
        SearchIndexActivation response = searchIndexActivationRestController.createSearchIndexActivation(searchIndexActivationCreateRequest);

        // Verify the calls.
        verify(searchIndexActivationService, times(1)).createSearchIndexActivation(searchIndexActivationCreateRequest);

        // Validate the returned object.
        assertEquals(searchIndexActivation, response);
    }
}
