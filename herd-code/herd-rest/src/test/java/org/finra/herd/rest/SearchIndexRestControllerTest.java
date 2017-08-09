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

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;
import org.finra.herd.service.SearchIndexService;

/**
 * This class tests search index functionality within the search index REST controller.
 */
public class SearchIndexRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private SearchIndexRestController searchIndexRestController;

    @Mock
    private SearchIndexService searchIndexService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSearchIndex()
    {
        // Create a search index create request.
        SearchIndexCreateRequest searchIndexCreateRequest = new SearchIndexCreateRequest(SEARCH_INDEX_TYPE);

        // Create a search index create response.
        SearchIndex searchIndex =
            new SearchIndex(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG,
                NO_SEARCH_INDEX_STATISTICS, USER_ID, CREATED_ON,
                UPDATED_ON);

        // Mock the call to the search index service.
        when(searchIndexService.createSearchIndex(searchIndexCreateRequest)).thenReturn(searchIndex);

        // Create a search index.
        SearchIndex response = searchIndexRestController.createSearchIndex(searchIndexCreateRequest);

        // Verify the calls.
        verify(searchIndexService, times(1)).createSearchIndex(searchIndexCreateRequest);

        // Validate the returned object.
        assertEquals(searchIndex, response);
    }

    @Test
    public void testDeleteSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a search index delete response.
        SearchIndex searchIndex =
            new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG, NO_SEARCH_INDEX_STATISTICS, USER_ID,
                CREATED_ON, UPDATED_ON);

        // Mock the call to the search index service.
        when(searchIndexService.deleteSearchIndex(searchIndexKey)).thenReturn(searchIndex);

        // Delete a search index.
        SearchIndex response = searchIndexRestController.deleteSearchIndex(SEARCH_INDEX_NAME);

        // Verify the calls.
        verify(searchIndexService, times(1)).deleteSearchIndex(new SearchIndexKey(SEARCH_INDEX_NAME));

        // Validate the returned object.
        assertEquals(searchIndex, response);
    }

    @Test
    public void testGetSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a search index get response.
        SearchIndex searchIndex =
            new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG, NO_SEARCH_INDEX_STATISTICS, USER_ID,
                CREATED_ON, UPDATED_ON);

        // Mock the call to the search index service.
        when(searchIndexService.getSearchIndex(searchIndexKey)).thenReturn(searchIndex);

        // Get a search index.
        SearchIndex response = searchIndexRestController.getSearchIndex(SEARCH_INDEX_NAME);

        // Verify the calls.
        verify(searchIndexService, times(1)).getSearchIndex(new SearchIndexKey(SEARCH_INDEX_NAME));

        // Validate the returned object.
        assertEquals(searchIndex, response);
    }

    @Test
    public void testGetSearchIndexes()
    {
        // Create a get search indexes response.
        SearchIndexKeys searchIndexKeys = new SearchIndexKeys(Arrays.asList(new SearchIndexKey(SEARCH_INDEX_NAME), new SearchIndexKey(SEARCH_INDEX_NAME_2)));

        // Mock the call to the search index service.
        when(searchIndexService.getSearchIndexes()).thenReturn(searchIndexKeys);

        // Get search indexes.
        SearchIndexKeys response = searchIndexRestController.getSearchIndexes();

        // Verify the calls.
        verify(searchIndexService, times(1)).getSearchIndexes();

        // Validate the returned object.
        assertEquals(searchIndexKeys, response);
    }
}
