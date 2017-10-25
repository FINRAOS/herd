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

import java.util.ArrayList;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.service.IndexSearchService;

/**
 * This class tests search index functionality within the index search REST controller.
 */
public class IndexSearchRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private IndexSearchRestController indexSearchRestController;

    @Mock
    private IndexSearchService indexSearchService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexSearch()
    {
        // Create an index search request.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, NO_INDEX_SEARCH_FILTERS, NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create an index search response.
        IndexSearchResponse indexSearchResponse = new IndexSearchResponse(LONG_VALUE, new ArrayList<>(), new ArrayList<>());

        // Create a set of search optional fields.
        Set<String> fields = Sets.newHashSet(FIELD_SHORT_DESCRIPTION);

        // Mock the external calls.
        when(indexSearchService.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test.
        IndexSearchResponse result = indexSearchRestController.indexSearch(fields, indexSearchRequest);

        // Verify the external calls.
        verify(indexSearchService).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchService);

        // Validate the result.
        assertEquals(indexSearchResponse, result);
    }
}
