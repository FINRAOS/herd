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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.service.IndexSearchService;

/**
 * This class tests search index functionality within the index search REST controller.
 */
public class IndexSearchRestControllerTest extends AbstractRestTest
{
    private static final String INDEX_SEARCH_RESULT_TYPE_BUSINESS_OBJECT_DEFINITION = "businessObjectDefinitionIndex";

    private static final String INDEX_SEARCH_RESULT_TYPE_TAG = "tagIndex";

    private static final List<IndexSearchFilter> NO_SEARCH_FILTERS = null;

    private static final int ONE_TIME = 1;

    private static final String SEARCH_TERM = "Search Term";

    private static final int TOTAL_INDEX_SEARCH_RESULTS = 500;

    @InjectMocks
    private IndexSearchRestController indexSearchRestController;

    @Mock
    private IndexSearchService indexSearchService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    public void testIndexSearch(List<IndexSearchFilter> indexSearchFilters, List<String> facetFields)
    {
        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, facetFields, false);

        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search results
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultBusinessObjectDefinition =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_BUSINESS_OBJECT_DEFINITION, indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
                BDEF_SHORT_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultTag);
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);

        List<Facet> facets = new ArrayList<>();
        if (facetFields != null)
        {
            facets.add(new Facet("facet1", new Long(1), "type 1", "id", null));
            facets.add(new Facet("facet2", new Long(2), "type 2", "id2", null));
        }

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Mock the call to the index search service
        //Mockito.doReturn(indexSearchResponse).when(indexSearchService.indexSearch(indexSearchRequest, fields));

        when(indexSearchService.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromRestCall = indexSearchRestController.indexSearch(fields, indexSearchRequest);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchService, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchService);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromRestCall, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromRestCall, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchNoFacets()
    {
        testIndexSearch(NO_SEARCH_FILTERS, null);
    }

    @Test
    public void testIndexSearchWithFacets()
    {
        List<String> facetFields = Arrays.asList("ResultType", "Tag");
        testIndexSearch(NO_SEARCH_FILTERS, facetFields);
    }

    @Test
    public void testIndexSearchWithFilters()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);
        indexSearchKey.setTagKey(tagKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        List<String> facetFields = Arrays.asList("ResultType", "Tag");

        testIndexSearch(indexSearchFilters, facetFields);
    }

    @Test
    public void testIndexSearchWithFiltersAndExcludeFlag()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);
        indexSearchKey.setTagKey(tagKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        // Set exclude flag to true
        indexSearchFilter.setIsExclusionSearchFilter(true);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        List<String> facetFields = Arrays.asList("ResultType", "Tag");

        testIndexSearch(indexSearchFilters, facetFields);
    }

}
