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
package org.finra.herd.service;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.service.impl.IndexSearchServiceImpl;

/**
 * IndexSearchServiceTest
 */
public class IndexSearchServiceTest extends AbstractServiceTest
{
    private static final String INDEX_SEARCH_RESULT_TYPE_BUSINESS_OBJECT_DEFINITION = "businessObjectDefinitionIndex";

    private static final String INDEX_SEARCH_RESULT_TYPE_TAG = "tagIndex";

    private static final int ONE_TIME = 1;

    private static final String SEARCH_TERM = "Search Term";

    private static final int TOTAL_INDEX_SEARCH_RESULTS = 500;

    @InjectMocks
    private IndexSearchServiceImpl indexSearchService;

    @Mock
    private IndexSearchDao indexSearchDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexSearchNoFacets()
    {
        testIndexSearch(null);
    }

    public void testIndexSearch(List<String> facetFields)
    {
        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, facetFields);

        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search results
        final IndexSearchResult indexSearchResultBusinessObjectDefinition =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_BUSINESS_OBJECT_DEFINITION, indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
                BDEF_SHORT_DESCRIPTION);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        List<Facet> facets = new ArrayList<>();
        if (facetFields != null)
        {
            facets.add(new Facet("facet1", new Long(1), "type 1", "id", null));
            facets.add(new Facet("facet2", new Long(2), "type 2", "id2", null));
        }

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, facets);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithFacet()
    {
        List<String> facetFields = Arrays.asList("ResultType", "Tag");
        testIndexSearch(facetFields);
    }

    @Test
    public void testIndexSearchWithInvalidFacets()
    {
        String inCorrectFacetField = "resultTypeee";
        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, Arrays.asList(inCorrectFacetField));
        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields);
            fail("should throw exception.");
        }
        catch (IllegalArgumentException ex)
        {
            String expectedMessage =  String.format("Facet field \"%s\" is not supported.", inCorrectFacetField.toLowerCase());
            assertEquals(ex.getMessage(), expectedMessage);
        }

    }
}
