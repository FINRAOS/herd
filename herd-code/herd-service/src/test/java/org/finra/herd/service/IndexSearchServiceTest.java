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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.helper.IndexSearchResultTypeHelper;
import org.finra.herd.service.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.impl.IndexSearchServiceImpl;

/**
 * IndexSearchServiceTest
 */
public class IndexSearchServiceTest extends AbstractServiceTest
{
    private static final String INDEX_SEARCH_RESULT_TYPE_BUSINESS_OBJECT_DEFINITION = "businessObjectDefinitionIndex";

    private static final String INDEX_SEARCH_RESULT_TYPE_TAG = "tagIndex";

    private static final int ONE_TIME = 1;

    private static final int TWO_TIMES = 2;

    private static final String SEARCH_TERM = "Search Term";

    private static final int TOTAL_INDEX_SEARCH_RESULTS = 500;

    @InjectMocks
    private IndexSearchServiceImpl indexSearchService;

    @Mock
    private IndexSearchDao indexSearchDao;

    @Mock
    private TagHelper tagHelper;

    @Mock
    private IndexSearchResultTypeHelper indexSearchResultTypeHelper;

    @Mock
    private TagDaoHelper tagDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexSearch()
    {
        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, null, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

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
    public void testIndexSearchWithTagFilter()
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

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Create tag entity for the mock to return
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        tagEntity.setTagType(tagTypeEntity);

        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to tagHelper
        verify(tagHelper, times(ONE_TIME)).validateTagKey(tagKey);
        verifyNoMoreInteractions(tagHelper);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithTagFilterAndExcludeFlagSet()
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

        // Set exclude filter flag to true
        indexSearchFilter.setIsExclusionSearchFilter(true);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Create tag entity for the mock to return
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        tagEntity.setTagType(tagTypeEntity);

        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to tagHelper
        verify(tagHelper, times(ONE_TIME)).validateTagKey(tagKey);
        verifyNoMoreInteractions(tagHelper);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithResultTypeFilter()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final IndexSearchResultTypeKey resultTypeKey = new IndexSearchResultTypeKey(BUSINESS_OBJECT_DEFINITION_INDEX);
        indexSearchKey.setIndexSearchResultTypeKey(resultTypeKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to index search result type helper
        verify(indexSearchResultTypeHelper, times(ONE_TIME)).validateIndexSearchResultTypeKey(resultTypeKey);
        verifyNoMoreInteractions(indexSearchResultTypeHelper);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithResultTypeFilterAndExcludeFilterSet()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final IndexSearchResultTypeKey resultTypeKey = new IndexSearchResultTypeKey(BUSINESS_OBJECT_DEFINITION_INDEX);
        indexSearchKey.setIndexSearchResultTypeKey(resultTypeKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        // Set exclude filter flag to true
        indexSearchFilter.setIsExclusionSearchFilter(true);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to index search result type helper
        verify(indexSearchResultTypeHelper, times(ONE_TIME)).validateIndexSearchResultTypeKey(resultTypeKey);
        verifyNoMoreInteractions(indexSearchResultTypeHelper);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithTagFacet()
    {
        // Create a list of requested facets
        final List<String> facetsRequested = Collections.singletonList(ElasticsearchHelper.TAG_FACET);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, null, facetsRequested, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Create expected facet result
        final Facet tagFacet = new Facet();
        final List<Facet> facets = Collections.singletonList(tagFacet);

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

        // Validate that facets object was part of the response
        assertThat("Facets response was null", indexSearchResponseFromService.getFacets(), not(nullValue()));

    }

    @Test
    public void testIndexSearchWithTagAndResultTypeInOneSearchKeyFilter()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);
        indexSearchKey.setTagKey(tagKey);

        final IndexSearchResultTypeKey resultTypeKey = new IndexSearchResultTypeKey(BUSINESS_OBJECT_DEFINITION_INDEX);
        indexSearchKey.setIndexSearchResultTypeKey(resultTypeKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        try
        {
            // Call the method under test
            IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);
            fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("Exactly one instance of index search result type key or tag key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchWithTwoTagKeysInSameFilter()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKeyOne = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);
        indexSearchKeyOne.setTagKey(tagKey);

        // Create another index search key
        final IndexSearchKey indexSearchKeyTwo = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKeyTwo = new TagKey(TAG_TYPE_CODE, TAG_CODE_2);
        indexSearchKeyTwo.setTagKey(tagKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = ImmutableList.of(indexSearchKeyOne, indexSearchKeyTwo);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Create tag entity for the mock to return
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        tagEntity.setTagType(tagTypeEntity);

        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequest, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequest, fields);

        // Verify the method call to tagHelper
        verify(tagHelper, times(TWO_TIMES)).validateTagKey(any());
        verifyNoMoreInteractions(tagHelper);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequest, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }

    @Test
    public void testIndexSearchWithTagAndResultTypeInOneSearchFilter()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);
        indexSearchKey.setTagKey(tagKey);

        // Create another index search key
        final IndexSearchKey indexSearchKeyTwo = new IndexSearchKey();

        final IndexSearchResultTypeKey resultTypeKey = new IndexSearchResultTypeKey(BUSINESS_OBJECT_DEFINITION_INDEX);
        indexSearchKeyTwo.setIndexSearchResultTypeKey(resultTypeKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = ImmutableList.of(indexSearchKey, indexSearchKeyTwo);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create tag entity for the mock to return
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        tagEntity.setTagType(tagTypeEntity);

        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        try
        {
            // Call the method under test
            indexSearchService.indexSearch(indexSearchRequest, fields);
            fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("Index search keys should be a homogeneous list of either index search result type keys or tag keys.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchWithEmptyFilters()
    {
        final List<IndexSearchFilter> emptyFilters = new ArrayList<>();
        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, emptyFilters, null, false);

        // Create a new fields set that will be used when testing the index search method
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        try
        {
            // Call the method under test
            indexSearchService.indexSearch(indexSearchRequest, fields);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("At least one index search filter must be specified.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchWithHitHighlighting()
    {
        // Create index search request with hit highlighting enabled
        final IndexSearchRequest indexSearchRequestHighlightingEnabled = new IndexSearchRequest(SEARCH_TERM, null, null, HIT_HIGHLIGHTING_ENABLED);

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
                BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(INDEX_SEARCH_RESULT_TYPE_TAG, indexSearchResultKeyTag, TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Mock the call to the index search service
        when(indexSearchDao.indexSearch(indexSearchRequestHighlightingEnabled, fields)).thenReturn(indexSearchResponse);

        // Call the method under test
        IndexSearchResponse indexSearchResponseFromService = indexSearchService.indexSearch(indexSearchRequestHighlightingEnabled, fields);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequestHighlightingEnabled, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));

        // Create index search request with highlighting disabled
        final IndexSearchRequest indexSearchRequestHighlightingDisabled = new IndexSearchRequest(SEARCH_TERM, null, null, HIT_HIGHLIGHTING_DISABLED);

        when(indexSearchDao.indexSearch(indexSearchRequestHighlightingDisabled, fields)).thenReturn(indexSearchResponse);

        // Verify the method call to indexSearchService.indexSearch()
        verify(indexSearchDao, times(ONE_TIME)).indexSearch(indexSearchRequestHighlightingEnabled, fields);
        verifyNoMoreInteractions(indexSearchDao);

        // Validate the returned object.
        assertThat("Index search response was null.", indexSearchResponseFromService, not(nullValue()));
        assertThat("Index search response was not correct.", indexSearchResponseFromService, is(indexSearchResponse));
        assertThat("Index search response was not an instance of IndexSearchResponse.class.", indexSearchResponse, instanceOf(IndexSearchResponse.class));
    }


}
