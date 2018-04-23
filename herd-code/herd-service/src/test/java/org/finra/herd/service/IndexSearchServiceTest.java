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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.impl.IndexSearchServiceImpl;

/**
 * IndexSearchServiceTest
 */
public class IndexSearchServiceTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private IndexSearchDao indexSearchDao;

    @InjectMocks
    private IndexSearchServiceImpl indexSearchService;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Mock
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Mock
    private TagDaoHelper tagDaoHelper;

    @Mock
    private TagHelper tagHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexSearch()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);

        // Create an index search request.
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, ImmutableList.of(new IndexSearchFilter(EXCLUSION_SEARCH_FILTER,
                ImmutableList.of(new IndexSearchKey(tagKey, NO_INDEX_SEARCH_RESULT_TYPE_KEY, NO_INCLUDE_TAG_HIERARCHY))),
            new IndexSearchFilter(EXCLUSION_SEARCH_FILTER,
                ImmutableList.of(new IndexSearchKey(NO_TAG_KEY, new IndexSearchResultTypeKey(INDEX_SEARCH_RESULT_TYPE), NO_INCLUDE_TAG_HIERARCHY)))),
            Collections.singletonList(ElasticsearchHelper.TAG_FACET), ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search results
        final IndexSearchResult indexSearchResultBusinessObjectDefinition = new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name(),
            new SearchIndexKey(BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME), indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
            BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), new SearchIndexKey(TAG_SEARCH_INDEX_NAME), indexSearchResultKeyTag,
                TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Construct a search index entity
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());

        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        // Create a tag entity.
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());
        tagEntity.setTagType(tagTypeEntity);

        // Mock the call to the index search service
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);
        when(alternateKeyHelper.validateStringParameter("An", "index search result type", INDEX_SEARCH_RESULT_TYPE)).thenReturn(INDEX_SEARCH_RESULT_TYPE);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name())).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name())).thenReturn(SEARCH_INDEX_NAME_2);
        when(indexSearchDao.indexSearch(indexSearchRequest, fields, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2)).thenReturn(indexSearchResponse);

        // Call the method under test.
        IndexSearchResponse result = indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);

        // Verify the external calls.
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(tagKey);
        verify(alternateKeyHelper).validateStringParameter("An", "index search result type", INDEX_SEARCH_RESULT_TYPE);
        verify(searchIndexTypeDaoHelper).getSearchIndexTypeEntity(INDEX_SEARCH_RESULT_TYPE);
        verify(indexSearchDao).indexSearch(indexSearchRequest, fields, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2);
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(indexSearchResponse, result);
    }

    @Test
    public void testIndexSearchWithMatch()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);

        // Create an index search request.
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, ImmutableList.of(new IndexSearchFilter(EXCLUSION_SEARCH_FILTER,
                ImmutableList.of(new IndexSearchKey(tagKey, NO_INDEX_SEARCH_RESULT_TYPE_KEY, NO_INCLUDE_TAG_HIERARCHY))),
            new IndexSearchFilter(EXCLUSION_SEARCH_FILTER,
                ImmutableList.of(new IndexSearchKey(NO_TAG_KEY, new IndexSearchResultTypeKey(INDEX_SEARCH_RESULT_TYPE), NO_INCLUDE_TAG_HIERARCHY)))),
            Collections.singletonList(ElasticsearchHelper.TAG_FACET), ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a set of match fields.
        final Set<String> match = Sets.newHashSet(MATCH_COLUMN);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search results
        final IndexSearchResult indexSearchResultBusinessObjectDefinition = new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name(),
            new SearchIndexKey(BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME), indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
            BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), new SearchIndexKey(TAG_SEARCH_INDEX_NAME), indexSearchResultKeyTag,
                TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Construct a search index entity
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());

        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        // Create a tag entity.
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());
        tagEntity.setTagType(tagTypeEntity);

        // Mock the call to the index search service
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);
        when(alternateKeyHelper.validateStringParameter("An", "index search result type", INDEX_SEARCH_RESULT_TYPE)).thenReturn(INDEX_SEARCH_RESULT_TYPE);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name())).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name())).thenReturn(SEARCH_INDEX_NAME_2);
        when(indexSearchDao.indexSearch(indexSearchRequest, fields, match, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2)).thenReturn(indexSearchResponse);

        // Call the method under test.
        IndexSearchResponse result = indexSearchService.indexSearch(indexSearchRequest, fields, match);

        // Verify the external calls.
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(tagKey);
        verify(alternateKeyHelper).validateStringParameter("An", "index search result type", INDEX_SEARCH_RESULT_TYPE);
        verify(searchIndexTypeDaoHelper).getSearchIndexTypeEntity(INDEX_SEARCH_RESULT_TYPE);
        verify(indexSearchDao).indexSearch(indexSearchRequest, fields, match, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2);
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(indexSearchResponse, result);
    }

    @Test
    public void testIndexSearchEmptyIndexSearchFilterList()
    {
        // Create index search request with an empty list of index search filters.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, new ArrayList<>(), NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one index search filter must be specified.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchInvalidFacet()
    {
        // Create an index search request with an invalid facet.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, NO_INDEX_SEARCH_FILTERS, Collections.singletonList(INVALID_VALUE), NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Facet field \"%s\" is not supported.", INVALID_VALUE.toLowerCase()), e.getMessage());
        }
    }

    @Test
    public void testIndexSearchInvalidIndexSearchFilter()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);

        // Create an index search filter that contains index search keys for tag and result type.
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, ImmutableList
            .of(new IndexSearchKey(tagKey, NO_INDEX_SEARCH_RESULT_TYPE_KEY, NO_INCLUDE_TAG_HIERARCHY),
                new IndexSearchKey(NO_TAG_KEY, new IndexSearchResultTypeKey(INDEX_SEARCH_RESULT_TYPE), NO_INCLUDE_TAG_HIERARCHY)));

        // Create an index search request.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, Collections.singletonList(indexSearchFilter), NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagTypeCode());

        // Create a tag entity.
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());
        tagEntity.setTagType(tagTypeEntity);

        // Mock the external calls.
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Index search keys should be a homogeneous list of either index search result type keys or tag keys.", e.getMessage());
        }

        // Verify the external calls.
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(tagKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchInvalidIndexSearchKey()
    {
        // Create an index search key that contains both tag and result type keys.
        final IndexSearchKey indexSearchKey =
            new IndexSearchKey(new TagKey(TAG_TYPE_CODE, TAG_CODE), new IndexSearchResultTypeKey(INDEX_SEARCH_RESULT_TYPE), NO_INCLUDE_TAG_HIERARCHY);

        // Create an index search filter.
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(NO_EXCLUSION_SEARCH_FILTER, Collections.singletonList(indexSearchKey));

        // Create an index search request.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, Collections.singletonList(indexSearchFilter), NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one instance of index search result type key or tag key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchInvalidSearchTerm()
    {
        // Create an index search request with an invalid search term.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(EMPTY_STRING, NO_INDEX_SEARCH_FILTERS, NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The search term length must be at least %d characters.", IndexSearchServiceImpl.SEARCH_TERM_MINIMUM_ALLOWABLE_LENGTH),
                e.getMessage());
        }
    }

    @Test
    public void testIndexSearchMissingOptionalParameters()
    {
        // Create an index search request.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(SEARCH_TERM, NO_INDEX_SEARCH_FILTERS, NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search results
        final IndexSearchResult indexSearchResultBusinessObjectDefinition = new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name(),
            new SearchIndexKey(BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME), indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
            BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), new SearchIndexKey(TAG_SEARCH_INDEX_NAME), indexSearchResultKeyTag,
                TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Construct a search index entity
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());

        // Mock the call to the index search service
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name())).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name())).thenReturn(SEARCH_INDEX_NAME_2);
        when(indexSearchDao.indexSearch(indexSearchRequest, NO_FIELDS, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2)).thenReturn(indexSearchResponse);

        // Call the method under test.
        IndexSearchResponse result = indexSearchService.indexSearch(indexSearchRequest, NO_FIELDS, NO_MATCH);

        // Verify the external calls.
        verify(indexSearchDao).indexSearch(indexSearchRequest, NO_FIELDS, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2);
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(indexSearchResponse, result);
    }

    @Test
    public void testIndexSearchNoSearchTermAndNoSearchFilter()
    {
        // Create an index search request without a search term and without a search filter.
        final IndexSearchRequest indexSearchRequest =
            new IndexSearchRequest(null, NO_INDEX_SEARCH_FILTERS, NO_INDEX_SEARCH_FACET_FIELDS, NO_ENABLE_HIT_HIGHLIGHTING);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Try to call the method under test.
        try
        {
            indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A search term or a search filter must be specified.", e.getMessage());
        }
    }

    @Test
    public void testIndexSearchWithResultTypeFilter()
    {
        // Create an index search key
        final IndexSearchKey indexSearchKey = new IndexSearchKey();

        // Create a tag key
        final IndexSearchResultTypeKey resultTypeKey = new IndexSearchResultTypeKey(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        indexSearchKey.setIndexSearchResultTypeKey(resultTypeKey);

        // Create an index search keys list and add the previously defined key to it
        final List<IndexSearchKey> indexSearchKeys = Collections.singletonList(indexSearchKey);

        // Create an index search filter with the keys previously defined
        final IndexSearchFilter indexSearchFilter = new IndexSearchFilter(EXCLUSION_SEARCH_FILTER, indexSearchKeys);

        List<IndexSearchFilter> indexSearchFilters = Collections.singletonList(indexSearchFilter);

        // Create index search request
        final IndexSearchRequest indexSearchRequest = new IndexSearchRequest(SEARCH_TERM, indexSearchFilters, null, false);

        // Create a set of fields.
        final Set<String> fields = Sets.newHashSet(FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyBusinessObjectDefinition =
            new IndexSearchResultKey(null, new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Create a new index search result key and populate it with a tag key
        final IndexSearchResultKey indexSearchResultKeyTag = new IndexSearchResultKey(new TagKey(TAG_TYPE, TAG_CODE), null);

        // Create a new index search results
        final IndexSearchResult indexSearchResultBusinessObjectDefinition = new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name(),
            new SearchIndexKey(BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME), indexSearchResultKeyBusinessObjectDefinition, BDEF_DISPLAY_NAME,
            BDEF_SHORT_DESCRIPTION, null);
        final IndexSearchResult indexSearchResultTag =
            new IndexSearchResult(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), new SearchIndexKey(TAG_SEARCH_INDEX_NAME), indexSearchResultKeyTag,
                TAG_DISPLAY_NAME, TAG_DESCRIPTION, null);

        // Create a list to contain the index search results
        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();
        indexSearchResults.add(indexSearchResultBusinessObjectDefinition);
        indexSearchResults.add(indexSearchResultTag);

        // Construct an index search response
        final IndexSearchResponse indexSearchResponse = new IndexSearchResponse(TOTAL_INDEX_SEARCH_RESULTS, indexSearchResults, null);

        // Mock the call to the index search service
        when(alternateKeyHelper.validateStringParameter("An", "index search result type", SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name()))
            .thenReturn(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name())).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name())).thenReturn(SEARCH_INDEX_NAME_2);
        when(indexSearchDao.indexSearch(indexSearchRequest, fields, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2)).thenReturn(indexSearchResponse);

        // Call the method under test.
        IndexSearchResponse result = indexSearchService.indexSearch(indexSearchRequest, fields, NO_MATCH);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("An", "index search result type", SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexTypeDaoHelper).getSearchIndexTypeEntity(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        verify(searchIndexDaoHelper).getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());
        verify(indexSearchDao).indexSearch(indexSearchRequest, fields, NO_MATCH, SEARCH_INDEX_NAME, SEARCH_INDEX_NAME_2);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(indexSearchResponse, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, indexSearchDao, searchIndexDaoHelper, searchIndexTypeDaoHelper, tagDaoHelper, tagHelper);
    }
}
