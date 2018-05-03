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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.FacetFieldValidationService;
import org.finra.herd.service.IndexSearchService;
import org.finra.herd.service.SearchableService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;

/**
 * IndexSearchServiceImpl is the implementation of the IndexSearchService and includes an indexSearch method that will handle search requests against a search
 * index.
 */
@Service
public class IndexSearchServiceImpl implements IndexSearchService, SearchableService, FacetFieldValidationService
{
    /**
     * Constant to hold the display name option for the index search
     */
    public static final String DISPLAY_NAME_FIELD = "displayname";

    /**
     * Constant to hold the column match field option for the index search.
     */
    public static final String MATCH_COLUMN_FIELD = "column";

    /**
     * The minimum allowable length of a search term.
     */
    public static final int SEARCH_TERM_MINIMUM_ALLOWABLE_LENGTH = 3;

    /**
     * Constant to hold the short description option for the index search.
     */
    public static final String SHORT_DESCRIPTION_FIELD = "shortdescription";

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private IndexSearchDao indexSearchDao;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Autowired
    private TagHelper tagHelper;

    @Override
    public IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields, final Set<String> match)
    {
        // Validate the search response fields
        validateSearchResponseFields(fields);

        // Validate the search response match
        validateSearchMatchFields(match);

        // Validate the search request
        validateIndexSearchRequest(request);

        Set<String> facetFields = new HashSet<>();
        if (CollectionUtils.isNotEmpty(request.getFacetFields()))
        {
            facetFields.addAll(validateFacetFields(new HashSet<>(request.getFacetFields())));

            //set the facets fields after validation
            request.setFacetFields(new ArrayList<>(facetFields));
        }

        // Fetch the current active indexes
        String bdefActiveIndex = searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
        String tagActiveIndex = searchIndexDaoHelper.getActiveSearchIndex(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());

        return indexSearchDao.indexSearch(request, fields, match, bdefActiveIndex, tagActiveIndex);
    }

    /**
     * Private method to validate the index search request.
     *
     * @param request the index search request
     */
    private void validateIndexSearchRequest(final IndexSearchRequest request)
    {
        // Validate that either a search term or search filter is provided
        Assert.isTrue(request.getSearchTerm() != null || request.getIndexSearchFilters() != null, "A search term or a search filter must be specified.");

        // Validate the search term if specified in the request
        if (request.getSearchTerm() != null)
        {
            validateIndexSearchRequestSearchTerm(request.getSearchTerm());
        }

        // Validate the index search filters if specified in the request
        if (request.getIndexSearchFilters() != null)
        {
            validateIndexSearchFilters(request.getIndexSearchFilters());
        }
    }

    /**
     * Private method to validate the index search request search term.
     *
     * @param indexSearchTerm the index search term string
     */
    private void validateIndexSearchRequestSearchTerm(final String indexSearchTerm)
    {
        // The following characters will be react like spaces during the search: '-' and '_'
        // Confirm that the search term is long enough
        Assert.isTrue(indexSearchTerm.replace('-', ' ').replace('_', ' ').trim().length() >= SEARCH_TERM_MINIMUM_ALLOWABLE_LENGTH,
            "The search term length must be at least " + SEARCH_TERM_MINIMUM_ALLOWABLE_LENGTH + " characters.");
    }

    /**
     * Validates the specified index search filters.
     *
     * @param indexSearchFilters the index search filters
     */
    private void validateIndexSearchFilters(List<IndexSearchFilter> indexSearchFilters)
    {
        // Validate that the search filters list is not empty
        Assert.notEmpty(indexSearchFilters, "At least one index search filter must be specified.");

        for (IndexSearchFilter searchFilter : indexSearchFilters)
        {
            // Silently skip a search filter which is null
            if (searchFilter != null)
            {
                // Validate that each search filter has at least one index search key
                Assert.notEmpty(searchFilter.getIndexSearchKeys(), "At least one index search key must be specified.");

                // Guard against a single null element in the index search keys list
                if (searchFilter.getIndexSearchKeys().get(0) != null)
                {
                    // Get the instance type of the key in the search filter, match all other keys with this
                    Class<?> expectedInstanceType =
                        searchFilter.getIndexSearchKeys().get(0).getIndexSearchResultTypeKey() != null ? IndexSearchResultTypeKey.class : TagKey.class;

                    searchFilter.getIndexSearchKeys().forEach(indexSearchKey -> {
                        // Validate that each search key has either an index search result type key or a tag key
                        Assert.isTrue((indexSearchKey.getIndexSearchResultTypeKey() != null) ^ (indexSearchKey.getTagKey() != null),
                            "Exactly one instance of index search result type key or tag key must be specified.");

                        Class<?> actualInstanceType = indexSearchKey.getIndexSearchResultTypeKey() != null ? IndexSearchResultTypeKey.class : TagKey.class;

                        // Validate that search keys within the same filter have either index search result type keys or tag keys
                        Assert.isTrue(expectedInstanceType.equals(actualInstanceType),
                            "Index search keys should be a homogeneous list of either index search result type keys or tag keys.");

                        // Validate tag key if present
                        if (indexSearchKey.getTagKey() != null)
                        {
                            tagHelper.validateTagKey(indexSearchKey.getTagKey());

                            // Validates that a tag entity exists for the specified tag key and gets the actual key from the database
                            // We then modify the index search filter key to use the actual values because it eventually becomes a filter query and it will not
                            // automatically be case-sensitivity and whitespace resilient.
                            TagEntity actualTagEntity = tagDaoHelper.getTagEntity(indexSearchKey.getTagKey());
                            TagKey tagKey = new TagKey(actualTagEntity.getTagType().getCode(), actualTagEntity.getTagCode());

                            indexSearchKey.setTagKey(tagKey);
                        }

                        // Validate search result type key if present
                        if (indexSearchKey.getIndexSearchResultTypeKey() != null)
                        {
                            validateIndexSearchResultTypeKey(indexSearchKey.getIndexSearchResultTypeKey());

                            // Ensure that specified search index type exists.
                            searchIndexTypeDaoHelper.getSearchIndexTypeEntity(indexSearchKey.getIndexSearchResultTypeKey().getIndexSearchResultType());
                        }
                    });
                }
            }
        }
    }

    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(SHORT_DESCRIPTION_FIELD, DISPLAY_NAME_FIELD);
    }

    private Set<String> getValidSearchMatchFields()
    {
        return ImmutableSet.of(MATCH_COLUMN_FIELD);
    }

    @Override
    public Set<String> getValidFacetFields()
    {
        return ImmutableSet.of(ElasticsearchHelper.TAG_FACET, ElasticsearchHelper.RESULT_TYPE_FACET);
    }

    /**
     * Validates an index search result type key. This method also trims the key parameters.
     *
     * @param indexSearchResultTypeKey the specified index search result type key
     */
    private void validateIndexSearchResultTypeKey(IndexSearchResultTypeKey indexSearchResultTypeKey)
    {
        indexSearchResultTypeKey.setIndexSearchResultType(
            alternateKeyHelper.validateStringParameter("An", "index search result type", indexSearchResultTypeKey.getIndexSearchResultType()));
    }

    /**
     * Validates a set of search match fields. This method also trims and lowers the match fields.
     *
     * @param match the search match fields to be validated
     */
    private void validateSearchMatchFields(Set<String> match)
    {
        // Create a local copy of the match fields set so that we can stream it to modify the match fields set
        Set<String> localCopy = new HashSet<>(match);

        // Clear the match set
        match.clear();

        // Add to the match set field the strings both trimmed and lower cased and filter out empty and null strings
        localCopy.stream().filter(StringUtils::isNotBlank).map(String::trim).map(String::toLowerCase).forEachOrdered(match::add);

        // Validate the field names
        match.forEach(field -> Assert.isTrue(getValidSearchMatchFields().contains(field), String.format("Search match field \"%s\" is not supported.", field)));
    }
}
