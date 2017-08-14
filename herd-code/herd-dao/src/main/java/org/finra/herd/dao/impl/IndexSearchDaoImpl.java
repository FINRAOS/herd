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
package org.finra.herd.dao.impl;

import static org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.BEST_FIELDS;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.PHRASE_PREFIX;
import static org.elasticsearch.index.query.QueryBuilders.disMaxQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.dao.helper.ElasticsearchClientImpl;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.dao.helper.JestClientHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.Field;
import org.finra.herd.model.api.xml.Highlight;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.IndexSearchHighlightFields;

/**
 * IndexSearchDaoImpl
 */
@Repository
public class IndexSearchDaoImpl implements IndexSearchDao
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexSearchDaoImpl.class);

    /**
     * Best fields query boost
     */
    private static final float BEST_FIELDS_QUERY_BOOST = 1f;

    /**
     * String to select the tag type code and namespace code
     */
    private static final String CODE = "code";

    /**
     * Source string for the description
     */
    private static final String DESCRIPTION_SOURCE = "description";

    /**
     * Constant to hold the display name option for the business object definition search
     */
    private static final String DISPLAY_NAME_FIELD = "displayname";

    /**
     * Source string for the display name
     */
    private static final String DISPLAY_NAME_SOURCE = "displayName";

    /**
     * String to select the namespace
     */
    private static final String NAMESPACE = "namespace";

    /**
     * Source string for the namespace code
     */
    private static final String NAMESPACE_CODE_SOURCE = "namespace.code";

    /**
     * Source string for the name
     */
    private static final String NAME_SOURCE = "name";

    /**
     * N-Grams field type
     */
    private static final String FIELD_TYPE_NGRAMS = "ngrams";

    /**
     * Phrase prefix query boost
     */
    private static final float PHRASE_PREFIX_QUERY_BOOST = 10f;

    /**
     * The number of the indexSearch results to return
     */
    private static final int SEARCH_RESULT_SIZE = 200;

    /**
     * Constant to hold the short description option for the business object definition search
     */
    private static final String SHORT_DESCRIPTION_FIELD = "shortdescription";

    /**
     * Stemmed field type
     */
    private static final String FIELD_TYPE_STEMMED = "stemmed";

    /**
     * Source string for the tagCode
     */
    private static final String TAG_CODE_SOURCE = "tagCode";

    /**
     * String to select the tag type
     */
    private static final String TAG_TYPE = "tagType";

    /**
     * Source string for the tagType.code
     */
    private static final String TAG_TYPE_CODE_SOURCE = "tagType.code";

    /**
     * Source string for the business object definition tags
     */
    private static final String BDEF_TAGS_SOURCE = "businessObjectDefinitionTags";

    /**
     * Source string for the business object definition tags search score multiplier
     */
    private static final String BDEF_TAGS_SEARCH_SCORE_MULTIPLIER = "tagSearchScoreMultiplier";

    /**
     * The configuration helper used to retrieve configuration values
     */
    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Helper to deserialize JSON values
     */
    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private ElasticsearchHelper elasticsearchHelper;

    @Autowired
    private JestClientHelper jestClientHelper;

    @Override
    public IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields, final String bdefActiveIndex,
        final String tagActiveIndex)
    {
        final Integer tagShortDescMaxLength = configurationHelper.getProperty(ConfigurationValue.TAG_SHORT_DESCRIPTION_LENGTH, Integer.class);
        final Integer businessObjectDefinitionShortDescMaxLength =
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);

        // Build two multi match queries, one with phrase prefix, and one with best fields, but boost the phrase prefix
        final MultiMatchQueryBuilder phrasePrefixMultiMatchQueryBuilder =
            buildMultiMatchQuery(request.getSearchTerm(), PHRASE_PREFIX, PHRASE_PREFIX_QUERY_BOOST, FIELD_TYPE_STEMMED);
        final MultiMatchQueryBuilder bestFieldsMultiMatchQueryBuilder =
            buildMultiMatchQuery(request.getSearchTerm(), BEST_FIELDS, BEST_FIELDS_QUERY_BOOST, FIELD_TYPE_NGRAMS);

        QueryBuilder queryBuilder;

        // Add filter clauses if index search filters are specified in the request
        if (CollectionUtils.isNotEmpty(request.getIndexSearchFilters()))
        {
            BoolQueryBuilder indexSearchQueryBuilder =
                elasticsearchHelper.addIndexSearchFilterBooleanClause(request.getIndexSearchFilters(), bdefActiveIndex, tagActiveIndex);

            // Add the multi match queries to a dis max query and wrap within a bool query, then apply filters to it
            queryBuilder = QueryBuilders.boolQuery().must(disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder))
                .filter(indexSearchQueryBuilder);
        }
        else
        {
            // Add only the multi match queries to a dis max query if no filters are specified
            queryBuilder = disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder);
        }

        // Get function score query builder
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getFunctionScoreQueryBuilder(queryBuilder, bdefActiveIndex);

        // The fields in the search indexes to return
        final String[] searchSources =
            {NAME_SOURCE, NAMESPACE_CODE_SOURCE, TAG_CODE_SOURCE, TAG_TYPE_CODE_SOURCE, DISPLAY_NAME_SOURCE, DESCRIPTION_SOURCE, BDEF_TAGS_SOURCE,
                BDEF_TAGS_SEARCH_SCORE_MULTIPLIER};

        // Create a new indexSearch source builder
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Fetch only the required fields
        searchSourceBuilder.fetchSource(searchSources, null);
        searchSourceBuilder.query(functionScoreQueryBuilder);

        // Create a indexSearch request builder
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(bdefActiveIndex, tagActiveIndex);
        searchRequestBuilder.setSource(searchSourceBuilder).setSize(SEARCH_RESULT_SIZE).addSort(SortBuilders.scoreSort());

        String preTag = null;
        String postTag = null;

        // Add highlighting if specified in the request
        if (BooleanUtils.isTrue(request.isEnableHitHighlighting()))
        {
            // Fetch configured 'tag' values for highlighting
            preTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_PRETAGS);
            postTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_POSTTAGS);

            searchRequestBuilder.highlighter(buildHighlightQuery(preTag, postTag));
        }

        // Add facet aggregations if specified in the request
        if (CollectionUtils.isNotEmpty(request.getFacetFields()))
        {
            searchRequestBuilder = elasticsearchHelper.addFacetFieldAggregations(new HashSet<>(request.getFacetFields()), searchRequestBuilder);
        }

        // Log the actual elasticsearch query when debug is enabled
        LOGGER.info("indexSearchRequest={}", searchRequestBuilder.toString());

        // Retrieve the indexSearch response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString()).addIndex(Arrays.asList(bdefActiveIndex, tagActiveIndex));
        final SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());
        final List<SearchResult.Hit<Map, Void>> searchHitList = searchResult.getHits(Map.class);

        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();

        // For each indexSearch hit
        for (final SearchResult.Hit<Map, Void> hit : searchHitList)
        {
            // Get the source map from the indexSearch hit
            @SuppressWarnings("unchecked")
            final Map<String, Object> sourceMap = hit.source;

            // Get the index from which this result is from
            final String index = hit.index;

            // Create a new document to populate with the indexSearch results
            final IndexSearchResult indexSearchResult = new IndexSearchResult();

            // Populate the results
            indexSearchResult.setIndexSearchResultType(index);
            if (fields.contains(DISPLAY_NAME_FIELD))
            {
                indexSearchResult.setDisplayName((String) sourceMap.get(DISPLAY_NAME_SOURCE));
            }

            // Populate tag index specific key
            if (index.equals(tagActiveIndex))
            {
                if (fields.contains(SHORT_DESCRIPTION_FIELD))
                {
                    indexSearchResult
                        .setShortDescription(HerdStringUtils.getShortDescription((String) sourceMap.get(DESCRIPTION_SOURCE), tagShortDescMaxLength));
                }

                final TagKey tagKey = new TagKey();
                tagKey.setTagCode((String) sourceMap.get(TAG_CODE_SOURCE));
                tagKey.setTagTypeCode((String) ((Map) sourceMap.get(TAG_TYPE)).get(CODE));
                indexSearchResult.setIndexSearchResultKey(new IndexSearchResultKey(tagKey, null));
            }
            // Populate business object definition key
            else if (index.equals(bdefActiveIndex))
            {
                if (fields.contains(SHORT_DESCRIPTION_FIELD))
                {
                    indexSearchResult.setShortDescription(
                        HerdStringUtils.getShortDescription((String) sourceMap.get(DESCRIPTION_SOURCE), businessObjectDefinitionShortDescMaxLength));
                }

                final BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey();
                businessObjectDefinitionKey.setNamespace((String) ((Map) sourceMap.get(NAMESPACE)).get(CODE));
                businessObjectDefinitionKey.setBusinessObjectDefinitionName((String) sourceMap.get(NAME_SOURCE));
                indexSearchResult.setIndexSearchResultKey(new IndexSearchResultKey(null, businessObjectDefinitionKey));
            }

            if (BooleanUtils.isTrue(request.isEnableHitHighlighting()))
            {
                // Extract highlighted content from the search hit and clean html tags except the pre/post-tags as configured
                Highlight highlightedContent = extractHighlightedContent(hit, preTag, postTag);

                // Set highlighted content in the response element
                indexSearchResult.setHighlight(highlightedContent);
            }

            indexSearchResults.add(indexSearchResult);
        }

        List<Facet> facets = null;
        if (CollectionUtils.isNotEmpty(request.getFacetFields()))
        {
            // Extract facets from the search response
            facets = new ArrayList<>(extractFacets(request, searchResult));
        }

        return new IndexSearchResponse(searchResult.getTotal(), indexSearchResults, facets);
    }

    /**
     * Processes the scripts and score function
     *
     * @param queryBuilder the query builder
     *
     * @return the function score query builder
     */
    private FunctionScoreQueryBuilder getFunctionScoreQueryBuilder(QueryBuilder queryBuilder, String bdefActiveIndex)
    {
        // Script for tag search score multiplier. If bdef set to tag search score multiplier else set to a default value.
        String inlineScript = "_score * (doc['_index'].value == '" + bdefActiveIndex + "' ? doc['" + BDEF_TAGS_SEARCH_SCORE_MULTIPLIER + "']: 1)";

        // Set the lang to groovy
        Script script = new Script(ScriptType.INLINE, "groovy", inlineScript, Collections.emptyMap());

        // Set the script
        ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders.scriptFunction(script);

        // Create function score query builder
        return new FunctionScoreQueryBuilder(queryBuilder, scoreFunction);
    }

    /**
     * Extracts highlighted content from a given {@link SearchHit}
     *
     * @param searchHit a given {@link SearchHit} from the elasticsearch results
     * @param preTag the specified pre-tag for highlighting
     * @param postTag the specified post-tag for highlighting
     *
     * @return {@link Highlight} a cleaned highlighted content
     */
    private Highlight extractHighlightedContent(SearchResult.Hit<Map, Void> searchHit, String preTag, String postTag)
    {
        Highlight highlightedContent = new Highlight();

        List<Field> highlightFields = new ArrayList<>();

        // make sure there is highlighted content in the search hit
        if (MapUtils.isNotEmpty(searchHit.highlight))
        {
            Set<String> keySet = searchHit.highlight.keySet();

            for (String key : keySet)
            {
                Field field = new Field();

                // Extract the field-name
                field.setFieldName(key);

                List<String> cleanFragments = new ArrayList<>();

                // Extract fragments which have the highlighted content
                List<String> fragments = searchHit.highlight.get(key);

                for (String fragment : fragments)
                {
                    cleanFragments.add(HerdStringUtils.stripHtml(fragment, preTag, postTag));
                }
                field.setFragments(cleanFragments);
                highlightFields.add(field);
            }
        }

        highlightedContent.setFields(highlightFields);

        return highlightedContent;
    }

    /**
     * Extracts facet information from a {@link SearchResult} object
     *
     * @param request The specified {@link IndexSearchRequest}
     * @param searchResult A given {@link SearchResult} to extract the facet information from
     *
     * @return A list of {@link Facet} objects
     */
    private List<Facet> extractFacets(IndexSearchRequest request, SearchResult searchResult)
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        if (request.getFacetFields().contains(ElasticsearchHelper.TAG_FACET))
        {
            elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(searchResult));
            elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(elasticsearchHelper.getTagTagIndexSearchResponseDto(searchResult));
        }
        if (request.getFacetFields().contains(ElasticsearchHelper.RESULT_TYPE_FACET))
        {
            elasticsearchResponseDto.setResultTypeIndexSearchResponseDtos(elasticsearchHelper.getResultTypeIndexSearchResponseDto(searchResult));
        }

        return elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, true);
    }

    /**
     * Private method to build a multi match query.
     *
     * @param searchTerm the term on which to search
     *
     * @return the multi match query
     */
    private MultiMatchQueryBuilder buildMultiMatchQuery(final String searchTerm, final MultiMatchQueryBuilder.Type queryType, final float queryBoost,
        final String fieldType)
    {
        final MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchTerm).type(queryType);
        multiMatchQueryBuilder.boost(queryBoost);

        if (fieldType.equals(FIELD_TYPE_STEMMED))
        {
            // Get the configured value for 'stemmed' fields and their respective boosts if any
            String stemmedFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_STEMMED);

            try
            {
                @SuppressWarnings("unchecked")
                final Map<String, String> stemmedFieldsWithBoost = jsonHelper.unmarshallJsonToObject(Map.class, stemmedFieldsValue);
                final Map<String, Float> fieldsBoosts = new HashMap<>();

                // This additional step is needed because trying to cast an unmarshalled json to a Map of anything other than String key-value pairs won't work
                stemmedFieldsWithBoost.entrySet().forEach(entry -> fieldsBoosts.put(entry.getKey(), Float.parseFloat(entry.getValue())));

                // Set the fields and their respective boosts to the multi-match query
                multiMatchQueryBuilder.fields(fieldsBoosts);
            }
            catch (IOException e)
            {
                LOGGER.warn("Could not parse the configured JSON value for stemmed fields: {}", ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_STEMMED, e);
            }
        }

        if (fieldType.equals(FIELD_TYPE_NGRAMS))
        {
            // Get the configured value for 'ngrams' fields and their respective boosts if any
            String ngramsFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_NGRAMS);

            try
            {
                @SuppressWarnings("unchecked")
                final Map<String, String> ngramsFieldsWithBoost = jsonHelper.unmarshallJsonToObject(Map.class, ngramsFieldsValue);

                final Map<String, Float> fieldsBoosts = new HashMap<>();

                // This additional step is needed because trying to cast an unmarshalled json to a Map of anything other than String key-value pairs won't work
                ngramsFieldsWithBoost.entrySet().forEach(entry -> fieldsBoosts.put(entry.getKey(), Float.parseFloat(entry.getValue())));

                // Set the fields and their respective boosts to the multi-match query
                multiMatchQueryBuilder.fields(fieldsBoosts);
            }
            catch (IOException e)
            {
                LOGGER.warn("Could not parse the configured JSON value for ngrams fields: {}", ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_NGRAMS, e);
            }
        }

        return multiMatchQueryBuilder;
    }

    /**
     * Builds a {@link HighlightBuilder} based on (pre/post)tags and fields fetched from the DB config which is added to the main {@link SearchRequestBuilder}
     *
     * @param preTag The specified pre-tag to be used for highlighting
     * @param postTag The specified post-tag to be used for highlighting
     *
     * @return A configured {@link HighlightBuilder} object
     */
    private HighlightBuilder buildHighlightQuery(String preTag, String postTag)
    {
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        // Field matching is not needed since we are matching on multiple 'type' fields like stemmed and ngrams and enabling highlighting on all those fields
        // will yield duplicates
        highlightBuilder.requireFieldMatch(false);

        // Set the configured value for pre-tags for highlighting
        highlightBuilder.preTags(preTag);

        // Set the configured value for post-tags for highlighting
        highlightBuilder.postTags(postTag);

        // Get highlight fields value from configuration
        String highlightFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_FIELDS);

        try
        {
            @SuppressWarnings("unchecked")
            IndexSearchHighlightFields highlightFieldsConfig = jsonHelper.unmarshallJsonToObject(IndexSearchHighlightFields.class, highlightFieldsValue);

            highlightFieldsConfig.getHighlightFields().forEach(highlightFieldConfig -> {

                // set the field name to the configured value
                HighlightBuilder.Field highlightField = new HighlightBuilder.Field(highlightFieldConfig.getFieldName());

                // set matched_fields to the configured list of fields, this accounts for 'multifields' that analyze the same string in different ways
                if (CollectionUtils.isNotEmpty(highlightFieldConfig.getMatchedFields()))
                {
                    highlightField.matchedFields(highlightFieldConfig.getMatchedFields().toArray(new String[0]));
                }

                // set fragment size to the configured value
                if (highlightFieldConfig.getFragmentSize() != null)
                {
                    highlightField.fragmentSize(highlightFieldConfig.getFragmentSize());
                }

                // set the number of desired fragments to the configured value
                if (highlightFieldConfig.getNumOfFragments() != null)
                {
                    highlightField.numOfFragments(highlightFieldConfig.getNumOfFragments());
                }

                highlightBuilder.field(highlightField);
            });

        }
        catch (IOException e)
        {
            LOGGER.warn("Could not parse the configured value for highlight fields: {}", ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_FIELDS, e);
        }

        return highlightBuilder;
    }
}