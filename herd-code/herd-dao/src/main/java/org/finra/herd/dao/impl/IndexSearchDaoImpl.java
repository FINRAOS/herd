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
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.PHRASE;
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
import org.apache.commons.lang.StringUtils;
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
import org.finra.herd.dao.helper.HerdSearchQueryHelper;
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
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.IndexSearchHighlightFields;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;

@Repository
public class IndexSearchDaoImpl implements IndexSearchDao
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexSearchDaoImpl.class);

    /**
     * String to select the tag type code and namespace code
     */
    private static final String CODE = "code";

    /**
     * String that represents the column name field
     */
    private static final String COLUMNS_NAME_FIELD = "columns.name";

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
     * String to signify the match field is column
     */
    private static final String MATCH_COLUMN = "column";

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
     * The number of the indexSearch results to return
     */
    private static final int SEARCH_RESULT_SIZE = 200;

    /**
     * String that represents the schemaColumn name field
     */
    private static final String SCHEMA_COLUMNS_NAME_FIELD = "schemaColumns.name";

    /**
     * Constant to hold the short description option for the business object definition search
     */
    private static final String SHORT_DESCRIPTION_FIELD = "shortdescription";

    /**
     * N-Grams field type
     */
    private static final String FIELD_TYPE_NGRAMS = "ngrams";

    /**
     * Shingles field type
     */
    private static final String FIELD_TYPE_SHINGLES = "shingles";

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

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private ElasticsearchHelper elasticsearchHelper;

    @Autowired
    private JestClientHelper jestClientHelper;

    @Autowired
    private HerdSearchQueryHelper herdSearchQueryHelper;

    @Override
    public IndexSearchResponse indexSearch(final IndexSearchRequest indexSearchRequest, final Set<String> fields, final Set<String> match,
        final String bdefActiveIndex, final String tagActiveIndex)
    {
        // Build a basic Boolean query upon which add all the necessary clauses as needed
        BoolQueryBuilder indexSearchQueryBuilder = QueryBuilders.boolQuery();

        String searchPhrase = indexSearchRequest.getSearchTerm();

        // If there is a search phrase, then process it
        if (StringUtils.isNotEmpty(searchPhrase))
        {
            // Determine if negation terms are present
            boolean negationTermsExist = herdSearchQueryHelper.determineNegationTermsPresent(indexSearchRequest);

            // Add the negation queries builder within a 'must-not' clause to the parent bool query if negation terms exist
            if (negationTermsExist)
            {
                // Build negation queries- each term is added to the query with a 'must-not' clause,
                List<String> negationTerms = herdSearchQueryHelper.extractNegationTerms(indexSearchRequest);

                if (CollectionUtils.isNotEmpty(negationTerms))
                {
                    negationTerms.forEach(term ->
                    {
                        indexSearchQueryBuilder.mustNot(buildMultiMatchQuery(term, PHRASE, 100f, FIELD_TYPE_STEMMED, match));
                    });
                }

                // Remove the negation terms from the search phrase
                searchPhrase = herdSearchQueryHelper.extractSearchPhrase(indexSearchRequest);
            }

            // Build a Dismax query with three primary components (multi-match queries) with boost values, these values can be configured in the
            // DB which provides a way to dynamically tune search behavior at runtime:
            //  1. Phrase match query on shingles fields.
            //  2. Phrase prefix query on stemmed fields.
            //  3. Best fields query on ngrams fields.
            final MultiMatchQueryBuilder phrasePrefixMultiMatchQueryBuilder = buildMultiMatchQuery(searchPhrase, PHRASE_PREFIX,
                configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_PHRASE_PREFIX_QUERY_BOOST, Float.class), FIELD_TYPE_STEMMED, match);

            final MultiMatchQueryBuilder bestFieldsMultiMatchQueryBuilder = buildMultiMatchQuery(searchPhrase, BEST_FIELDS,
                configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BEST_FIELDS_QUERY_BOOST, Float.class), FIELD_TYPE_NGRAMS, match);

            final MultiMatchQueryBuilder phraseMultiMatchQueryBuilder =
                buildMultiMatchQuery(searchPhrase, PHRASE, configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_PHRASE_QUERY_BOOST, Float.class),
                    FIELD_TYPE_SHINGLES, match);

            final MultiMatchQueryBuilder phraseStemmedMultiMatchQueryBuilder =
                buildMultiMatchQuery(searchPhrase, PHRASE, configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_PHRASE_QUERY_BOOST, Float.class),
                    FIELD_TYPE_STEMMED, match);

            // Add the multi match queries to a dis max query and add to the parent bool query within a 'must' clause
            indexSearchQueryBuilder.must(
                disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder).add(phraseMultiMatchQueryBuilder)
                    .add(phraseStemmedMultiMatchQueryBuilder));
        }

        // Add filter clauses if index search filters are specified in the request
        if (CollectionUtils.isNotEmpty(indexSearchRequest.getIndexSearchFilters()))
        {
            indexSearchQueryBuilder
                .filter(elasticsearchHelper.addIndexSearchFilterBooleanClause(indexSearchRequest.getIndexSearchFilters(), bdefActiveIndex, tagActiveIndex));
        }

        // Get function score query builder
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getFunctionScoreQueryBuilder(indexSearchQueryBuilder, bdefActiveIndex);

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

        // Add highlighting if specified in the request
        if (BooleanUtils.isTrue(indexSearchRequest.isEnableHitHighlighting()))
        {
            // Fetch configured 'tag' values for highlighting
            String preTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_PRETAGS);
            String postTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_POSTTAGS);

            searchRequestBuilder.highlighter(buildHighlightQuery(preTag, postTag, match));
        }

        // Add facet aggregations if specified in the request
        if (CollectionUtils.isNotEmpty(indexSearchRequest.getFacetFields()))
        {
            searchRequestBuilder = elasticsearchHelper.addFacetFieldAggregations(new HashSet<>(indexSearchRequest.getFacetFields()), searchRequestBuilder);
        }

        // Log the actual elasticsearch query when debug is enabled
        LOGGER.debug("indexSearchRequest={}", searchRequestBuilder.toString());

        // Retrieve the indexSearch response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString()).addIndices(Arrays.asList(bdefActiveIndex, tagActiveIndex));
        final SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());
        final List<IndexSearchResult> indexSearchResults =
            buildIndexSearchResults(fields, tagActiveIndex, bdefActiveIndex, searchResult, indexSearchRequest.isEnableHitHighlighting());

        List<Facet> facets = null;
        if (CollectionUtils.isNotEmpty(indexSearchRequest.getFacetFields()))
        {
            // Extract facets from the search response
            facets = new ArrayList<>(extractFacets(indexSearchRequest, searchResult, bdefActiveIndex, tagActiveIndex));
        }

        return new IndexSearchResponse(searchResult.getTotal(), indexSearchResults, facets);
    }

    /**
     * Extracts and builds a list of {@link IndexSearchResult}s from a given {@link SearchResult}
     *
     * @param fields the specified fields to be included in the response
     * @param tagActiveIndex the name of the active tag index
     * @param bdefActiveIndex the name of the active business object definition index
     * @param searchResult the raw search result returned by the elasticsearch client
     * @param isHighlightingEnabled boolean which specifies if highlighting is requested or not
     *
     * @return A {@link List} of {@link IndexSearchResult} which represent the search response
     */
    private List<IndexSearchResult> buildIndexSearchResults(Set<String> fields, String tagActiveIndex, String bdefActiveIndex, SearchResult searchResult,
        Boolean isHighlightingEnabled)
    {
        final Integer tagShortDescMaxLength = configurationHelper.getProperty(ConfigurationValue.TAG_SHORT_DESCRIPTION_LENGTH, Integer.class);
        final Integer businessObjectDefinitionShortDescMaxLength =
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);

        List<IndexSearchResult> indexSearchResults = new ArrayList<>();

        try
        {


            final List<SearchResult.Hit<Map, Void>> searchHitList = searchResult.getHits(Map.class);

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
                indexSearchResult.setSearchIndexKey(new SearchIndexKey(index));
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
                    indexSearchResult.setIndexSearchResultType(SearchIndexTypeEntity.SearchIndexTypes.TAG.name());
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
                    indexSearchResult.setIndexSearchResultType(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name());
                    indexSearchResult.setIndexSearchResultKey(new IndexSearchResultKey(null, businessObjectDefinitionKey));
                }
                else
                {
                    throw new IllegalStateException(String
                        .format("Search result index name \"%s\" does not match any of the active search indexes. tagActiveIndex=\"%s\" bdefActiveIndex=\"%s\"",
                            index, tagActiveIndex, bdefActiveIndex));
                }

                if (BooleanUtils.isTrue(isHighlightingEnabled))
                {
                    // Fetch configured 'tag' values for highlighting
                    String preTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_PRETAGS);
                    String postTag = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_POSTTAGS);

                    // Extract highlighted content from the search hit and clean html tags except the pre/post-tags as configured
                    Highlight highlightedContent = extractHighlightedContent(hit, preTag, postTag);

                    // Set highlighted content in the response element
                    indexSearchResult.setHighlight(highlightedContent);
                }

                indexSearchResults.add(indexSearchResult);
            }
        }
        catch (RuntimeException e)
        {
            // Log the error along with the search response and throw the exception.
            LOGGER.error("Failed to parse search results. tagActiveIndex=\"{}\" bdefActiveIndex=\"{}\" fields={} isHighlightingEnabled={} searchResult={}",
                tagActiveIndex, bdefActiveIndex, jsonHelper.objectToJson(fields), isHighlightingEnabled, jsonHelper.objectToJson(searchResult), e);

            // Throw an exception.
            throw new IllegalStateException("Unexpected response received when attempting to retrieve search results.");
        }

        return indexSearchResults;
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
     * @param bdefActiveIndex the name of the active index for business object definitions
     * @param tagActiveIndex the name os the active index for tags
     *
     * @return A list of {@link Facet} objects
     */
    private List<Facet> extractFacets(IndexSearchRequest request, SearchResult searchResult, final String bdefActiveIndex, final String tagActiveIndex)
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

        return elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, bdefActiveIndex, tagActiveIndex);
    }

    /**
     * Private method to build a multi match query.
     *
     * @param searchTerm the term on which to search
     * @param queryType the query type for this multi match query
     * @param queryBoost the query boost for this multi match query
     * @param fieldType the field type for this multi match query
     * @param match the set of match fields that are to be searched upon in the index search
     *
     * @return the multi match query
     */
    private MultiMatchQueryBuilder buildMultiMatchQuery(final String searchTerm, final MultiMatchQueryBuilder.Type queryType, final float queryBoost,
        final String fieldType, Set<String> match)
    {
        // Get the slop value for this multi match query
        Integer phraseQuerySlop = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_PHRASE_QUERY_SLOP, Integer.class);

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchTerm).type(queryType);
        multiMatchQueryBuilder.boost(queryBoost);

        if (fieldType.equals(FIELD_TYPE_STEMMED))
        {
            // Get the configured value for 'stemmed' fields and their respective boosts if any
            String stemmedFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_STEMMED);

            // build the query
            buildMultiMatchQueryWithBoosts(multiMatchQueryBuilder, stemmedFieldsValue, match);

            if (queryType.equals(PHRASE))
            {
                // Set a "slop" value to allow the matched phrase to be slightly different from an exact phrase match
                // The slop parameter tells the match phrase query how far apart terms are allowed to be while still considering the document a match
                multiMatchQueryBuilder.slop(phraseQuerySlop);
            }
        }

        if (fieldType.equals(FIELD_TYPE_NGRAMS))
        {
            // Get the configured value for 'ngrams' fields and their respective boosts if any
            String ngramsFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_NGRAMS);

            // build the query
            buildMultiMatchQueryWithBoosts(multiMatchQueryBuilder, ngramsFieldsValue, match);
        }

        if (fieldType.equals(FIELD_TYPE_SHINGLES))
        {
            // Set a "slop" value to allow the matched phrase to be slightly different from an exact phrase match
            // The slop parameter tells the match phrase query how far apart terms are allowed to be while still considering the document a match
            multiMatchQueryBuilder.slop(phraseQuerySlop);

            // Get the configured value for 'shingles' fields and their respective boosts if any
            String shinglesFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_SHINGLES);

            // build the query
            buildMultiMatchQueryWithBoosts(multiMatchQueryBuilder, shinglesFieldsValue, match);
        }

        return multiMatchQueryBuilder;
    }

    /**
     * Private method to build a multimatch query based on a given set of fields and boost values in json format
     *
     * @param multiMatchQueryBuilder A {@link MultiMatchQueryBuilder} which should be constructed
     * @param fieldsBoostsJsonString A json formatted String which contains individual fields and their boost values
     * @param match the set of match fields that are to be searched upon in the index search
     */
    private void buildMultiMatchQueryWithBoosts(MultiMatchQueryBuilder multiMatchQueryBuilder, String fieldsBoostsJsonString, Set<String> match)
    {
        try
        {
            @SuppressWarnings("unchecked")
            final Map<String, String> fieldsBoostsMap = jsonHelper.unmarshallJsonToObject(Map.class, fieldsBoostsJsonString);

            // This additional step is needed because trying to cast an unmarshalled json to a Map of anything other than String key-value pairs won't work
            final Map<String, Float> fieldsBoosts = new HashMap<>();

            // If the match column is included
            if (match != null && match.contains(MATCH_COLUMN))
            {
                // Add only the column.name and schemaColumn.name fields to the fieldsBoosts map
                fieldsBoostsMap.forEach((field, boostValue) ->
                {
                    if (field.contains(COLUMNS_NAME_FIELD) || field.contains(SCHEMA_COLUMNS_NAME_FIELD))
                    {
                        fieldsBoosts.put(field, Float.parseFloat(boostValue));
                    }
                });
            }
            else
            {
                fieldsBoostsMap.forEach((field, boostValue) -> fieldsBoosts.put(field, Float.parseFloat(boostValue)));
            }

            // Set the fields and their respective boosts to the multi-match query
            multiMatchQueryBuilder.fields(fieldsBoosts);
        }
        catch (IOException e)
        {
            LOGGER.warn("Could not parse the configured JSON value for ngrams fields: {}", ConfigurationValue.ELASTICSEARCH_SEARCHABLE_FIELDS_NGRAMS, e);
        }
    }

    /**
     * Builds a {@link HighlightBuilder} based on (pre/post)tags and fields fetched from the DB config which is added to the main {@link SearchRequestBuilder}
     *
     * @param preTag The specified pre-tag to be used for highlighting
     * @param postTag The specified post-tag to be used for highlighting
     * @param match the set of match fields that are to be searched upon in the index search
     *
     * @return A configured {@link HighlightBuilder} object
     */
    private HighlightBuilder buildHighlightQuery(String preTag, String postTag, Set<String> match)
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
        String highlightFieldsValue;

        // If the match column is included
        if (match != null && match.contains(MATCH_COLUMN))
        {
            highlightFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_COLUMN_MATCH_HIGHLIGHT_FIELDS);
        }
        else
        {
            highlightFieldsValue = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_HIGHLIGHT_FIELDS);
        }

        try
        {
            @SuppressWarnings("unchecked")
            IndexSearchHighlightFields highlightFieldsConfig = jsonHelper.unmarshallJsonToObject(IndexSearchHighlightFields.class, highlightFieldsValue);

            highlightFieldsConfig.getHighlightFields().forEach(highlightFieldConfig ->
            {

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
            LOGGER.warn("Could not parse the configured value for highlight fields: {}", highlightFieldsValue, e);
        }

        return highlightBuilder;
    }
}
