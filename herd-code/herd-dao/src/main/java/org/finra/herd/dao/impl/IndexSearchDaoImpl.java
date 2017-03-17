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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchResponseDto;

/**
 * IndexSearchDaoImpl
 */
@Repository
public class IndexSearchDaoImpl implements IndexSearchDao
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexSearchDaoImpl.class);

    /**
     * The attributes name search field
     */
    private static final String ATTRIBUTES_NAME_SEARCH_FIELD = "attributes.name";

    /**
     * The attributes value search field
     */
    private static final String ATTRIBUTES_VALUE_SEARCH_FIELD = "attributes.value";

    /**
     * Best fields query boost
     */
    private static final float BEST_FIELDS_QUERY_BOOST = 1f;

    /**
     * The business objects formats attributes name search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_NAME_SEARCH_FIELD = "businessObjectFormats.attributes.name";

    /**
     * The business objects formats attributes search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_VALUE_SEARCH_FIELD = "businessObjectFormats.attributes.value";

    /**
     * The business objects formats attributes definitions search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_ATTRIBUTE_DEFINITIONS_NAME_SEARCH_FIELD = "businessObjectFormats.attributeDefinitions.name";

    /**
     * The business objects formats description search search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_DESCRIPTION_SEARCH_FIELD = "businessObjectFormats.description";

    /**
     * The business objects formats file type code search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_FILE_TYPE_CODE_SEARCH_FIELD = "businessObjectFormats.fileType.code";

    /**
     * The business objects formats file type description search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_FILE_TYPE_DESCRIPTION_SEARCH_FIELD = "businessObjectFormats.fileType.description";

    /**
     * The business objects formats partition key search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_PARTITION_KEY_SEARCH_FIELD = "businessObjectFormats.partitionKey";

    /**
     * The business objects formats partition search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_PARTITION_SEARCH_FIELD = "businessObjectFormats.partitionKeyGroup.partitionKeyGroupName";

    /**
     * The business objects formats usage search field
     */
    private static final String BUSINESS_OBJECTS_FORMATS_USAGE_SEARCH_FIELD = "businessObjectFormats.usage";

    /**
     * The business object definition index
     */
    private static final String BUSINESS_OBJECT_DEFINITION_INDEX = "bdef";

    /**
     * The boost amount for the business object definition index
     */
    private static final float BUSINESS_OBJECT_DEFINITION_INDEX_BOOST = 1f;

    /**
     * The business object definition tags tag children tag entities description search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD =
        "businessObjectDefinitionTags.tag.childrenTagEntities.description";

    /**
     * The business object definition tags tag children tag entities display name search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD =
        "businessObjectDefinitionTags.tag.childrenTagEntities.displayName";

    /**
     * The business object definition tags tag children tag entities tag code search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD =
        "businessObjectDefinitionTags.tag.childrenTagEntities.tagCode";

    /**
     * The business object definition tags tag children tag entities tag type code search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD =
        "businessObjectDefinitionTags.tag.childrenTagEntities.tagType.code";

    /**
     * The business object definition tags tag children tag entities tag type display name search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD =
        "businessObjectDefinitionTags.tag.childrenTagEntities.tagType.displayName";

    /**
     * The business object definition tags tag description search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DESCRIPTION_SEARCH_FIELD = "businessObjectDefinitionTags.tag.description";

    /**
     * The business object definition tags tag display name search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DISPLAY_NAME_SEARCH_FIELD = "businessObjectDefinitionTags.tag.displayName";

    /**
     * The business object definition tags tag tag code search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_CODE_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagCode";

    /**
     * The business object definition tags tag tag type code search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_CODE_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagType.code";

    /**
     * The business object definition tags tag tag type display name search field
     */
    private static final String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagType.displayName";

    /**
     * The children tag entities description search field
     */
    private static final String CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD = "childrenTagEntities.description";

    /**
     * The children tag entities display name search field
     */
    private static final String CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD = "childrenTagEntities.displayName";

    /**
     * The children tag entities tag code search field
     */
    private static final String CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD = "childrenTagEntities.tagCode";

    /**
     * The children tag entities tag type code search field
     */
    private static final String CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD = "childrenTagEntities.tagType.code";

    /**
     * The children tag entities tag type description search field
     */
    private static final String CHILDREN_TAG_ENTITIES_TAG_TYPE_DESCRIPTION_SEARCH_FIELD = "childrenTagEntities.tagType.description";

    /**
     * The children tag entities tag type display name search field
     */
    private static final String CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "childrenTagEntities.tagType.displayName";

    /**
     * String to select the tag type code and namespace code
     */
    private static final String CODE = "code";

    /**
     * The columns description search boost
     */
    private static final Float COLUMNS_DESCRIPTION_SEARCH_BOOST = 10f;

    /**
     * The columns description search field
     */
    private static final String COLUMNS_DESCRIPTION_SEARCH_FIELD = "columns.description";

    /**
     * The columns name search boost
     */
    private static final Float COLUMNS_NAME_SEARCH_BOOST = 10f;

    /**
     * The columns name search field
     */
    private static final String COLUMNS_NAME_SEARCH_FIELD = "columns.name";

    /**
     * The data provider search boost
     */
    private static final Float DATA_PROVIDER_SEARCH_BOOST = 10f;

    /**
     * The data provider search field
     */
    private static final String DATA_PROVIDER_SEARCH_FIELD = "dataProvider.name";

    /**
     * The description search boost
     */
    private static final Float DESCRIPTION_SEARCH_BOOST = 10f;

    /**
     * The description search field
     */
    private static final String DESCRIPTION_SEARCH_FIELD = "description";

    /**
     * Source string for the description
     */
    private static final String DESCRIPTION_SOURCE = "description";

    /**
     * The descriptive business object format file type code search field
     */
    private static final String DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_FILE_TYPE_CODE_SEARCH_FIELD = "descriptiveBusinessObjectFormat.fileType.code";

    /**
     * The descriptive business object format usage search field
     */
    private static final String DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_USAGE_SEARCH_FIELD = "descriptiveBusinessObjectFormat.usage";

    /**
     * Constant to hold the display name option for the business object definition search
     */
    private static final String DISPLAY_NAME_FIELD = "displayname";

    /**
     * The display name search boost
     */
    private static final Float DISPLAY_NAME_SEARCH_BOOST = 50f;

    /**
     * The display name search field
     */
    private static final String DISPLAY_NAME_SEARCH_FIELD = "displayName";

    /**
     * Source string for the display name
     */
    private static final String DISPLAY_NAME_SOURCE = "displayName";

    /**
     * String to select the namespace
     */
    private static final String NAMESPACE = "namespace";

    /**
     * The namespace code search boost
     */
    private static final Float NAMESPACE_CODE_SEARCH_BOOST = 15f;

    /**
     * The namespace code search field
     */
    private static final String NAMESPACE_CODE_SEARCH_FIELD = "namespace.code";

    /**
     * Source string for the namespace code
     */
    private static final String NAMESPACE_CODE_SOURCE = "namespace.code";

    /**
     * The name search boost
     */
    private static final Float NAME_SEARCH_BOOST = 15f;

    /**
     * The name search field
     */
    private static final String NAME_SEARCH_FIELD = "name";

    /**
     * Source string for the name
     */
    private static final String NAME_SOURCE = "name";

    /**
     * N-Grams string appender for field names
     */
    private static final String NGRAMS_FIELD_NAME_APPENDER = ".ngrams";

    /**
     * Phrase prefix query boost
     */
    private static final float PHRASE_PREFIX_QUERY_BOOST = 10f;

    /**
     * The sample data files directory path search field
     */
    private static final String SAMPLE_DATA_FILES_DIRECTORY_PATH_SEARCH_FIELD = "sampleDataFiles.directoryPath";

    /**
     * The sample data files file name search field
     */
    private static final String SAMPLE_DATA_FILES_FILE_NAME_SEARCH_FIELD = "sampleDataFiles.fileName";

    /**
     * The number of the indexSearch results to return
     */
    private static final int SEARCH_RESULT_SIZE = 200;

    /**
     * Constant to hold the short description option for the business object definition search
     */
    private static final String SHORT_DESCRIPTION_FIELD = "shortdescription";

    /**
     * Stemmed string appender for field names
     */
    private static final String STEMMED_FIELD_NAME_APPENDER = ".stemmed";

    /**
     * The tag code search boost
     */
    private static final Float TAG_CODE_SEARCH_BOOST = 15f;

    /**
     * The tag code search field
     */
    private static final String TAG_CODE_SEARCH_FIELD = "tagCode";

    /**
     * Source string for the tagCode
     */
    private static final String TAG_CODE_SOURCE = "tagCode";

    /**
     * The tag index
     */
    private static final String TAG_INDEX = "tag";

    /**
     * The boost amount for the tag index
     */
    private static final float TAG_INDEX_BOOST = 1000f;

    /**
     * String to select the tag type
     */
    private static final String TAG_TYPE = "tagType";

    /**
     * The tag type code search field
     */
    private static final String TAG_TYPE_CODE_SEARCH_FIELD = "tagType.code";

    /**
     * Source string for the tagType.code
     */
    private static final String TAG_TYPE_CODE_SOURCE = "tagType.code";

    /**
     * The tag type description search field
     */
    private static final String TAG_TYPE_DESCRIPTION_SEARCH_FIELD = "tagType.description";

    /**
     * The tag type display name search field
     */
    private static final String TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "tagType.displayName";

    /**
     * The configuration helper used to retrieve configuration values
     */
    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * The transport client is a connection to the elasticsearch index
     */
    @Autowired
    private TransportClient transportClient;

    @Autowired
    private ElasticsearchHelper elasticsearchHelper;

    @Override
    public IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields)
    {
        final Integer tagShortDescMaxLength = configurationHelper.getProperty(ConfigurationValue.TAG_SHORT_DESCRIPTION_LENGTH, Integer.class);
        final Integer businessObjectDefinitionShortDescMaxLength =
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);

        // Build two multi match queries, one with phrase prefix, and one with best fields, but boost the phrase prefix
        final MultiMatchQueryBuilder phrasePrefixMultiMatchQueryBuilder =
            buildMultiMatchQuery(request.getSearchTerm(), PHRASE_PREFIX, PHRASE_PREFIX_QUERY_BOOST, STEMMED_FIELD_NAME_APPENDER);
        final MultiMatchQueryBuilder bestFieldsMultiMatchQueryBuilder =
            buildMultiMatchQuery(request.getSearchTerm(), BEST_FIELDS, BEST_FIELDS_QUERY_BOOST, NGRAMS_FIELD_NAME_APPENDER);

        QueryBuilder queryBuilder;

        // Add filter clauses if index search filters are specified in the request
        if (CollectionUtils.isNotEmpty(request.getIndexSearchFilters()))
        {
            BoolQueryBuilder indexSearchQueryBuilder = new BoolQueryBuilder();

            elasticsearchHelper.addIndexSearchFilterBooleanClause(request.getIndexSearchFilters(), indexSearchQueryBuilder);

            // Add both multi match queries and the boolean clauses to a dis max query
            queryBuilder =
                QueryBuilders.disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder).add(indexSearchQueryBuilder);

        }
        else
        {
            // Add only the multi match queries to a dis max query if no filters are specified
            queryBuilder = QueryBuilders.disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder);
        }

        // The fields in the search indexes to return
        final String[] searchSources = {NAME_SOURCE, NAMESPACE_CODE_SOURCE, TAG_CODE_SOURCE, TAG_TYPE_CODE_SOURCE, DISPLAY_NAME_SOURCE, DESCRIPTION_SOURCE};

        // Create a new indexSearch source builder
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Fetch only the required fields
        searchSourceBuilder.fetchSource(searchSources, null);
        searchSourceBuilder.query(queryBuilder);

        // Create a indexSearch request builder
        final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(BUSINESS_OBJECT_DEFINITION_INDEX, TAG_INDEX);
        searchRequestBuilder.setSource(searchSourceBuilder)
            .setSize(SEARCH_RESULT_SIZE)
            .addIndexBoost(BUSINESS_OBJECT_DEFINITION_INDEX, BUSINESS_OBJECT_DEFINITION_INDEX_BOOST)
            .addIndexBoost(TAG_INDEX, TAG_INDEX_BOOST)
            .addSort(SortBuilders.scoreSort());

        // Add facet aggregations if specified in the request
        if (CollectionUtils.isNotEmpty(request.getFacetFields()))
        {
            elasticsearchHelper.addFacetFieldAggregations(new HashSet<>(request.getFacetFields()), searchRequestBuilder);
        }

        // Log the actual elasticsearch query when debug is enabled
        LOGGER.debug("indexSearchRequest={}", searchRequestBuilder.toString());

        // Retrieve the indexSearch response
        final SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        final SearchHits searchHits = searchResponse.getHits();
        final SearchHit[] searchHitArray = searchHits.hits();

        final List<IndexSearchResult> indexSearchResults = new ArrayList<>();

        // For each indexSearch hit
        for (final SearchHit searchHit : searchHitArray)
        {
            // Get the source map from the indexSearch hit
            final Map<String, Object> sourceMap = searchHit.sourceAsMap();

            // Get the index from which this result is from
            final String index = searchHit.getShard().getIndex();

            // Create a new document to populate with the indexSearch results
            final IndexSearchResult indexSearchResult = new IndexSearchResult();

            // Populate the results
            indexSearchResult.setIndexSearchResultType(index);
            if (fields.contains(DISPLAY_NAME_FIELD))
            {
                indexSearchResult.setDisplayName((String) sourceMap.get(DISPLAY_NAME_SOURCE));
            }

            // Populate tag index specific key
            if (index.equals(TAG_INDEX))
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
            else if (index.equals(BUSINESS_OBJECT_DEFINITION_INDEX))
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

            indexSearchResults.add(indexSearchResult);
        }

        List<Facet> facets = null;
        if (CollectionUtils.isNotEmpty(request.getFacetFields()))
        {
            ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
            if (request.getFacetFields().contains(ElasticsearchHelper.TAG_FACET))
            {
                elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(elasticsearchHelper.getTagTagIndexSearchResponseDto(searchResponse));
            }
            if (request.getFacetFields().contains(ElasticsearchHelper.RESULT_TYPE_FACET))
            {
                elasticsearchResponseDto.setResultTypeIndexSearchResponseDtos(elasticsearchHelper.getResultTypeIndexSearchResponseDto(searchResponse));
            }

            facets = elasticsearchHelper.getFacetsReponse(elasticsearchResponseDto, true);
        }

        return new IndexSearchResponse(searchHits.getTotalHits(), indexSearchResults, facets);
    }

    /**
     * Private method to build a multi match query.
     *
     * @param searchTerm the term on which to search
     *
     * @return the multi match query
     */
    private MultiMatchQueryBuilder buildMultiMatchQuery(final String searchTerm, final MultiMatchQueryBuilder.Type queryType, final float queryBoost,
        final String append)
    {
        final MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchTerm).type(queryType);
        multiMatchQueryBuilder.boost(queryBoost);
        multiMatchQueryBuilder.field(TAG_CODE_SEARCH_FIELD + append, TAG_CODE_SEARCH_BOOST);
        multiMatchQueryBuilder.field(TAG_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(TAG_TYPE_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(DISPLAY_NAME_SEARCH_FIELD + append, DISPLAY_NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(DESCRIPTION_SEARCH_FIELD + append, DESCRIPTION_SEARCH_BOOST);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(NAME_SEARCH_FIELD + append, NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(DATA_PROVIDER_SEARCH_FIELD + append, DATA_PROVIDER_SEARCH_BOOST);
        multiMatchQueryBuilder.field(NAMESPACE_CODE_SEARCH_FIELD + append, NAMESPACE_CODE_SEARCH_BOOST);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_USAGE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_FILE_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_FILE_TYPE_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_VALUE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_PARTITION_KEY_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_PARTITION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTE_DEFINITIONS_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(ATTRIBUTES_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(ATTRIBUTES_VALUE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(COLUMNS_NAME_SEARCH_FIELD + append, COLUMNS_NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(COLUMNS_DESCRIPTION_SEARCH_FIELD + append, COLUMNS_DESCRIPTION_SEARCH_BOOST);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_USAGE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_FILE_TYPE_CODE_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(SAMPLE_DATA_FILES_FILE_NAME_SEARCH_FIELD + append);
        multiMatchQueryBuilder.field(SAMPLE_DATA_FILES_DIRECTORY_PATH_SEARCH_FIELD + append);
        return multiMatchQueryBuilder;
    }
}
