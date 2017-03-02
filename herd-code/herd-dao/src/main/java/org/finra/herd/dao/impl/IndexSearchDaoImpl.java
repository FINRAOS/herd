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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.IndexSearchDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;
import org.finra.herd.model.api.xml.IndexSearchResult;
import org.finra.herd.model.api.xml.IndexSearchResultKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * IndexSearchDaoImpl
 */
@Repository
public class IndexSearchDaoImpl implements IndexSearchDao
{
    /**
     * Best fields query boost
     */
    private static final float BEST_FIELDS_QUERY_BOOST = 1f;

    /**
     * The business object definition index
     */
    private static final String BUSINESS_OBJECT_DEFINITION_INDEX = "bdef";

    /**
     * The boost amount for the business object definition index
     */
    private static final float BUSINESS_OBJECT_DEFINITION_INDEX_BOOST = 1f;

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
     * Source string for the name
     */
    private static final String NAME_SOURCE = "name";

    /**
     * String to select the namespace
     */
    private static final String NAMESPACE = "namespace";

    /**
     * Source string for the namespace code
     */
    private static final String NAMESPACE_CODE_SOURCE = "namespace.code";

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
     * The stop words analyzer, will use the elastic search default stop words
     */
    private static final String STOP_ANALYZER = "stop";

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
     * Source string for the tagType.code
     */
    private static final String TAG_TYPE_CODE_SOURCE = "tagType.code";

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

    @Override
    public IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields)
    {
        // Build two multi match queries, one with phrase prefix, and one with best fields, but boost the phrase prefix
        final MultiMatchQueryBuilder phrasePrefixMultiMatchQueryBuilder =
            buildMultiMatchQuery(request.getSearchTerm(), PHRASE_PREFIX, PHRASE_PREFIX_QUERY_BOOST);
        final MultiMatchQueryBuilder bestFieldsMultiMatchQueryBuilder = buildMultiMatchQuery(request.getSearchTerm(), BEST_FIELDS, BEST_FIELDS_QUERY_BOOST);

        // Add both multi match queries to a dis max query
        final QueryBuilder queryBuilder = QueryBuilders.disMaxQuery().add(phrasePrefixMultiMatchQueryBuilder).add(bestFieldsMultiMatchQueryBuilder);

        // The fields in the search indexes to return
        final String[] searchSources = {NAME_SOURCE, NAMESPACE_CODE_SOURCE, TAG_CODE_SOURCE, TAG_TYPE_CODE_SOURCE, DISPLAY_NAME_SOURCE, DESCRIPTION_SOURCE};

        // Create a new indexSearch source builder
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Fetch only the required fields
        searchSourceBuilder.fetchSource(searchSources, null);
        searchSourceBuilder.query(queryBuilder);

        // Create a indexSearch request builder
        final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(BUSINESS_OBJECT_DEFINITION_INDEX, TAG_INDEX);
        searchRequestBuilder.setSource(searchSourceBuilder).setSize(SEARCH_RESULT_SIZE)
            .addIndexBoost(BUSINESS_OBJECT_DEFINITION_INDEX, BUSINESS_OBJECT_DEFINITION_INDEX_BOOST).addIndexBoost(TAG_INDEX, TAG_INDEX_BOOST)
            .addSort(SortBuilders.scoreSort());

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
                    final Integer shortDescMaxLength = configurationHelper.getProperty(ConfigurationValue.TAG_SHORT_DESCRIPTION_LENGTH, Integer.class);
                    indexSearchResult.setShortDescription(HerdStringUtils.getShortDescription((String) sourceMap.get(DESCRIPTION_SOURCE), shortDescMaxLength));
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
                    final Integer shortDescMaxLength =
                        configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
                    indexSearchResult.setShortDescription(HerdStringUtils.getShortDescription((String) sourceMap.get(DESCRIPTION_SOURCE), shortDescMaxLength));
                }

                final BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey();
                businessObjectDefinitionKey.setNamespace((String) ((Map) sourceMap.get(NAMESPACE)).get(CODE));
                businessObjectDefinitionKey.setBusinessObjectDefinitionName((String) sourceMap.get(NAME_SOURCE));
                indexSearchResult.setIndexSearchResultKey(new IndexSearchResultKey(null, businessObjectDefinitionKey));
            }

            indexSearchResults.add(indexSearchResult);
        }

        return new IndexSearchResponse(searchHits.getTotalHits(), indexSearchResults);
    }

    /**
     * Private method to build a multi match query. This is a phrase prefix query that uses the stop analyzer. It will use the default stop words that are a
     * part of elasticsearch. The stop words are: , an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the, their,
     * then, there, these, they, this, to, was, will, with.
     *
     * @param searchTerm the term on which to search
     *
     * @return the multi match query
     */
    private MultiMatchQueryBuilder buildMultiMatchQuery(final String searchTerm, final MultiMatchQueryBuilder.Type queryType, final float queryBoost)
    {
        final MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchTerm).type(queryType).analyzer(STOP_ANALYZER);
        multiMatchQueryBuilder.boost(queryBoost);
        multiMatchQueryBuilder.field(TAG_CODE_SEARCH_FIELD, TAG_CODE_SEARCH_BOOST);
        multiMatchQueryBuilder.field(TAG_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(TAG_TYPE_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(DISPLAY_NAME_SEARCH_FIELD, DISPLAY_NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(DESCRIPTION_SEARCH_FIELD, DESCRIPTION_SEARCH_BOOST);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_TAG_TYPE_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(NAME_SEARCH_FIELD, NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(DATA_PROVIDER_SEARCH_FIELD, DATA_PROVIDER_SEARCH_BOOST);
        multiMatchQueryBuilder.field(NAMESPACE_CODE_SEARCH_FIELD, NAMESPACE_CODE_SEARCH_BOOST);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_USAGE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_FILE_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_FILE_TYPE_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_VALUE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_PARTITION_KEY_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_PARTITION__SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECTS_FORMATS_ATTRIBUTE_DEFINITIONS_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(ATTRIBUTES_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(ATTRIBUTES_VALUE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(COLUMNS_NAME_SEARCH_FIELD, COLUMNS_NAME_SEARCH_BOOST);
        multiMatchQueryBuilder.field(COLUMNS_DESCRIPTION_SEARCH_FIELD, COLUMNS_DESCRIPTION_SEARCH_BOOST);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD);
        multiMatchQueryBuilder.field(DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_USAGE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_FILE_TYPE_CODE_SEARCH_FIELD);
        multiMatchQueryBuilder.field(SAMPLE_DATA_FILES_FILE_NAME_SEARCH_FIELD);
        multiMatchQueryBuilder.field(SAMPLE_DATA_FILES_DIRECTORY_PATH_SEARCH_FIELD);
        return multiMatchQueryBuilder;
    }
}
