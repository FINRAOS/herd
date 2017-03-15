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
package org.finra.herd.service.functional;

import static org.finra.herd.service.functional.SearchFilterType.EXCLUSION_SEARCH_FILTER;
import static org.finra.herd.service.functional.SearchFilterType.INCLUSION_SEARCH_FILTER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.BusinessObjectDefinitionIndexSearchResponseDto;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.TagEntity;


/**
 * This class contains functions that can be used to work with an Elasticsearch index.
 */
@Component
public class ElasticsearchFunctions implements SearchFunctions
{
    /**
     * Page size
     */
    public static final int ELASTIC_SEARCH_SCROLL_PAGE_SIZE = 100;

    /**
     * Scroll keep alive in milliseconds
     */
    public static final int ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME = 60000;

    /**
     * Sort the business object definition by name
     */
    public static final String BUSINESS_OBJECT_DEFINITION_SORT_FIELD = "name.keyword";

    /**
     * The business object definition id search index key
     */
    public static final String SEARCH_INDEX_BUSINESS_OBJECT_DEFINITION_ID_KEY = "id";

    /**
     * Source string for the dataProvider name
     */
    public static final String DATA_PROVIDER_NAME_SOURCE = "dataProvider.name";

    /**
     * Source string for the description
     */
    public static final String DESCRIPTION_SOURCE = "description";

    /**
     * Source string for the display name
     */
    public static final String DISPLAY_NAME_SOURCE = "displayName";

    /**
     * The name keyword
     */
    public static final String NAME_FIELD = "name.keyword";

    /**
     * Source string for the name
     */
    public static final String NAME_SOURCE = "name";

    /**
     * Source string for the namespace code
     */
    public static final String NAMESPACE_CODE_SOURCE = "namespace.code";

    /**
     * Raw field for the namespace code
     */
    public static final String NAMESPACE_CODE_SORT_FIELD = "namespace.code.keyword";

    /**
     * The logger used to write messages to the log
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchFunctions.class);

    /**
     * The nested path of business object definition tags
     */
    public static final String NESTED_BDEFTAGS_PATH = "businessObjectDefinitionTags.tag";

    /**
     * The tag type code field
     */
    public static final String TAGTYPE_CODE_FIELD = NESTED_BDEFTAGS_PATH + ".tagType.code.keyword";

    /**
     * The tag type display name field
     */
    public static final String TAGTYPE_NAME_FIELD = NESTED_BDEFTAGS_PATH + ".tagType.displayName.keyword";

    /**
     * The tag code field
     */
    public static final String TAG_CODE_FIELD = NESTED_BDEFTAGS_PATH + ".tagCode.keyword";

    /**
     * The tag display name field
     */
    public static final String TAG_NAME_FIELD = NESTED_BDEFTAGS_PATH + ".displayName.keyword";

    /**
     * The nested aggregation name for tag facet. User defined.
     */
    public static final String TAG_FACET_AGGS = "tagFacet";

    /**
     * The user defined tag type code sub aggregation name.
     */
    public static final String TAGTYPE_CODE_AGGREGATION = "tagTypeCodes";

    /**
     * The user defined tag type display name sub aggregation name.
     */
    public static final String TAGTYPE_NAME_AGGREGATION = "tagTypeDisplayNames";

    /**
     * The user defined tag code sub aggregation name.
     */
    public static final String TAG_CODE_AGGREGATION = "tagCodes";

    /**
     * The user defined tag display name sub aggregation name.
     */
    public static final String TAG_NAME_AGGREGATION = "tagDisplayNames";

    /**
     * The tag Facet Field  name
     */
    public static final String TAG_FACET = "tag";

    /**
     * The user defined agg name for tag type facet.
     */
    public static final String TAG_TYPE_FACET_AGGS = "tagTypeFacet";

    /**
     * The namespace code sub agg
     */
    public static final String NAMESPACE_CODE_AGGS = "namespaceCodes";

    /**
     * The business object definition name sub agg
     */
    public static final String BDEF_NAME_AGGS = "bdefName";

    /**
     * namespace field
     */
    public static final String NAMESPACE_FIELD = "namespace.code.keyword";

    /**
     * business object definition name field
     */
    public static final String BDEF_NAME_FIELD = "name.keyword";

    /**
     * The transport client is a connection to the elasticsearch index
     */
    @Autowired
    private TransportClient transportClient;

    /**
     * A helper class for JSON functionality
     */
    @Autowired
    private JsonHelper jsonHelper;

    /**
     * A helper class for working with Strings
     */
    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * The index function will take as arguments indexName, documentType, id, json and add the document to the index.
     */
    private final QuadConsumer<String, String, String, String> indexFunction = (indexName, documentType, id, json) -> {
        final IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, id);
        indexRequestBuilder.setSource(json);
        indexRequestBuilder.execute().actionGet();
    };

    /**
     * The validate function will take as arguments indexName, documentType, id, json and validate the document against the index.
     */
    private final QuadConsumer<String, String, String, String> validateFunction = (indexName, documentType, id, json) -> {
        LOGGER.info("Validating Elasticsearch document, indexName={}, documentType={}, id={}.", indexName, documentType, id);

        // Get the document from the index
        final GetRequestBuilder getRequestBuilder = transportClient.prepareGet(indexName, documentType, id);
        final GetResponse getResponse = getRequestBuilder.execute().actionGet();

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // If the document does not exist in the index add the document to the index
        if (StringUtils.isEmpty(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not exist in the index, adding the document to the index.");
            final IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, id);
            indexRequestBuilder.setSource(json);
            indexRequestBuilder.execute().actionGet();
        }
        // Else if the JSON does not match the JSON from the index update the index
        else if (!json.equals(jsonStringFromIndex))
        {
            LOGGER.warn("Document does not match the document in the index, updating the document in the index.");
            final UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(indexName, documentType, id);
            updateRequestBuilder.setDoc(json);
            updateRequestBuilder.execute().actionGet();
        }
    };

    /**
     * The isValid function will take as arguments indexName, documentType, id, json and validate the document against the index and return true if the document
     * is valid and false otherwise.
     */
    private final QuadPredicate<String, String, String, String> isValidFunction = (indexName, documentType, id, json) -> {
        // Get the document from the index
        final GetResponse getResponse = transportClient.prepareGet(indexName, documentType, id).execute().actionGet();

        // Retrieve the JSON string from the get response
        final String jsonStringFromIndex = getResponse.getSourceAsString();

        // Return true if the json from the index is not null or empty and the json from the index matches the object from the database
        return StringUtils.isNotEmpty(jsonStringFromIndex) && jsonStringFromIndex.equals(json);
    };

    /**
     * The index exists predicate will take as an argument the index name and will return tree if the index exists and false otherwise.
     */
    private final Predicate<String> indexExistsFunction = indexName -> {
        final IndicesExistsResponse indicesExistsResponse = transportClient.admin().indices().prepareExists(indexName).execute().actionGet();
        return indicesExistsResponse.isExists();
    };

    /**
     * The delete index function will take as an argument the index name and will delete the index.
     */
    private final Consumer<String> deleteIndexFunction = indexName -> {
        LOGGER.info("Deleting Elasticsearch index, indexName={}.", indexName);
        final DeleteIndexRequestBuilder deleteIndexRequestBuilder = transportClient.admin().indices().prepareDelete(indexName);
        deleteIndexRequestBuilder.execute().actionGet();
    };

    /**
     * The create index documents function will take as arguments the index name, document type, and a map of new documents. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    private final TriConsumer<String, String, Map<String, String>> createIndexDocumentsFunction = (indexName, documentType, documentMap) -> {
        LOGGER.info("Creating Elasticsearch index documents, indexName={}, documentType={}, documentMap={}.", indexName, documentType,
            Joiner.on(",").withKeyValueSeparator("=").join(documentMap));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

        // For each document prepare an insert request and add it to the bulk request builder
        documentMap.forEach((id, jsonString) -> {
            final IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, id);
            indexRequestBuilder.setSource(jsonString);
            bulkRequestBuilder.add(indexRequestBuilder);
        });

        // Execute the bulk update request
        final BulkResponse bulkResponse = bulkRequestBuilder.get();

        // If there are failures log them
        if (bulkResponse.hasFailures())
        {
            LOGGER.error("Bulk response error = {}", bulkResponse.buildFailureMessage());
        }
    };

    /**
     * The create index function will take as arguments the index name, document type, and mapping and will create a new index.
     */
    private final QuadConsumer<String, String, String, String> createIndexFunction = (indexName, documentType, mapping, settings) -> {
        LOGGER.info("Creating Elasticsearch index, indexName={}, documentType={}.", indexName, documentType);
        final CreateIndexRequestBuilder createIndexRequestBuilder = transportClient.admin().indices().prepareCreate(indexName);
        createIndexRequestBuilder.setSettings(settings);
        createIndexRequestBuilder.addMapping(documentType, mapping);
        createIndexRequestBuilder.execute().actionGet();
    };

    /**
     * The delete document by id function will delete a document in the index by the document id.
     */
    private final TriConsumer<String, String, String> deleteDocumentByIdFunction = (indexName, documentType, id) -> {
        LOGGER.info("Deleting Elasticsearch document from index, indexName={}, documentType={}, id={}.", indexName, documentType, id);
        final DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(indexName, documentType, id);
        deleteRequestBuilder.execute().actionGet();
    };

    /**
     * The delete index documents function will delete a list of document in the index by a list of document ids.
     */
    private final TriConsumer<String, String, List<Integer>> deleteIndexDocumentsFunction = (indexName, documentType, ids) -> {
        LOGGER.info("Deleting Elasticsearch documents from index, indexName={}, documentType={}, ids={}.", indexName, documentType,
            ids.stream().map(Object::toString).collect(Collectors.joining(",")));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

        // For each document prepare a delete request and add it to the bulk request builder
        ids.forEach(id -> {
            final DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(indexName, documentType, id.toString());
            bulkRequestBuilder.add(deleteRequestBuilder);
        });

        // Execute the bulk update request
        final BulkResponse bulkResponse = bulkRequestBuilder.get();

        // If there are failures log them
        if (bulkResponse.hasFailures())
        {
            LOGGER.error("Bulk response error = {}", bulkResponse.buildFailureMessage());
        }
    };

    /**
     * The number of types in index function will take as arguments the index name and the document type and will return the number of documents in the index.
     */
    private final BiFunction<String, String, Long> numberOfTypesInIndexFunction = (indexName, documentType) -> {
        final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName).setTypes(documentType);
        final SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        return searchResponse.getHits().getTotalHits();
    };

    /**
     * The ids in index function will take as arguments the index name and the document type and will return a list of all the ids in the index.
     */
    private final BiFunction<String, String, List<String>> idsInIndexFunction = (indexName, documentType) -> {
        // Create an array list for storing the ids
        List<String> idList = new ArrayList<>();

        // Create a search request and set the scroll time and scroll size
        final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);
        searchRequestBuilder.setTypes(documentType).setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
            .setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.hits();

        // While there are hits available, page through the results and add them to the id list
        while (hits.length != 0)
        {
            for (SearchHit searchHit : hits)
            {
                idList.add(searchHit.id());
            }

            SearchScrollRequestBuilder searchScrollRequestBuilder = transportClient.prepareSearchScroll(searchResponse.getScrollId());
            searchScrollRequestBuilder.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
            searchResponse = searchScrollRequestBuilder.execute().actionGet();
            searchHits = searchResponse.getHits();
            hits = searchHits.hits();
        }

        return idList;
    };

    /**
     * Accepts a list of tag entity lists and returns a list of business object definition entities related to them.
     * <p>
     * Each list of tag entities comes from a single search filter, all of which are OR-ed together in a boolean term query for searching on the index. All such
     * lists come from individual search filters which are then AND-ed together and form a compound boolean query. This function performs a 'constant-score term
     * query' on the index based on tag code and tag type code because it is only a filtering query and no analysis/scoring is needed. The function also
     * retrieves a term-aggregation type facet information based on the facet field(s) if requested.
     */
    private final QuadFunction<String, String, List<Map<SearchFilterType, List<TagEntity>>>, Set<String>, ElasticsearchResponseDto>
        searchBusinessObjectDefinitionsByTagsFunction = (indexName, documentType, nestedTagEntityMaps, facetFieldsList) -> {

            ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

            List<List<TagEntity>> nestedInclusionTagEntityLists = new ArrayList<>();
            List<List<TagEntity>> nestedExclusionTagEntityLists = new ArrayList<>();

            for (Map<SearchFilterType, List<TagEntity>> tagEntityMap : nestedTagEntityMaps)
            {
                if (tagEntityMap.containsKey(INCLUSION_SEARCH_FILTER))
                {
                    nestedInclusionTagEntityLists.add(tagEntityMap.get(INCLUSION_SEARCH_FILTER));
                }
                else if (tagEntityMap.containsKey(EXCLUSION_SEARCH_FILTER))
                {
                    nestedExclusionTagEntityLists.add(tagEntityMap.get(EXCLUSION_SEARCH_FILTER));
                }
            }

            LOGGER.info("Searching Elasticsearch business object definition documents from index, indexName={} and documentType={}, by tagEntityList={}",
                indexName, documentType, tagEntityListToString(flattenTagEntitiesList(nestedInclusionTagEntityLists)));

            LOGGER.info("Excluding the following tagEntityList={}",
                indexName, documentType, tagEntityListToString(flattenTagEntitiesList(nestedExclusionTagEntityLists)));

            BoolQueryBuilder compoundSearchFiltersQueryBuilder = new BoolQueryBuilder();

            // If there are only exclusion tag entities then, get everything else, but the exclusion tags.
            if (CollectionUtils.isEmpty(flattenTagEntitiesList(nestedInclusionTagEntityLists)))
            {
                WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery(NAME_FIELD, "*");
                compoundSearchFiltersQueryBuilder.must(wildcardQueryBuilder);
            }

            // Inclusion
            for (List<TagEntity> tagEntities : nestedInclusionTagEntityLists)
            {
                BoolQueryBuilder searchFilterQueryBuilder = new BoolQueryBuilder();

                for (TagEntity tagEntity : tagEntities)
                {
                    // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                    ConstantScoreQueryBuilder searchKeyQueryBuilder = QueryBuilders.constantScoreQuery(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(TAGTYPE_CODE_FIELD, tagEntity.getTagType().getCode()))
                            .must(QueryBuilders.termQuery(TAG_CODE_FIELD, tagEntity.getTagCode()))
                    );

                    // Individual tag-keys are OR-ed
                    searchFilterQueryBuilder.should(searchKeyQueryBuilder);
                }

                // Individual search-filters are AND-ed
                compoundSearchFiltersQueryBuilder.must(searchFilterQueryBuilder);
            }

            // Exclusion
            for (List<TagEntity> tagEntities : nestedExclusionTagEntityLists)
            {
                for (TagEntity tagEntity : tagEntities)
                {
                    // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                    QueryBuilder searchKeyQueryBuilder = QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(TAGTYPE_CODE_FIELD, tagEntity.getTagType().getCode()))
                        .must(QueryBuilders.termQuery(TAG_CODE_FIELD, tagEntity.getTagCode()));

                    // Exclusion: individual tag-keys are added as a must not query
                    compoundSearchFiltersQueryBuilder.mustNot(searchKeyQueryBuilder);
                }
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            // Fetch only the required fields
            searchSourceBuilder
                .fetchSource(new String[] {DATA_PROVIDER_NAME_SOURCE, DESCRIPTION_SOURCE, DISPLAY_NAME_SOURCE, NAME_SOURCE, NAMESPACE_CODE_SOURCE}, null);
            searchSourceBuilder.query(compoundSearchFiltersQueryBuilder);

            // Create a search request and set the scroll time and scroll size
            final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);

            // Construct scroll query
            searchRequestBuilder.setTypes(documentType)
                .setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
                .setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)
                .setSource(searchSourceBuilder)

            // Add sorting criteria.
            // First, sort in ascending order on business object definition name
            // then sort in ascending order on namespace code
                .addSort(SortBuilders.fieldSort(BUSINESS_OBJECT_DEFINITION_SORT_FIELD).order(SortOrder.ASC))
                .addSort(SortBuilders.fieldSort(NAMESPACE_CODE_SORT_FIELD).order(SortOrder.ASC));

            //Add aggregation builder if facet fields are present
            if (CollectionUtils.isNotEmpty(facetFieldsList))
            {
                addFacetFieldAggregations(facetFieldsList, elasticsearchResponseDto, searchRequestBuilder);
            }

            // Log the actual search query
            LOGGER.info("bdefIndexSearchQuery={}", searchRequestBuilder.toString());

            elasticsearchResponseDto
                .setBusinessObjectDefinitionIndexSearchResponseDtos(scrollSearchResultsIntoBusinessObjectDefinitionDto(searchRequestBuilder));


            return elasticsearchResponseDto;
        };


    private void addFacetFieldAggregations(Set<String> facetFieldsList, ElasticsearchResponseDto elasticsearchResponseDto,
        SearchRequestBuilder searchRequestBuilder)
    {
        if (!CollectionUtils.isEmpty(facetFieldsList) && (facetFieldsList.contains(TAG_FACET)))
        {

            searchRequestBuilder.addAggregation(AggregationBuilders.nested(TAG_FACET_AGGS, NESTED_BDEFTAGS_PATH).subAggregation(
                AggregationBuilders.terms(TAGTYPE_CODE_AGGREGATION).field(TAGTYPE_CODE_FIELD).subAggregation(
                    AggregationBuilders.terms(TAGTYPE_NAME_AGGREGATION).field(TAGTYPE_NAME_FIELD).subAggregation(
                        AggregationBuilders.terms(TAG_CODE_AGGREGATION).field(TAG_CODE_FIELD)
                            .subAggregation(AggregationBuilders.terms(TAG_NAME_AGGREGATION).field(TAG_NAME_FIELD))))));

            searchRequestBuilder.addAggregation(AggregationBuilders.terms(TAG_TYPE_FACET_AGGS).field(TAGTYPE_CODE_FIELD).subAggregation(
                AggregationBuilders.terms(NAMESPACE_CODE_AGGS).field(NAMESPACE_FIELD)
                    .subAggregation(AggregationBuilders.terms(BDEF_NAME_AGGS).field(BDEF_NAME_FIELD))));

            elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(searchResponseIntoFacetInformation(searchRequestBuilder));

        }
    }

    /**
     * The find all business object definitions function will return all business object definition entities in the search index.
     */
    private final TriFunction<String, String, Set<String>, ElasticsearchResponseDto> findAllBusinessObjectDefinitionsFunction =
        (indexName, documentType, facetFieldsList) -> {

            LOGGER.info("Elasticsearch get all business object definition documents from index, indexName={} and documentType={}.", indexName, documentType);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder
                .fetchSource(new String[] {DATA_PROVIDER_NAME_SOURCE, DESCRIPTION_SOURCE, DISPLAY_NAME_SOURCE, NAME_SOURCE, NAMESPACE_CODE_SOURCE}, null);

            ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

            // Create a search request and set the scroll time and scroll size
            final SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);

            searchRequestBuilder.setTypes(documentType)
                .setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
                .setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)
                .setSource(searchSourceBuilder)

                // Set sort options.
                // First, sort on business object definition name
                // then sort on namespace code
                .addSort(SortBuilders.fieldSort(BUSINESS_OBJECT_DEFINITION_SORT_FIELD).order(SortOrder.ASC))
                .addSort(SortBuilders.fieldSort(NAMESPACE_CODE_SORT_FIELD).order(SortOrder.ASC));

            //Add aggregation builder if facet fields are present
            addFacetFieldAggregations(facetFieldsList, elasticsearchResponseDto, searchRequestBuilder);

            elasticsearchResponseDto
                .setBusinessObjectDefinitionIndexSearchResponseDtos(scrollSearchResultsIntoBusinessObjectDefinitionDto(searchRequestBuilder));

            return elasticsearchResponseDto;
        };

    private List<TagTypeIndexSearchResponseDto> searchResponseIntoFacetInformation(final SearchRequestBuilder searchRequestBuilder)
    {

        // Retrieve the search response
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Nested aggregation = searchResponse.getAggregations().get(TAG_FACET_AGGS);
        Terms tagTypeCodeAgg = aggregation.getAggregations().get(TAGTYPE_CODE_AGGREGATION);

        Terms tagTypeFacetAgg = searchResponse.getAggregations().get(TAG_TYPE_FACET_AGGS);

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();

        for (Terms.Bucket tagTypeCodeEntry : tagTypeCodeAgg.getBuckets())
        {
            List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos = new ArrayList<>();

            TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto =
                new TagTypeIndexSearchResponseDto(tagTypeCodeEntry.getKeyAsString(),
                    tagTypeFacetAgg.getBucketByKey(tagTypeCodeEntry.getKeyAsString()).getDocCount(), tagIndexSearchResponseDtos);
            tagTypeIndexSearchResponseDtos.add(tagTypeIndexSearchResponseDto);

            Terms tagTypeDisplayNameAggs = tagTypeCodeEntry.getAggregations().get(TAGTYPE_NAME_AGGREGATION);
            for (Terms.Bucket tagTypeDisplayNameEntry : tagTypeDisplayNameAggs.getBuckets())
            {
                tagTypeIndexSearchResponseDto.setDisplayName(tagTypeDisplayNameEntry.getKeyAsString());

                Terms tagCodeAggs = tagTypeDisplayNameEntry.getAggregations().get(TAG_CODE_AGGREGATION);
                TagIndexSearchResponseDto tagIndexSearchResponseDto;

                for (Terms.Bucket tagCodeEntry : tagCodeAggs.getBuckets())
                {
                    tagIndexSearchResponseDto = new TagIndexSearchResponseDto(tagCodeEntry.getKeyAsString(), tagCodeEntry.getDocCount());
                    tagIndexSearchResponseDtos.add(tagIndexSearchResponseDto);

                    Terms tagNameAggs = tagCodeEntry.getAggregations().get(TAG_NAME_AGGREGATION);
                    for (Terms.Bucket tagNameEntry : tagNameAggs.getBuckets())
                    {
                        tagIndexSearchResponseDto.setTagDisplayName(tagNameEntry.getKeyAsString());
                    }
                }
            }
        }

        return tagTypeIndexSearchResponseDtos;
    }

    /**
     * Flattens out a list of tag entity lists.
     *
     * @param nestedTagEntities the list of tag entity lists
     *
     * @return flattened list of tag entities
     */
    private List<TagEntity> flattenTagEntitiesList(List<List<TagEntity>> nestedTagEntities)
    {
        List<TagEntity> tagEntityList = new ArrayList<>();

        nestedTagEntities.forEach(tagEntityList::addAll);

        return tagEntityList;
    }

    /**
     * Private method to create a String representation of the list of tag entities for logging.
     *
     * @param tagEntityList the list of tag entities
     *
     * @return the String representation of the tag entity list
     */
    private String tagEntityListToString(List<TagEntity> tagEntityList)
    {
        List<String> tagEntityTagCodeAndTagTypeCode = new ArrayList<>();

        tagEntityList.forEach(tagEntity -> tagEntityTagCodeAndTagTypeCode
            .add("TagCode={" + tagEntity.getTagCode() + "} and TagTypeCode={" + tagEntity.getTagType().getCode() + "}"));

        return herdStringHelper.join(tagEntityTagCodeAndTagTypeCode, ",", "\\");
    }

    /**
     * Private method to handle scrolling through the results from the search request and adding them to a business object definition entity list.
     *
     * @param searchRequestBuilder the the search request to scroll through
     *
     * @return list of business object definition entities
     */
    private List<BusinessObjectDefinitionIndexSearchResponseDto> scrollSearchResultsIntoBusinessObjectDefinitionDto(
        final SearchRequestBuilder searchRequestBuilder)
    {
        // Retrieve the search response
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        // Create an array list for storing the BusinessObjectDefinitionEntities
        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();

        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.hits();

        // While there are hits available, page through the results and add them to the id list
        while (hits.length != 0)
        {
            for (SearchHit searchHit : hits)
            {
                String jsonInString = searchHit.getSourceAsString();

                try
                {
                    businessObjectDefinitionIndexSearchResponseDtoList
                        .add(jsonHelper.unmarshallJsonToObject(BusinessObjectDefinitionIndexSearchResponseDto.class, jsonInString));
                }
                catch (IOException ioException)
                {
                    LOGGER.warn("Could not convert JSON document id={} into BusinessObjectDefinition object. ", searchHit.id(), ioException);
                }
            }

            SearchScrollRequestBuilder searchScrollRequestBuilder = transportClient.prepareSearchScroll(searchResponse.getScrollId());
            searchScrollRequestBuilder.setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME));
            searchResponse = searchScrollRequestBuilder.execute().actionGet();
            searchHits = searchResponse.getHits();
            hits = searchHits.hits();
        }

        return businessObjectDefinitionIndexSearchResponseDtoList;
    }

    /**
     * The update index documents function will take as arguments the index name, document type, and a map of documents to update. The document map key is the
     * document id, and the value is the document as a JSON string.
     */
    private final TriConsumer<String, String, Map<String, String>> updateIndexDocumentsFunction = (indexName, documentType, documentMap) -> {
        LOGGER.info("Updating Elasticsearch index documents, indexName={}, documentType={}, documentMap={}.", indexName, documentType,
            Joiner.on(",").withKeyValueSeparator("=").join(documentMap));

        // Prepare a bulk request builder
        final BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

        // For each document prepare an update request and add it to the bulk request builder
        documentMap.forEach((id, jsonString) -> {
            final UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(indexName, documentType, id);
            updateRequestBuilder.setDoc(jsonString);
            bulkRequestBuilder.add(updateRequestBuilder);
        });

        // Execute the bulk update request
        final BulkResponse bulkResponse = bulkRequestBuilder.get();

        // If there are failures log them
        if (bulkResponse.hasFailures())
        {
            LOGGER.error("Bulk response error = {}", bulkResponse.buildFailureMessage());
        }
    };


    @Override
    public QuadConsumer<String, String, String, String> getIndexFunction()
    {
        return indexFunction;
    }

    @Override
    public QuadConsumer<String, String, String, String> getValidateFunction()
    {
        return validateFunction;
    }

    @Override
    public QuadPredicate<String, String, String, String> getIsValidFunction()
    {
        return isValidFunction;
    }

    @Override
    public Predicate<String> getIndexExistsFunction()
    {
        return indexExistsFunction;
    }

    @Override
    public Consumer<String> getDeleteIndexFunction()
    {
        return deleteIndexFunction;
    }

    @Override
    public TriConsumer<String, String, Map<String, String>> getCreateIndexDocumentsFunction()
    {
        return createIndexDocumentsFunction;
    }

    @Override
    public QuadConsumer<String, String, String, String> getCreateIndexFunction()
    {
        return createIndexFunction;
    }

    @Override
    public TriConsumer<String, String, String> getDeleteDocumentByIdFunction()
    {
        return deleteDocumentByIdFunction;
    }

    @Override
    public TriConsumer<String, String, List<Integer>> getDeleteIndexDocumentsFunction()
    {
        return deleteIndexDocumentsFunction;
    }

    @Override
    public BiFunction<String, String, Long> getNumberOfTypesInIndexFunction()
    {
        return numberOfTypesInIndexFunction;
    }

    @Override
    public BiFunction<String, String, List<String>> getIdsInIndexFunction()
    {
        return idsInIndexFunction;
    }

    @Override
    public TriFunction<String, String, Set<String>, ElasticsearchResponseDto> getFindAllBusinessObjectDefinitionsFunction()
    {
        return findAllBusinessObjectDefinitionsFunction;
    }

    @Override
    public QuadFunction<String, String, List<Map<SearchFilterType, List<TagEntity>>>, Set<String>, ElasticsearchResponseDto>
        getSearchBusinessObjectDefinitionsByTagsFunction()
    {
        return searchBusinessObjectDefinitionsByTagsFunction;
    }

    @Override
    public TriConsumer<String, String, Map<String, String>> getUpdateIndexDocumentsFunction()
    {
        return updateIndexDocumentsFunction;
    }
}
