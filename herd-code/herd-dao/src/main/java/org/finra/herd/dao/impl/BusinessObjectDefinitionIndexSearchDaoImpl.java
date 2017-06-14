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

import static org.finra.herd.dao.SearchFilterType.EXCLUSION_SEARCH_FILTER;
import static org.finra.herd.dao.SearchFilterType.INCLUSION_SEARCH_FILTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDefinitionIndexSearchDao;
import org.finra.herd.dao.SearchFilterType;
import org.finra.herd.dao.helper.ElasticsearchClientImpl;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JestClientHelper;
import org.finra.herd.model.dto.BusinessObjectDefinitionIndexSearchResponseDto;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.TagEntity;


@Repository
public class BusinessObjectDefinitionIndexSearchDaoImpl implements BusinessObjectDefinitionIndexSearchDao
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
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDefinitionIndexSearchDaoImpl.class);

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
     * scroll id
     */
    public static final String SCROLL_ID = "_scroll_id";

    /**
     * A helper class for working with Strings
     */
    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * elastic search helper
     */
    @Autowired
    private ElasticsearchHelper elasticsearchHelper;

    /**
     * jest client helper
     */
    @Autowired
    private JestClientHelper jestClientHelper;

    @Override
    public ElasticsearchResponseDto searchBusinessObjectDefinitionsByTags(String indexName, String documentType,
        List<Map<SearchFilterType, List<TagEntity>>> nestedTagEntityMaps, Set<String> facetFieldsList)
    {
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
        SearchRequestBuilder searchRequestBuilder =  new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName);

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
    }

    @Override
    public ElasticsearchResponseDto findAllBusinessObjectDefinitions(String indexName, String documentType, Set<String> facetFieldsList)
    {

        LOGGER.info("Elasticsearch get all business object definition documents from index, indexName={} and documentType={}.", indexName, documentType);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
            .fetchSource(new String[] {DATA_PROVIDER_NAME_SOURCE, DESCRIPTION_SOURCE, DISPLAY_NAME_SOURCE, NAME_SOURCE, NAMESPACE_CODE_SOURCE}, null);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        // Create a search request and set the scroll time and scroll size
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClientImpl(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName);

        searchRequestBuilder.setTypes(documentType)
           // .setScroll(new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME))
            //.setSize(ELASTIC_SEARCH_SCROLL_PAGE_SIZE)
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
    }

    /**
     * Private method to handle scrolling through the results from the search request and adding them to a business object definition entity list.
     *
     * @param searchRequestBuilder the the search request to scroll through
     *
     * @return list of business object definition entities
     * @throws Exception
     */
    private List<BusinessObjectDefinitionIndexSearchResponseDto> scrollSearchResultsIntoBusinessObjectDefinitionDto(
        final SearchRequestBuilder searchRequestBuilder)
    {
        // Retrieve the search response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString());

        searchBuilder.setParameter(Parameters.SIZE, ELASTIC_SEARCH_SCROLL_PAGE_SIZE);
        searchBuilder.setParameter(Parameters.SCROLL, new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME).toString());

        JestResult jestResult = jestClientHelper.searchExecute(searchBuilder.build());

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        List<BusinessObjectDefinitionIndexSearchResponseDto> resultList =
                jestResult.getSourceAsObjectList(BusinessObjectDefinitionIndexSearchResponseDto.class);

        while (resultList.size() != 0)
        {
            businessObjectDefinitionIndexSearchResponseDtoList.addAll(resultList);
            String scrollId = jestResult.getJsonObject().get(SCROLL_ID).getAsString();
            SearchScroll scroll = new SearchScroll.Builder(scrollId, new TimeValue(ELASTIC_SEARCH_SCROLL_KEEP_ALIVE_TIME).toString()).build();
            jestResult = jestClientHelper.searchScrollExecute(scroll);
            resultList =
                    jestResult.getSourceAsObjectList(BusinessObjectDefinitionIndexSearchResponseDto.class);
        }

        return businessObjectDefinitionIndexSearchResponseDtoList;
    }

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

    private List<TagTypeIndexSearchResponseDto> searchResponseIntoFacetInformation(final SearchRequestBuilder searchRequestBuilder)
    {
        // Retrieve the search response
        final Search.Builder searchBuilder = new Search.Builder(searchRequestBuilder.toString());
        SearchResult searchResult = jestClientHelper.searchExecute(searchBuilder.build());
        return elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(searchResult);
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
}
