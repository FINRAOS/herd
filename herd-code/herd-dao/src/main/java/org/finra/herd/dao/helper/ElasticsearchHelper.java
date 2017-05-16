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

package org.finra.herd.dao.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.BooleanUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.FacetTypeEnum;
import org.finra.herd.model.dto.ResultTypeIndexSearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;

@Component
public class ElasticsearchHelper
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
     * The nested path of business object definition tags
     */
    public static final String NESTED_BDEFTAGS_PATH = "businessObjectDefinitionTags.tag";

    /**
     * The tag type code field from tag index
     */
    public static final String TAGTYPE_CODE_FIELD_TAG_INDEX = "tagType.code.keyword";

    /**
     * The tag type display name field from tag index
     */
    public static final String TAGTYPE_NAME_FIELD_TAG_INDEX = "tagType.displayName.keyword";

    /**
     * The tag code field from tag index
     */
    public static final String TAG_CODE_FIELD_TAG_INDEX = "tagCode.keyword";

    /**
     * The tag display name field from tag index
     */
    public static final String TAG_NAME_FIELD_TAG_INDEX = "displayName.keyword";

    /**
     * The tag type code field
     */
    public static final String BDEF_TAGTYPE_CODE_FIELD = NESTED_BDEFTAGS_PATH + ".tagType.code.keyword";

    /**
     * The tag type display name field
     */
    public static final String BDEF_TAGTYPE_NAME_FIELD = NESTED_BDEFTAGS_PATH + ".tagType.displayName.keyword";

    /**
     * The tag code field
     */
    public static final String BDEF_TAG_CODE_FIELD = NESTED_BDEFTAGS_PATH + ".tagCode.keyword";

    /**
     * The tag display name field
     */
    public static final String BDEF_TAG_NAME_FIELD = NESTED_BDEFTAGS_PATH + ".displayName.keyword";

    /**
     * The tagCode field
     */
    public static final String TAG_TAG_CODE_FIELD = "tagCode.keyword";

    /**
     * The tag type code field
     */
    public static final String TAG_TAGTYPE_CODE_FIELD = "tagType.code.keyword";

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
     * the result type Facet
     */
    public static final String RESULT_TYPE_FACET = "resulttype";

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
     * The result type agg
     */
    public static final String RESULT_TYPE_AGGS = "resultType";

    /**
     *
     */
    public static final String RESULT_TYPE_FIELD = "_index";

    /**
     * namespace field
     */
    public static final String NAMESPACE_FIELD = "namespace.code.keyword";

    /**
     * business object definition name field
     */
    public static final String BDEF_NAME_FIELD = "name.keyword";

    /**
     * business object definition result type
     */
    public static final String BUS_OBJCT_DFNTN_RESULT_TYPE = "bdef";

    /**
     * tag result type
     */
    public static final String TAG_RESULT_TYPE = "tag";

    /**
     * Adds facet field aggregations
     *
     * @param facetFieldsList facet field list
     * @param searchRequestBuilder search request builder
     *
     * @return the specified search request builder with the aggregations applied to it
     */
    public SearchRequestBuilder addFacetFieldAggregations(Set<String> facetFieldsList, SearchRequestBuilder searchRequestBuilder)
    {
        if (CollectionUtils.isNotEmpty(facetFieldsList))
        {
            if (facetFieldsList.contains(TAG_FACET))
            {
                searchRequestBuilder.addAggregation(AggregationBuilders.nested(TAG_FACET_AGGS, NESTED_BDEFTAGS_PATH).subAggregation(
                    AggregationBuilders.terms(TAGTYPE_CODE_AGGREGATION).field(BDEF_TAGTYPE_CODE_FIELD).subAggregation(
                        AggregationBuilders.terms(TAGTYPE_NAME_AGGREGATION).field(BDEF_TAGTYPE_NAME_FIELD).subAggregation(
                            AggregationBuilders.terms(TAG_CODE_AGGREGATION).field(BDEF_TAG_CODE_FIELD)
                                .subAggregation(AggregationBuilders.terms(TAG_NAME_AGGREGATION).field(BDEF_TAG_NAME_FIELD))))));

                searchRequestBuilder.addAggregation(AggregationBuilders.terms(TAG_TYPE_FACET_AGGS).field(TAGTYPE_CODE_FIELD_TAG_INDEX).subAggregation(
                    AggregationBuilders.terms(TAGTYPE_NAME_AGGREGATION).field(TAGTYPE_NAME_FIELD_TAG_INDEX).subAggregation(
                        AggregationBuilders.terms(TAG_CODE_AGGREGATION).field(TAG_CODE_FIELD_TAG_INDEX)
                            .subAggregation(AggregationBuilders.terms(TAG_NAME_AGGREGATION).field(TAG_NAME_FIELD_TAG_INDEX)))));

            }
            if (facetFieldsList.contains(RESULT_TYPE_FACET))
            {
                searchRequestBuilder.addAggregation(AggregationBuilders.terms(RESULT_TYPE_AGGS).field(RESULT_TYPE_FIELD));
            }
        }

        return searchRequestBuilder;
    }

    /**
     * Navigates the specified index search filters and adds boolean filter clauses to a given {@link SearchRequestBuilder}
     *
     * @param indexSearchFilters the specified search filters
     *
     * @return boolean query with the filters applied
     */
    public BoolQueryBuilder addIndexSearchFilterBooleanClause(List<IndexSearchFilter> indexSearchFilters)
    {
        BoolQueryBuilder compoundBoolQueryBuilder = new BoolQueryBuilder();
        for (IndexSearchFilter indexSearchFilter : indexSearchFilters)
        {
            BoolQueryBuilder indexSearchFilterClauseBuilder = applySearchFilterClause(indexSearchFilter);

            // If the search filter is marked with the exclusion flag then apply the entire compound filter clause on the request builder within a MUST NOT
            // clause.
            if (BooleanUtils.isTrue(indexSearchFilter.isIsExclusionSearchFilter()))
            {
                compoundBoolQueryBuilder.mustNot(indexSearchFilterClauseBuilder);
            }
            else
            {
                // Individual search filters are AND-ed (the compound filter clause is applied on the search request builder within a MUST clause)
                compoundBoolQueryBuilder.must(indexSearchFilterClauseBuilder);
            }
        }

        return compoundBoolQueryBuilder;
    }

    /**
     * Resolves the search filters into an Elasticsearch {@link BoolQueryBuilder}
     *
     * @param indexSearchFilter the specified search filter
     *
     * @return {@link BoolQueryBuilder} the resolved filter query
     */
    private BoolQueryBuilder applySearchFilterClause(IndexSearchFilter indexSearchFilter)
    {
        BoolQueryBuilder indexSearchFilterClauseBuilder = new BoolQueryBuilder();

        for (IndexSearchKey indexSearchKey : indexSearchFilter.getIndexSearchKeys())
        {
            if (null != indexSearchKey.getTagKey())
            {
                // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                ConstantScoreQueryBuilder searchKeyQueryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery().should(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BDEF_TAGTYPE_CODE_FIELD, indexSearchKey.getTagKey().getTagTypeCode()))
                        .must(QueryBuilders.termQuery(BDEF_TAG_CODE_FIELD, indexSearchKey.getTagKey().getTagCode()))).should(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery(TAG_TAGTYPE_CODE_FIELD, indexSearchKey.getTagKey().getTagTypeCode()))
                        .must(QueryBuilders.termQuery(TAG_TAG_CODE_FIELD, indexSearchKey.getTagKey().getTagCode()))));

                // Individual index search keys are OR-ed
                indexSearchFilterClauseBuilder.should(searchKeyQueryBuilder);
            }
            if (null != indexSearchKey.getIndexSearchResultTypeKey())
            {
                // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                ConstantScoreQueryBuilder searchKeyQueryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(RESULT_TYPE_FIELD, indexSearchKey.getIndexSearchResultTypeKey().getIndexSearchResultType().toLowerCase())));

                // Individual index search keys are OR-ed
                indexSearchFilterClauseBuilder.should(searchKeyQueryBuilder);
            }
        }

        return indexSearchFilterClauseBuilder;
    }

    /**
     * Creates result type facet response dto
     *
     * @param searchResponse search response
     *
     * @return result type facet response dto list
     */
    public List<ResultTypeIndexSearchResponseDto> getResultTypeIndexSearchResponseDto(SearchResponse searchResponse)
    {
        List<ResultTypeIndexSearchResponseDto> list = new ArrayList<>();
        Terms aggregation = searchResponse.getAggregations().get(RESULT_TYPE_AGGS);

        for (Terms.Bucket resultTypeEntry : aggregation.getBuckets())
        {
            ResultTypeIndexSearchResponseDto dto = new ResultTypeIndexSearchResponseDto();
            dto.setResultTypeCode(resultTypeEntry.getKeyAsString());
            dto.setResultTypeDisplayName(resultTypeEntry.getKeyAsString());
            dto.setCount(resultTypeEntry.getDocCount());
            list.add(dto);
        }

        return list;
    }

    /**
     * get Tag Type index response
     *
     * @param aggregation aggregation
     *
     * @return list of tag type index search dto
     */
    private List<TagTypeIndexSearchResponseDto> getTagTypeIndexSearchResponseDtosFromTerms(Terms aggregation)
    {
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();

        for (Terms.Bucket tagTypeCodeEntry : aggregation.getBuckets())
        {
            List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos = new ArrayList<>();

            TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto =
                new TagTypeIndexSearchResponseDto(tagTypeCodeEntry.getKeyAsString(), tagTypeCodeEntry.getDocCount(), tagIndexSearchResponseDtos, null);
            tagTypeIndexSearchResponseDtos.add(tagTypeIndexSearchResponseDto);

            Terms tagTypeDisplayNameAggs = tagTypeCodeEntry.getAggregations().get(TAGTYPE_NAME_AGGREGATION);
            for (Terms.Bucket tagTypeDisplayNameEntry : tagTypeDisplayNameAggs.getBuckets())
            {
                tagTypeIndexSearchResponseDto.setDisplayName(tagTypeDisplayNameEntry.getKeyAsString());

                Terms tagCodeAggs = tagTypeDisplayNameEntry.getAggregations().get(TAG_CODE_AGGREGATION);
                TagIndexSearchResponseDto tagIndexSearchResponseDto;

                for (Terms.Bucket tagCodeEntry : tagCodeAggs.getBuckets())
                {
                    tagIndexSearchResponseDto = new TagIndexSearchResponseDto(tagCodeEntry.getKeyAsString(), tagCodeEntry.getDocCount(), null);
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
     * get tag index search response dto
     *
     * @param searchResponse elastic search response
     *
     * @return list of tag type index search response dto
     */
    public List<TagTypeIndexSearchResponseDto> getTagTagIndexSearchResponseDto(SearchResponse searchResponse)
    {
        Terms aggregation = searchResponse.getAggregations().get(TAG_TYPE_FACET_AGGS);

        return getTagTypeIndexSearchResponseDtosFromTerms(aggregation);
    }

    /**
     * create tag tag index response dto
     *
     * @param searchResponse search response
     *
     * @return tag type index search response dto list
     */
    public List<TagTypeIndexSearchResponseDto> getNestedTagTagIndexSearchResponseDto(SearchResponse searchResponse)
    {
        Nested aggregation = searchResponse.getAggregations().get(TAG_FACET_AGGS);
        Terms tagTypeCodeAgg = aggregation.getAggregations().get(TAGTYPE_CODE_AGGREGATION);

        return getTagTypeIndexSearchResponseDtosFromTerms(tagTypeCodeAgg);

    }

    /**
     * create tag index search response facet
     *
     * @param tagTypeIndexSearchResponseDto response dto
     *
     * @return tag type facet
     */
    private Facet createTagTypeFacet(TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto)
    {
        List<Facet> tagFacets = new ArrayList<>();

        if (tagTypeIndexSearchResponseDto.getTagIndexSearchResponseDtos() != null)
        {
            for (TagIndexSearchResponseDto tagIndexSearchResponseDto : tagTypeIndexSearchResponseDto.getTagIndexSearchResponseDtos())
            {
                long facetCount = tagIndexSearchResponseDto.getCount();
                Facet tagFacet =
                    new Facet(tagIndexSearchResponseDto.getTagDisplayName(), facetCount, FacetTypeEnum.TAG.value(), tagIndexSearchResponseDto.getTagCode(),
                        null);
                tagFacets.add(tagFacet);
            }
        }

        long facetCount = tagTypeIndexSearchResponseDto.getCount();
        return new Facet(tagTypeIndexSearchResponseDto.getDisplayName(), facetCount, FacetTypeEnum.TAG_TYPE.value(), tagTypeIndexSearchResponseDto.getCode(),
            tagFacets);
    }

    /**
     * get the facets in the response
     *
     * @param elasticsearchResponseDto elastic search response dto
     * @param includingTagInCount if include tag in the facet count
     *
     * @return facets in the response dto
     */
    public List<Facet> getFacetsResponse(ElasticsearchResponseDto elasticsearchResponseDto, boolean includingTagInCount)
    {
        List<Facet> facets = new ArrayList<>();

        List<Facet> tagTypeFacets = null;
        if (elasticsearchResponseDto.getNestTagTypeIndexSearchResponseDtos() != null)
        {
            tagTypeFacets = new ArrayList<>();
            //construct a list of facet information
            for (TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto : elasticsearchResponseDto.getNestTagTypeIndexSearchResponseDtos())
            {
                tagTypeFacets.add(createTagTypeFacet(tagTypeIndexSearchResponseDto));
            }

            facets.addAll(tagTypeFacets);
        }

        if (elasticsearchResponseDto.getTagTypeIndexSearchResponseDtos() != null)
        {
            for (TagTypeIndexSearchResponseDto tagTypeIndexDto : elasticsearchResponseDto.getTagTypeIndexSearchResponseDtos())
            {
                boolean foundMatchingTagType = false;
                for (Facet tagFacet : facets)
                {
                    if (tagFacet.getFacetId().equals(tagTypeIndexDto.getCode()))
                    {
                        foundMatchingTagType = true;
                        boolean foundMatchingTagCode = false;
                        for (TagIndexSearchResponseDto tagIndexDto : tagTypeIndexDto.getTagIndexSearchResponseDtos())
                        {
                            for (Facet nestedTagIndexDto : tagFacet.getFacets())
                            {
                                if (tagIndexDto.getTagCode().equals(nestedTagIndexDto.getFacetId()))
                                {
                                    foundMatchingTagCode = true;
                                    //add one to the facet count because the tag itself is in the result
                                    nestedTagIndexDto.setFacetCount(nestedTagIndexDto.getFacetCount() + 1);
                                }
                            }
                            if (!foundMatchingTagCode)
                            {
                                tagFacet.getFacets().add(
                                    new Facet(tagIndexDto.getTagDisplayName(), tagIndexDto.getCount(), FacetTypeEnum.TAG.value(), tagIndexDto.getTagCode(),
                                        null));
                            }
                        }
                    }
                }
                if (!foundMatchingTagType)
                {
                    facets.add(createTagTypeFacet(tagTypeIndexDto));
                }
            }
        }

        if (elasticsearchResponseDto.getResultTypeIndexSearchResponseDtos() != null)
        {
            List<Facet> resultTypeFacets = new ArrayList<>();
            //construct a list of facet information
            for (ResultTypeIndexSearchResponseDto resultTypeIndexSearchResponseDto : elasticsearchResponseDto.getResultTypeIndexSearchResponseDtos())
            {
                Facet resultTypeFacet = new Facet(resultTypeIndexSearchResponseDto.getResultTypeDisplayName(), resultTypeIndexSearchResponseDto.getCount(),
                    FacetTypeEnum.RESULT_TYPE.value(), resultTypeIndexSearchResponseDto.getResultTypeCode(), null);
                resultTypeFacets.add(resultTypeFacet);
            }
            facets.addAll(resultTypeFacets);
        }

        return facets;
    }
}
