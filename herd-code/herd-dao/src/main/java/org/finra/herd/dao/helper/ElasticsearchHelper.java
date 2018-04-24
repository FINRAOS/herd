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

import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.BooleanUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.FacetTypeEnum;
import org.finra.herd.model.dto.ResultTypeIndexSearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.model.jpa.TagEntity;

@Component
public class ElasticsearchHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchHelper.class);

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    /**
     * Source string for the display name
     */
    public static final String DISPLAY_NAME_SOURCE = "displayName";

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
     * The result type agg
     */
    public static final String RESULT_TYPE_AGGS = "resultType";

    /**
     *
     */
    public static final String RESULT_TYPE_FIELD = "_index";

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
     * @param bdefActiveIndex the active bdef index name
     * @param tagActiveIndex the active tag index name
     *
     * @return boolean query with the filters applied
     */
    public BoolQueryBuilder addIndexSearchFilterBooleanClause(List<IndexSearchFilter> indexSearchFilters, String bdefActiveIndex, String tagActiveIndex)
    {
        BoolQueryBuilder compoundBoolQueryBuilder = new BoolQueryBuilder();
        for (IndexSearchFilter indexSearchFilter : indexSearchFilters)
        {
            BoolQueryBuilder indexSearchFilterClauseBuilder = applySearchFilterClause(indexSearchFilter, bdefActiveIndex, tagActiveIndex);

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
     * @param bdefActiveIndex the name of the bdef active index
     * @param tagActiveIndex the name of the tag active index
     *
     * @return {@link BoolQueryBuilder} the resolved filter query
     */
    private BoolQueryBuilder applySearchFilterClause(IndexSearchFilter indexSearchFilter, String bdefActiveIndex, String tagActiveIndex)
    {
        BoolQueryBuilder indexSearchFilterClauseBuilder = new BoolQueryBuilder();

        for (IndexSearchKey indexSearchKey : indexSearchFilter.getIndexSearchKeys())
        {
            if (indexSearchKey.getTagKey() != null)
            {
                List<TagEntity> tagEntities = new ArrayList<>();
                TagEntity indexSearchTagEntity = tagDaoHelper.getTagEntity(indexSearchKey.getTagKey());

                // If includeTagHierarchy is true, get list of children tag entities down the hierarchy of the specified tag.
                tagEntities.add(indexSearchTagEntity);
                if (BooleanUtils.isTrue(indexSearchKey.isIncludeTagHierarchy()))
                {
                    tagEntities.addAll(tagDaoHelper.getTagChildrenEntities(indexSearchTagEntity));
                }

                // For each tag entity add it to the search query
                for (TagEntity tagEntity : tagEntities)
                {
                    // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                    ConstantScoreQueryBuilder searchKeyQueryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery().should(
                        QueryBuilders.boolQuery().must(QueryBuilders.termQuery(BDEF_TAGTYPE_CODE_FIELD, tagEntity.getTagType().getCode()))
                            .must(QueryBuilders.termQuery(BDEF_TAG_CODE_FIELD, tagEntity.getTagCode()))).should(
                        QueryBuilders.boolQuery().must(QueryBuilders.termQuery(TAG_TAGTYPE_CODE_FIELD, tagEntity.getTagType().getCode()))
                            .must(QueryBuilders.termQuery(TAG_TAG_CODE_FIELD, tagEntity.getTagCode()))));

                    // Individual index search keys are OR-ed
                    indexSearchFilterClauseBuilder.should(searchKeyQueryBuilder);
                }
            }
            if (indexSearchKey.getIndexSearchResultTypeKey() != null)
            {
                String indexSearchResultType = indexSearchKey.getIndexSearchResultTypeKey().getIndexSearchResultType();
                String indexName = indexSearchResultType.equalsIgnoreCase(SearchIndexTypeEntity.SearchIndexTypes.TAG.name()) ? tagActiveIndex : bdefActiveIndex;

                // Add constant-score term queries for tagType-code and tag-code from the tag-key.
                ConstantScoreQueryBuilder searchKeyQueryBuilder =
                    QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(RESULT_TYPE_FIELD, indexName)));

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
        Terms aggregation = getAggregation(searchResponse, RESULT_TYPE_AGGS);

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
     * Creates result type facet response dto
     *
     * @param searchResult search result
     *
     * @return result type facet response dto list
     */
    public List<ResultTypeIndexSearchResponseDto> getResultTypeIndexSearchResponseDto(SearchResult searchResult)
    {
        MetricAggregation metricAggregation = searchResult.getAggregations();
        TermsAggregation resultTypeAggregation = metricAggregation.getTermsAggregation(RESULT_TYPE_AGGS);

        List<TermsAggregation.Entry> buckets = resultTypeAggregation.getBuckets();

        List<ResultTypeIndexSearchResponseDto> resultTypeIndexSearchResponseDtos = new ArrayList<>();

        for (TermsAggregation.Entry entry : buckets)
        {
            ResultTypeIndexSearchResponseDto dto = new ResultTypeIndexSearchResponseDto();
            dto.setResultTypeCode(entry.getKeyAsString());
            dto.setResultTypeDisplayName(entry.getKeyAsString());
            dto.setCount(entry.getCount());
            resultTypeIndexSearchResponseDtos.add(dto);
        }

        return resultTypeIndexSearchResponseDtos;
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
                new TagTypeIndexSearchResponseDto(tagTypeCodeEntry.getKeyAsString(), tagIndexSearchResponseDtos, null);
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
        Terms aggregation = getAggregation(searchResponse, TAG_TYPE_FACET_AGGS);

        return getTagTypeIndexSearchResponseDtosFromTerms(aggregation);
    }

    /**
     * get tag index search response dto
     *
     * @param searchResult elastic search result
     *
     * @return list of tag type index search response dto
     */
    public List<TagTypeIndexSearchResponseDto> getTagTagIndexSearchResponseDto(SearchResult searchResult)
    {
        MetricAggregation metricAggregation = searchResult.getAggregations();
        TermsAggregation tagTypeFacetAggregation = metricAggregation.getTermsAggregation(TAG_TYPE_FACET_AGGS);
        return getTagTypeIndexSearchResponseDtosFromTermsAggregation(tagTypeFacetAggregation);
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
        Terms tagTypeCodeAgg = getNestedAggregation(searchResponse, TAG_FACET_AGGS, TAGTYPE_CODE_AGGREGATION);

        return getTagTypeIndexSearchResponseDtosFromTerms(tagTypeCodeAgg);
    }

    /**
     * create tag tag index response dto
     *
     * @param searchResult search result
     *
     * @return tag type index search response dto list
     */
    public List<TagTypeIndexSearchResponseDto> getNestedTagTagIndexSearchResponseDto(SearchResult searchResult)
    {
        MetricAggregation metricAggregation = searchResult.getAggregations();
        MetricAggregation tagFacetAggregation = metricAggregation.getSumAggregation(TAG_FACET_AGGS);
        TermsAggregation tagTypeCodesAggregation = tagFacetAggregation.getTermsAggregation(TAGTYPE_CODE_AGGREGATION);
        return getTagTypeIndexSearchResponseDtosFromTermsAggregation(tagTypeCodesAggregation);
    }

    /**
     * get Tag Type index response
     *
     * @param termsAggregation termsAggregation
     *
     * @return list of tag type index search dto
     */
    private List<TagTypeIndexSearchResponseDto> getTagTypeIndexSearchResponseDtosFromTermsAggregation(TermsAggregation termsAggregation)
    {
        List<TermsAggregation.Entry> bucketsL0 = termsAggregation.getBuckets();

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();

        for (TermsAggregation.Entry entryL1 : bucketsL0)
        {
            List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos = new ArrayList<>();

            TermsAggregation termsAggregationL1 = entryL1.getTermsAggregation(TAGTYPE_NAME_AGGREGATION);
            List<TermsAggregation.Entry> bucketsL1 = termsAggregationL1.getBuckets();

            TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto =
                new TagTypeIndexSearchResponseDto(entryL1.getKeyAsString(), tagIndexSearchResponseDtos, null);

            tagTypeIndexSearchResponseDtos.add(tagTypeIndexSearchResponseDto);

            for (TermsAggregation.Entry entryL2 : bucketsL1)
            {
                tagTypeIndexSearchResponseDto.setDisplayName(entryL2.getKeyAsString());
                TermsAggregation entryTermsAggregation = entryL2.getTermsAggregation(TAG_CODE_AGGREGATION);
                List<TermsAggregation.Entry> bucketsL2 = entryTermsAggregation.getBuckets();

                for (TermsAggregation.Entry entryL3 : bucketsL2)
                {
                    TagIndexSearchResponseDto tagIndexSearchResponseDto = new TagIndexSearchResponseDto(entryL3.getKeyAsString(), entryL3.getCount(), null);
                    tagIndexSearchResponseDtos.add(tagIndexSearchResponseDto);
                    TermsAggregation entryEntryTermsAggregation = entryL3.getTermsAggregation(TAG_NAME_AGGREGATION);

                    List<TermsAggregation.Entry> bucketsL3 = entryEntryTermsAggregation.getBuckets();

                    for (TermsAggregation.Entry entryL4 : bucketsL3)
                    {
                        tagIndexSearchResponseDto.setTagDisplayName(entryL4.getKeyAsString());
                    }
                }
            }
        }

        return tagTypeIndexSearchResponseDtos;
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

        return new Facet(tagTypeIndexSearchResponseDto.getDisplayName(), null, FacetTypeEnum.TAG_TYPE.value(), tagTypeIndexSearchResponseDto.getCode(),
            tagFacets);
    }

    /**
     * get the facets in the response
     *
     * @param elasticsearchResponseDto elastic search response dto
     * @param bdefActiveIndex the name of the active index for business object definitions
     * @param tagActiveIndex the name os the active index for tags
     *
     * @return facets in the response dto
     */
    public List<Facet> getFacetsResponse(ElasticsearchResponseDto elasticsearchResponseDto, final String bdefActiveIndex, final String tagActiveIndex)
    {
        List<Facet> facets = new ArrayList<>();

        List<Facet> tagTypeFacets;
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
                String facetId = getSearchIndexType(resultTypeIndexSearchResponseDto.getResultTypeDisplayName(), bdefActiveIndex, tagActiveIndex);
                Facet resultTypeFacet = new Facet(facetId, resultTypeIndexSearchResponseDto.getCount(), FacetTypeEnum.RESULT_TYPE.value(), facetId, null);
                resultTypeFacets.add(resultTypeFacet);
            }
            facets.addAll(resultTypeFacets);
        }

        return facets;
    }

    /**
     * Returns the aggregation that is associated with the specified name. This method also validates that the retrieved aggregation exists.
     *
     * @param searchResponse the response of the search request
     * @param aggregationName the name of the aggregation
     *
     * @return the aggregation
     */
    public Terms getAggregation(SearchResponse searchResponse, String aggregationName)
    {
        // Retrieve the aggregations from the search response.
        Aggregations aggregations = getAggregationsFromSearchResponse(searchResponse);

        // Retrieve the specified aggregation.
        Terms aggregation = aggregations.get(aggregationName);

        // Fail if retrieved aggregation is null.
        if (aggregation == null)
        {
            // Log the error along with the search response contents.
            LOGGER.error("Failed to retrieve \"{}\" aggregation from the search response. searchResponse={}", aggregationName,
                jsonHelper.objectToJson(searchResponse));

            // Throw an exception.
            throw new IllegalStateException("Invalid search result.");
        }

        return aggregation;
    }

    /**
     * Returns the sub-aggregation that is associated with the specified nested aggregation. This method also validates that the retrieved sub-aggregation
     * exists.
     *
     * @param searchResponse the response of the search request
     * @param nestedAggregationName the name of the nested aggregation
     * @param subAggregationName the name of the sub-aggregation
     *
     * @return the aggregation
     */
    public Terms getNestedAggregation(SearchResponse searchResponse, String nestedAggregationName, String subAggregationName)
    {
        // Retrieve the aggregations from the search response.
        Aggregations searchResponseAggregations = getAggregationsFromSearchResponse(searchResponse);

        // Retrieve the nested aggregation.
        Nested nestedAggregation = searchResponseAggregations.get(nestedAggregationName);

        // Fail if the retrieved nested aggregation is null.
        if (nestedAggregation == null)
        {
            // Log the error along with the search response contents.
            LOGGER.error("Failed to retrieve \"{}\" nested aggregation from the search response. searchResponse={}", nestedAggregationName,
                jsonHelper.objectToJson(searchResponse));

            // Throw an exception.
            throw new IllegalStateException("Invalid search result.");
        }

        // Retrieve the aggregations from the nested aggregation.
        Aggregations nestedAggregationAggregations = getAggregationsFromNestedAggregation(nestedAggregation, searchResponse);

        // Retrieve the sub-aggregation.
        Terms subAggregation = nestedAggregationAggregations.get(subAggregationName);

        // Fail if retrieved sub-aggregation is null.
        if (subAggregation == null)
        {
            // Log the error along with the search response contents.
            LOGGER.error("Failed to retrieve \"{}\" sub-aggregation from \"{}\" nested aggregation. searchResponse={} nestedAggregation={}", subAggregationName,
                nestedAggregationName, jsonHelper.objectToJson(searchResponse), jsonHelper.objectToJson(nestedAggregation));

            // Throw an exception.
            throw new IllegalStateException("Invalid search result.");
        }

        return subAggregation;
    }

    /**
     * Return the relative search index type for the index name.
     *
     * @param indexName the name of the index
     * @param bdefActiveIndex the name of the active index for business object definitions
     * @param tagActiveIndex the name os the active index for tags
     *
     * @return the search index type
     */
    public String getSearchIndexType(final String indexName, final String bdefActiveIndex, final String tagActiveIndex)
    {
        String searchIndexType;

        if (indexName.equals(bdefActiveIndex))
        {
            searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();
        }
        else if (indexName.equals(tagActiveIndex))
        {
            searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.TAG.name();
        }
        else
        {
            throw new IllegalStateException(String
                .format("Search result index name \"%s\" does not match any of the active search indexes. bdefActiveIndex=%s tagActiveIndex=%s", indexName,
                    bdefActiveIndex, tagActiveIndex));
        }

        return searchIndexType;
    }

    /**
     * Returns a representation of a set of aggregations from the nested aggregation. This method also validates that the retrieved object is not null.
     *
     * @param nestedAggregation the nested aggregation
     * @param searchResponse the response of the search request
     *
     * @return the aggregations
     */
    protected Aggregations getAggregationsFromNestedAggregation(Nested nestedAggregation, SearchResponse searchResponse)
    {
        // Retrieve the aggregations.
        Aggregations aggregations = nestedAggregation.getAggregations();

        // Fail if the retrieved object is null.
        if (aggregations == null)
        {
            // Log the error along with the nested aggregation contents.
            LOGGER.error("Failed to retrieve aggregations from the nested aggregation. searchResponse={} nestedAggregation={}",
                jsonHelper.objectToJson(searchResponse), jsonHelper.objectToJson(nestedAggregation));

            // Throw an exception.
            throw new IllegalStateException("Invalid search result.");
        }

        return aggregations;
    }

    /**
     * Returns a representation of a set of aggregations from the search response. This method also validates that the retrieved object is not null.
     *
     * @param searchResponse the response of the search request
     *
     * @return the aggregations
     */
    protected Aggregations getAggregationsFromSearchResponse(SearchResponse searchResponse)
    {
        // Retrieve the aggregations.
        Aggregations aggregations = searchResponse.getAggregations();

        // Fail if the retrieved object is null.
        if (aggregations == null)
        {
            // Log the error along with the search response contents.
            LOGGER.error("Failed to retrieve aggregations from the search response. searchResponse={}", jsonHelper.objectToJson(searchResponse));

            // Throw an exception.
            throw new IllegalStateException("Invalid search result.");
        }

        return aggregations;
    }
}
