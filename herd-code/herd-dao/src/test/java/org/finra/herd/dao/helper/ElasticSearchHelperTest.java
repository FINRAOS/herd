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

import static org.finra.herd.dao.helper.ElasticsearchHelper.RESULT_TYPE_AGGS;
import static org.finra.herd.dao.helper.ElasticsearchHelper.RESULT_TYPE_FACET;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAGTYPE_CODE_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAGTYPE_NAME_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_CODE_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_FACET;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_FACET_AGGS;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_NAME_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_TYPE_FACET_AGGS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.SumAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.IndexSearchFilter;
import org.finra.herd.model.api.xml.IndexSearchKey;
import org.finra.herd.model.api.xml.IndexSearchResultTypeKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.FacetTypeEnum;
import org.finra.herd.model.dto.ResultTypeIndexSearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

public class ElasticSearchHelperTest extends AbstractDaoTest
{
    public static final String INDEX_SEARCH_RESULT_TYPE = "Index Search Result Type 1";

    public static final String TAG_TYPE_CODE = "Tag Type Code 1";

    public static final String TAG_TYPE_CODE_2 = "Tag Type Code 2";

    public static final String TAG_TYPE_CODE_3 = "Tag Type Code 3";

    public static final String TAG_TYPE_DISPLAY_NAME = "Tag Type DisplayName";

    public static final String TAG_TYPE_DISPLAY_NAME_2 = "Tag Type DisplayName 2";

    public static final String TAG_TYPE_DISPLAY_NAME_3 = "Tag Type DisplayName 3";

    public static final String TAG_CODE = "Tag Code 1";

    public static final String TAG_CODE_2 = "Tag Code 2";

    public static final int TAG_CODE_COUNT = 1;

    public static final String TAG_CODE_DISPLAY_NAME = "Tag Code DisplayName";

    public static final String TAG_CODE_DISPLAY_NAME_2 = "Tag Code DisplayName 2";

    @InjectMocks
    private ElasticsearchHelper elasticsearchHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private TagDaoHelper tagDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetNestedTagTagIndexSearchResponseDtoSearchResponseParameter()
    {
        SearchResponse searchResponse = mock(SearchResponse.class);
        Aggregations aggregations = mock(Aggregations.class);

        when(searchResponse.getAggregations()).thenReturn(aggregations);

        Nested nestedAggregation = mock(Nested.class);
        when(aggregations.get(TAG_FACET_AGGS)).thenReturn(nestedAggregation);

        Aggregations aggregationAggregations = mock(Aggregations.class);
        when(nestedAggregation.getAggregations()).thenReturn(aggregationAggregations);

        Terms subAggregation = mock(Terms.class);
        when(aggregationAggregations.get(TAGTYPE_CODE_AGGREGATION)).thenReturn(subAggregation);

        List<TagTypeIndexSearchResponseDto> result = elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(searchResponse);
        assertThat("Result is null.", result, is(notNullValue()));
    }

    @Test
    public void testGetNestedTagTagIndexSearchResponseDtoSearchResultParameter()
    {
        SearchResult searchResult = mock(SearchResult.class);
        MetricAggregation metricAggregation = mock(MetricAggregation.class);
        SumAggregation tagFacetAggregation = mock(SumAggregation.class);
        TermsAggregation tagTypeCodesAggregation = mock(TermsAggregation.class);

        when(searchResult.getAggregations()).thenReturn(metricAggregation);
        when(metricAggregation.getSumAggregation(TAG_FACET_AGGS)).thenReturn(tagFacetAggregation);
        when(tagFacetAggregation.getTermsAggregation(TAGTYPE_CODE_AGGREGATION)).thenReturn(tagTypeCodesAggregation);

        List<TagTypeIndexSearchResponseDto> result = elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(searchResult);
        assertThat("Result is null.", result, is(notNullValue()));
    }

    @Test
    public void testGetTagTagIndexSearchResponseDtoSearchResult()
    {
        SearchResult searchResult = mock(SearchResult.class);
        MetricAggregation metricAggregation = mock(MetricAggregation.class);
        TermsAggregation termsAggregation = mock(TermsAggregation.class);

        when(searchResult.getAggregations()).thenReturn(metricAggregation);
        when(metricAggregation.getTermsAggregation(TAG_TYPE_FACET_AGGS)).thenReturn(termsAggregation);

        List<TermsAggregation.Entry> buckets = new ArrayList<>();
        TermsAggregation.Entry entryL1 = mock(TermsAggregation.Entry.class);
        buckets.add(entryL1);
        when(termsAggregation.getBuckets()).thenReturn(buckets);

        TermsAggregation termsAggregationL1 = mock(TermsAggregation.class);
        when(entryL1.getTermsAggregation(TAGTYPE_NAME_AGGREGATION)).thenReturn(termsAggregationL1);

        List<TermsAggregation.Entry> bucketsL1 = new ArrayList<>();
        TermsAggregation.Entry entryL2 = mock(TermsAggregation.Entry.class);
        bucketsL1.add(entryL2);
        when(termsAggregationL1.getBuckets()).thenReturn(bucketsL1);

        TermsAggregation entryTermsAggregation = mock(TermsAggregation.class);
        when(entryL2.getTermsAggregation(TAG_CODE_AGGREGATION)).thenReturn(entryTermsAggregation);

        List<TermsAggregation.Entry> bucketsL2 = new ArrayList<>();
        TermsAggregation.Entry entryL3 = mock(TermsAggregation.Entry.class);
        bucketsL2.add(entryL3);
        when(entryTermsAggregation.getBuckets()).thenReturn(bucketsL2);

        TermsAggregation entryEntryTermsAggregation = mock(TermsAggregation.class);
        when(entryL3.getTermsAggregation(TAG_NAME_AGGREGATION)).thenReturn(entryEntryTermsAggregation);

        List<TermsAggregation.Entry> bucketsL3 = new ArrayList<>();
        TermsAggregation.Entry entryL4 = mock(TermsAggregation.Entry.class);
        bucketsL3.add(entryL4);
        when(entryEntryTermsAggregation.getBuckets()).thenReturn(bucketsL3);

        List<TagTypeIndexSearchResponseDto> result = elasticsearchHelper.getTagTagIndexSearchResponseDto(searchResult);
        assertThat("Result is null.", result, is(notNullValue()));
    }

    @Test
    public void testGetResultTypeIndexSearchResponseDtoSearchResult()
    {
        SearchResult searchResult = mock(SearchResult.class);
        MetricAggregation metricAggregation = mock(MetricAggregation.class);
        TermsAggregation termsAggregation = mock(TermsAggregation.class);
        List<TermsAggregation.Entry> buckets = new ArrayList<>();
        buckets.add(new TermsAggregation("TermAggregation", new JsonObject()).new Entry(new JsonObject(), "key", 1L));

        when(searchResult.getAggregations()).thenReturn(metricAggregation);
        when(metricAggregation.getTermsAggregation(RESULT_TYPE_AGGS)).thenReturn(termsAggregation);
        when(termsAggregation.getBuckets()).thenReturn(buckets);

        List<ResultTypeIndexSearchResponseDto> result = elasticsearchHelper.getResultTypeIndexSearchResponseDto(searchResult);
        assertThat("Result is null.", result, is(notNullValue()));
    }

    @Test
    public void testAddIndexSearchFilterBooleanClause()
    {
        TagKey tagKey = new TagKey();
        tagKey.setTagCode(TAG_CODE);
        tagKey.setTagTypeCode(TAG_TYPE_CODE);

        IndexSearchResultTypeKey indexSearchResultTypeKey = new IndexSearchResultTypeKey();
        indexSearchResultTypeKey.setIndexSearchResultType(INDEX_SEARCH_RESULT_TYPE);

        List<IndexSearchKey> indexSearchKeys = new ArrayList<>();
        IndexSearchKey indexSearchKey = new IndexSearchKey();
        indexSearchKey.setTagKey(tagKey);
        indexSearchKey.setIndexSearchResultTypeKey(indexSearchResultTypeKey);
        indexSearchKeys.add(indexSearchKey);

        List<IndexSearchFilter> indexSearchFilters = new ArrayList<>();
        IndexSearchFilter indexSearchFilter1 = new IndexSearchFilter();
        indexSearchFilter1.setIsExclusionSearchFilter(true);
        indexSearchFilter1.setIndexSearchKeys(indexSearchKeys);

        IndexSearchFilter indexSearchFilter2 = new IndexSearchFilter();
        indexSearchFilter2.setIsExclusionSearchFilter(false);
        indexSearchFilter2.setIndexSearchKeys(indexSearchKeys);

        indexSearchFilters.add(indexSearchFilter1);
        indexSearchFilters.add(indexSearchFilter2);

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagCode());

        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());
        tagEntity.setTagType(tagTypeEntity);

        // Setup the when clauses
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);

        BoolQueryBuilder result = elasticsearchHelper.addIndexSearchFilterBooleanClause(indexSearchFilters, "bdefIndex", "tagIndex");
        assertThat("Result is null.", result, is(notNullValue()));

        // Verify the when clauses
        verify(tagDaoHelper, times(2)).getTagEntity(tagKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testAddIndexSearchFilterBooleanClauseWithTagHierarchy()
    {
        TagKey tagKey = new TagKey();
        tagKey.setTagCode(TAG_CODE);
        tagKey.setTagTypeCode(TAG_TYPE_CODE);

        TagKey childTagKey = new TagKey();
        childTagKey.setTagCode(TAG_CODE_2);
        childTagKey.setTagTypeCode(TAG_TYPE_CODE);

        IndexSearchResultTypeKey indexSearchResultTypeKey = new IndexSearchResultTypeKey();
        indexSearchResultTypeKey.setIndexSearchResultType(INDEX_SEARCH_RESULT_TYPE);

        List<IndexSearchKey> indexSearchKeys = new ArrayList<>();
        IndexSearchKey indexSearchKey = new IndexSearchKey();
        indexSearchKey.setIncludeTagHierarchy(true);
        indexSearchKey.setTagKey(tagKey);
        indexSearchKey.setIndexSearchResultTypeKey(indexSearchResultTypeKey);
        indexSearchKeys.add(indexSearchKey);

        List<IndexSearchFilter> indexSearchFilters = new ArrayList<>();
        IndexSearchFilter indexSearchFilter1 = new IndexSearchFilter();
        indexSearchFilter1.setIsExclusionSearchFilter(true);
        indexSearchFilter1.setIndexSearchKeys(indexSearchKeys);

        IndexSearchFilter indexSearchFilter2 = new IndexSearchFilter();
        indexSearchFilter2.setIsExclusionSearchFilter(false);
        indexSearchFilter2.setIndexSearchKeys(indexSearchKeys);

        indexSearchFilters.add(indexSearchFilter1);
        indexSearchFilters.add(indexSearchFilter2);

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(tagKey.getTagCode());

        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(tagKey.getTagCode());
        tagEntity.setTagType(tagTypeEntity);

        TagEntity childTagEntity = new TagEntity();
        childTagEntity.setTagCode(childTagKey.getTagCode());
        childTagEntity.setTagType(tagTypeEntity);
        childTagEntity.setParentTagEntity(tagEntity);

        tagEntity.setChildrenTagEntities(Lists.newArrayList(childTagEntity));

        // Setup the when clauses
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);
        when(tagDaoHelper.getTagChildrenEntities(tagEntity)).thenReturn(Lists.newArrayList(childTagEntity));

        BoolQueryBuilder result = elasticsearchHelper.addIndexSearchFilterBooleanClause(indexSearchFilters, "bdefIndex", "tagIndex");
        assertThat("Result is null.", result, is(notNullValue()));

        // Verify the when clauses
        verify(tagDaoHelper, times(2)).getTagEntity(tagKey);
        verify(tagDaoHelper, times(2)).getTagChildrenEntities(tagEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testAddFacetFieldAggregationsWithFacetFields()
    {
        Set<String> facetFieldsList = new HashSet<>();
        facetFieldsList.add(TAG_FACET);
        facetFieldsList.add(RESULT_TYPE_FACET);
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SearchRequestBuilder result = elasticsearchHelper.addFacetFieldAggregations(facetFieldsList, searchRequestBuilder);
        assertThat("Result is null.", result, is(notNullValue()));
    }

    @Test
    public void testGetFacetsResponseWithEmptyResponseDto()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        List<Facet> facets =
            elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        assertThat("Facet size is not equal to 0.", facets.size(), equalTo(0));
    }

    @Test
    public void testGetFacetsResponse()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)),
                TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);

        List<ResultTypeIndexSearchResponseDto> resultTypeIndexSearchResponseDtos = new ArrayList<>();
        resultTypeIndexSearchResponseDtos.add(new ResultTypeIndexSearchResponseDto(TAG_CODE, TAG_COUNT, TAG_SEARCH_INDEX_NAME));
        elasticsearchResponseDto.setResultTypeIndexSearchResponseDtos(resultTypeIndexSearchResponseDtos);

        List<Facet> facets =
            elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, 1L, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        expectedFacets.add(new Facet(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), 120L, FacetTypeEnum.RESULT_TYPE.value(),
            SearchIndexTypeEntity.SearchIndexTypes.TAG.name(), null));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTag()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)),
                TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)),
                TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets =
            elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, 1L, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTagWithNoAssociatedBdefs()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)),
                TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE_2, 1, TAG_CODE_DISPLAY_NAME_2)),
                TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets =
            elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, 1L, FacetTypeEnum.TAG.value(), TAG_CODE, null));
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME_2, 1L, FacetTypeEnum.TAG.value(), TAG_CODE_2, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTagWithNoAssociatedBdefsNewTagType()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)),
                TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 =
            new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_3, Collections.singletonList(new TagIndexSearchResponseDto(TAG_CODE_2, 1, TAG_CODE_DISPLAY_NAME_2)),
                TAG_TYPE_DISPLAY_NAME_3);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets =
            elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, 1L, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        List<Facet> newTagFacet = new ArrayList<>();
        newTagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME_2, 1L, FacetTypeEnum.TAG.value(), TAG_CODE_2, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_3, null, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_3, newTagFacet));

        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetResultTypeIndexSearchResponseDto()
    {
        SearchResponse searchResponse = mock(SearchResponse.class);
        Terms terms = mock(Terms.class);
        Aggregations aggregations = mock(Aggregations.class);
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        List<Terms.Bucket> buckets = Collections.singletonList(bucket);

        when(searchResponse.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(RESULT_TYPE_AGGS)).thenReturn(terms);
        when(terms.getBuckets()).thenReturn(buckets);
        when(bucket.getKeyAsString()).thenReturn(TAG_CODE);
        when(bucket.getDocCount()).thenReturn(TAG_COUNT);

        List<ResultTypeIndexSearchResponseDto> expectedList = new ArrayList<>();
        expectedList.add(new ResultTypeIndexSearchResponseDto(TAG_CODE, TAG_COUNT, TAG_CODE));
        List<ResultTypeIndexSearchResponseDto> resultList = elasticsearchHelper.getResultTypeIndexSearchResponseDto(searchResponse);

        assertEquals(expectedList, resultList);
    }

    @Test
    public void testAddFacetFieldAggregations()
    {
        Set<String> facetSet = new HashSet<>();
        //it is ok to pass a null search request builder, as empty facet set bypass the processing
        elasticsearchHelper.addFacetFieldAggregations(facetSet, null);
    }

    @Test
    public void testGetTagTagIndexSearchResponseDto()
    {
        SearchResponse searchResponse = mock(SearchResponse.class);
        Terms terms = mock(Terms.class);
        Aggregations aggregations = mock(Aggregations.class);
        Terms.Bucket tagTypeCodeEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeCodeEntryList = Collections.singletonList(tagTypeCodeEntry);

        when(searchResponse.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(TAG_TYPE_FACET_AGGS)).thenReturn(terms);
        when(terms.getBuckets()).thenReturn(tagTypeCodeEntryList);
        when(tagTypeCodeEntry.getKeyAsString()).thenReturn(TAG_TYPE_CODE);
        when(tagTypeCodeEntry.getAggregations()).thenReturn(aggregations);

        Terms tagTypeDisplayNameAggs = mock(Terms.class);
        Terms.Bucket tagTypeDisplayNameEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeDisplayNameEntryList = Collections.singletonList(tagTypeDisplayNameEntry);
        when(aggregations.get(TAGTYPE_NAME_AGGREGATION)).thenReturn(tagTypeDisplayNameAggs);
        when(tagTypeDisplayNameEntry.getAggregations()).thenReturn(aggregations);
        when(tagTypeDisplayNameAggs.getBuckets()).thenReturn(tagTypeDisplayNameEntryList);
        when(tagTypeDisplayNameEntry.getKeyAsString()).thenReturn(TAG_TYPE_DISPLAY_NAME);

        StringTerms tagCodeAggs = mock(StringTerms.class);
        StringTerms.Bucket tagCodeEntry = mock(StringTerms.Bucket.class);
        List<Terms.Bucket> tagCodeEntryList = Collections.singletonList(tagCodeEntry);

        when(aggregations.get(TAG_CODE_AGGREGATION)).thenReturn(tagCodeAggs);
        when(tagCodeAggs.getBuckets()).thenReturn(tagCodeEntryList);
        when(tagCodeEntry.getAggregations()).thenReturn(aggregations);
        when(tagCodeEntry.getKeyAsString()).thenReturn(TAG_CODE);
        when(tagCodeEntry.getDocCount()).thenReturn((long) TAG_CODE_COUNT);
        Terms tagNameAggs = mock(Terms.class);
        Terms.Bucket tagNameEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagNameEntryList = Collections.singletonList(tagNameEntry);
        when(tagNameEntry.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(TAG_NAME_AGGREGATION)).thenReturn(tagNameAggs);
        when(tagNameAggs.getBuckets()).thenReturn(tagNameEntryList);
        when(tagNameEntry.getKeyAsString()).thenReturn(TAG_DISPLAY_NAME);

        List<TagTypeIndexSearchResponseDto> resultList = elasticsearchHelper.getTagTagIndexSearchResponseDto(searchResponse);
        List<TagTypeIndexSearchResponseDto> expectedList = new ArrayList<>();
        List<TagIndexSearchResponseDto> expectedTagList = new ArrayList<>();
        expectedTagList.add(new TagIndexSearchResponseDto(TAG_CODE, TAG_CODE_COUNT, TAG_DISPLAY_NAME));
        expectedList.add(new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, expectedTagList, TAG_TYPE_DISPLAY_NAME));

        assertEquals(expectedList, resultList);
    }

    @Test
    public void testGetAggregation()
    {
        // Create a mock aggregation.
        Terms aggregation = mock(Terms.class);

        // Create mock aggregations.
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.get(AGGREGATION_NAME)).thenReturn(aggregation);

        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Call the method under test.
        Terms result = elasticsearchHelper.getAggregation(searchResponse, AGGREGATION_NAME);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(aggregation, result);
    }

    @Test
    public void testGetAggregationAggregationIsNull()
    {
        // Create mock aggregations.
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.get(AGGREGATION_NAME)).thenReturn(null);

        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Mock the external calls.
        when(jsonHelper.objectToJson(searchResponse)).thenReturn(SEARCH_RESPONSE_JSON_STRING);

        // Try to call the method under test.
        try
        {
            elasticsearchHelper.getAggregation(searchResponse, AGGREGATION_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals("Invalid search result.", e.getMessage());
        }

        // Verify the external calls.
        verify(jsonHelper).objectToJson(searchResponse);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetNestedAggregation()
    {
        // Create a mock sub-aggregation.
        Terms subAggregation = mock(Terms.class);

        // Create mock nested aggregations.
        Aggregations nestedAggregations = mock(Aggregations.class);
        when(nestedAggregations.get(SUB_AGGREGATION_NAME)).thenReturn(subAggregation);

        // Create a mock nested aggregation.
        Nested nestedAggregation = mock(Nested.class);
        when(nestedAggregation.getAggregations()).thenReturn(nestedAggregations);

        // Create mock search response aggregations.
        Aggregations searchResponseAggregations = mock(Aggregations.class);
        when(searchResponseAggregations.get(NESTED_AGGREGATION_NAME)).thenReturn(nestedAggregation);

        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(searchResponseAggregations);

        // Call the method under test.
        Terms result = elasticsearchHelper.getNestedAggregation(searchResponse, NESTED_AGGREGATION_NAME, SUB_AGGREGATION_NAME);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(subAggregation, result);
    }

    @Test
    public void testGetNestedAggregationNestedAggregationIsNull()
    {
        // Create mock search response aggregations.
        Aggregations searchResponseAggregations = mock(Aggregations.class);
        when(searchResponseAggregations.get(NESTED_AGGREGATION_NAME)).thenReturn(null);

        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(searchResponseAggregations);

        // Mock the external calls.
        when(jsonHelper.objectToJson(searchResponse)).thenReturn(SEARCH_RESPONSE_JSON_STRING);

        // Try to call the method under test.
        try
        {
            elasticsearchHelper.getNestedAggregation(searchResponse, NESTED_AGGREGATION_NAME, SUB_AGGREGATION_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals("Invalid search result.", e.getMessage());
        }

        // Verify the external calls.
        verify(jsonHelper).objectToJson(searchResponse);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetNestedAggregationSubAggregationIsNull()
    {
        // Create mock nested aggregations.
        Aggregations nestedAggregations = mock(Aggregations.class);
        when(nestedAggregations.get(SUB_AGGREGATION_NAME)).thenReturn(null);

        // Create a mock nested aggregation.
        Nested nestedAggregation = mock(Nested.class);
        when(nestedAggregation.getAggregations()).thenReturn(nestedAggregations);

        // Create mock search response aggregations.
        Aggregations searchResponseAggregations = mock(Aggregations.class);
        when(searchResponseAggregations.get(NESTED_AGGREGATION_NAME)).thenReturn(nestedAggregation);

        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(searchResponseAggregations);

        // Mock the external calls.
        when(jsonHelper.objectToJson(searchResponse)).thenReturn(SEARCH_RESPONSE_JSON_STRING);
        when(jsonHelper.objectToJson(nestedAggregation)).thenReturn(NESTED_AGGREGATION_JSON_STRING);

        // Try to call the method under test.
        try
        {
            elasticsearchHelper.getNestedAggregation(searchResponse, NESTED_AGGREGATION_NAME, SUB_AGGREGATION_NAME);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals("Invalid search result.", e.getMessage());
        }

        // Verify the external calls.
        verify(jsonHelper).objectToJson(searchResponse);
        verify(jsonHelper).objectToJson(nestedAggregation);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetAggregationsFromNestedAggregationAggregationsSetIsNull()
    {
        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);

        // Create a mock nested aggregation.
        Nested nestedAggregation = mock(Nested.class);
        when(nestedAggregation.getAggregations()).thenReturn(null);

        // Mock the external calls.
        when(jsonHelper.objectToJson(searchResponse)).thenReturn(SEARCH_RESPONSE_JSON_STRING);
        when(jsonHelper.objectToJson(nestedAggregation)).thenReturn(NESTED_AGGREGATION_JSON_STRING);

        // Try to call the method under test.
        try
        {
            elasticsearchHelper.getAggregationsFromNestedAggregation(nestedAggregation, searchResponse);
        }
        catch (IllegalStateException e)
        {
            assertEquals("Invalid search result.", e.getMessage());
        }

        // Verify the external calls.
        verify(jsonHelper).objectToJson(searchResponse);
        verify(jsonHelper).objectToJson(nestedAggregation);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetAggregationsFromSearchResponseAggregationsSetIsNull()
    {
        // Create a mock search response.
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(null);

        // Mock the external calls.
        when(jsonHelper.objectToJson(searchResponse)).thenReturn(SEARCH_RESPONSE_JSON_STRING);

        // Try to call the method under test.
        try
        {
            elasticsearchHelper.getAggregationsFromSearchResponse(searchResponse);
        }
        catch (IllegalStateException e)
        {
            assertEquals("Invalid search result.", e.getMessage());
        }

        // Verify the external calls.
        verify(jsonHelper).objectToJson(searchResponse);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSearchIndexType()
    {
        assertEquals(SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name(), elasticsearchHelper
            .getSearchIndexType(BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME));

        assertEquals(SearchIndexTypeEntity.SearchIndexTypes.TAG.name(),
            elasticsearchHelper.getSearchIndexType(TAG_SEARCH_INDEX_NAME, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME));

        try
        {
            elasticsearchHelper.getSearchIndexType(SEARCH_INDEX_NAME, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME);
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Search result index name \"%s\" does not match any of the active search indexes. bdefActiveIndex=%s tagActiveIndex=%s",
                SEARCH_INDEX_NAME, BUSINESS_OBJECT_DEFINITION_SEARCH_INDEX_NAME, TAG_SEARCH_INDEX_NAME), e.getMessage());
        }
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(jsonHelper, tagDaoHelper);
    }
}
