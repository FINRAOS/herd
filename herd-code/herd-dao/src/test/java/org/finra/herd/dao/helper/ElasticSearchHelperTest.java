package org.finra.herd.dao.helper;

import static org.finra.herd.dao.helper.ElasticsearchHelper.RESULT_TYPE_AGGS;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAGTYPE_NAME_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_CODE_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_NAME_AGGREGATION;
import static org.finra.herd.dao.helper.ElasticsearchHelper.TAG_TYPE_FACET_AGGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.util.Assert;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.FacetTypeEnum;
import org.finra.herd.model.dto.ResultTypeIndexSearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;

public class ElasticSearchHelperTest extends AbstractDaoTest
{
    public static final String TAG_TYPE_CODE = "Tag Type Code 1";

    public static final String TAG_TYPE_CODE_2 = "Tag Type Code 2";

    public static final String TAG_TYPE_CODE_3 = "Tag Type Code 3";

    public static final String TAG_TYPE_DISPLAY_NAME = "Tag Type DisplayName";

    public static final String TAG_TYPE_DISPLAY_NAME_2 = "Tag Type DisplayName 2";

    public static final String TAG_TYPE_DISPLAY_NAME_3 = "Tag Type DisplayName 3";

    public static final int TAG_TYPE_CODE_COUNT = 1;

    public static final int TAG_TYPE_CODE_COUNT_2 = 2;

    public static final int TAG_TYPE_CODE_COUNT_3 = 2;

    public static final String TAG_CODE = "Tag Code 1";

    public static final String TAG_CODE_2 = "Tag Code 2";

    public static final int TAG_CODE_COUNT_2 = 1;

    public static final int TAG_CODE_COUNT = 1;

    public static final String TAG_CODE_DISPLAY_NAME = "Tag Code DisplayName";

    public static final String TAG_CODE_DISPLAY_NAME_2 = "Tag Code DisplayName 2";

    @InjectMocks
    private ElasticsearchHelper elasticsearchHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetFacetsResponseWithEmptyResponseDto()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        List<Facet> facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        Assert.isTrue(facets.size() == 0);
        facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, true);
        Assert.isTrue(facets.size() == 0);
    }

    @Test
    public void testGetFacetsResponse()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, (long) TAG_TYPE_CODE_COUNT, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, (long) TAG_TYPE_CODE_COUNT_2, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTag()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, (long) TAG_TYPE_CODE_COUNT, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT + 1, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, (long) TAG_TYPE_CODE_COUNT_2, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTagWithNoAssociatedBdefs()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE_2, 1, TAG_CODE_DISPLAY_NAME_2)), TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, (long) TAG_TYPE_CODE_COUNT, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, FacetTypeEnum.TAG.value(), TAG_CODE, null));
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME_2, (long) TAG_CODE_COUNT_2, FacetTypeEnum.TAG.value(), TAG_CODE_2, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, (long) TAG_TYPE_CODE_COUNT_2, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTagWithNoAssociatedBdefsNewTagType()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_3, TAG_TYPE_CODE_COUNT_3,
            Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE_2, 1, TAG_CODE_DISPLAY_NAME_2)), TAG_TYPE_DISPLAY_NAME_3);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticsearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME, (long) TAG_TYPE_CODE_COUNT, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE, new ArrayList<>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, FacetTypeEnum.TAG.value(), TAG_CODE, null));

        List<Facet> newTagFacet = new ArrayList<>();
        newTagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME_2, (long) TAG_CODE_COUNT_2, FacetTypeEnum.TAG.value(), TAG_CODE_2, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2, (long) TAG_TYPE_CODE_COUNT_2, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_2, tagFacet));
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_3, (long) TAG_TYPE_CODE_COUNT_3, FacetTypeEnum.TAG_TYPE.value(), TAG_TYPE_CODE_3, newTagFacet));

        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetResultTypeIndexSearchResponseDto()
    {
        SearchResponse searchResponse = mock(SearchResponse.class);
        Terms terms = mock(Terms.class);
        Aggregations aggregations = mock(Aggregations.class);
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        List<Terms.Bucket> buckets = Arrays.asList(bucket);

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
        List<Terms.Bucket> tagTypeCodeEntryList = Arrays.asList(tagTypeCodeEntry);

        when(searchResponse.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(TAG_TYPE_FACET_AGGS)).thenReturn(terms);
        when(terms.getBuckets()).thenReturn(tagTypeCodeEntryList);
        when(tagTypeCodeEntry.getKeyAsString()).thenReturn(TAG_TYPE_CODE);
        when(tagTypeCodeEntry.getDocCount()).thenReturn((long) TAG_TYPE_CODE_COUNT);
        when(tagTypeCodeEntry.getAggregations()).thenReturn(aggregations);

        Terms tagTypeDisplayNameAggs = mock(Terms.class);
        Terms.Bucket tagTypeDisplayNameEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeDisplayNameEntryList = Arrays.asList(tagTypeDisplayNameEntry);
        when(aggregations.get(TAGTYPE_NAME_AGGREGATION)).thenReturn(tagTypeDisplayNameAggs);
        when(tagTypeDisplayNameEntry.getAggregations()).thenReturn(aggregations);
        when(tagTypeDisplayNameAggs.getBuckets()).thenReturn(tagTypeDisplayNameEntryList);
        when(tagTypeDisplayNameEntry.getKeyAsString()).thenReturn(TAG_TYPE_DISPLAY_NAME);

        Terms tagCodeAggs = mock(Terms.class);
        Terms.Bucket tagCodeEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagCodeEntryList = Arrays.asList(tagCodeEntry);

        when(aggregations.get(TAG_CODE_AGGREGATION)).thenReturn(tagCodeAggs);
        when(tagCodeAggs.getBuckets()).thenReturn(tagCodeEntryList);
        when(tagCodeEntry.getAggregations()).thenReturn(aggregations);
        when(tagCodeEntry.getKeyAsString()).thenReturn(TAG_CODE);
        when(tagCodeEntry.getDocCount()).thenReturn((long) TAG_CODE_COUNT);
        Terms tagNameAggs = mock(Terms.class);
        Terms.Bucket tagNameEntry = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagNameEntryList = Arrays.asList(tagNameEntry);
        when(tagNameEntry.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(TAG_NAME_AGGREGATION)).thenReturn(tagNameAggs);
        when(tagNameAggs.getBuckets()).thenReturn(tagNameEntryList);
        when(tagNameEntry.getKeyAsString()).thenReturn(TAG_DISPLAY_NAME);

        List<TagTypeIndexSearchResponseDto> resultList = elasticsearchHelper.getTagTagIndexSearchResponseDto(searchResponse);
        List<TagTypeIndexSearchResponseDto> expectedList = new ArrayList<>();
        List<TagIndexSearchResponseDto> expectedTagList = new ArrayList<>();
        expectedTagList.add(new TagIndexSearchResponseDto(TAG_CODE, TAG_CODE_COUNT, TAG_DISPLAY_NAME));
        expectedList.add(new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, expectedTagList, TAG_TYPE_DISPLAY_NAME));

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

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(jsonHelper);
    }
}
