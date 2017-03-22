package org.finra.herd.dao.helper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;


public class ElasticSearchHelperTest extends AbstractDaoTest
{
    public static final String TAG_TYPE_CODE = "Tag Type Code 1";

    public static final String TAG_TYPE_CODE_2 = "Tag Type Code 2";

    public static final String TAG_TYPE_DISPLAY_NAME = "Tag Type DisplayName";

    public static final String TAG_TYPE_DISPLAY_NAME_2 = "Tag Type DisplayName 2";

    public static final int TAG_TYPE_CODE_COUNT = 1;

    public static final int TAG_TYPE_CODE_COUNT_2 = 2;

    public static final String TAG_CODE = "Tag Code 1";

    public static final String TAG_CODE_2 = "Tag Code 2";

    public static final int TAG_CODE_COUNT_2 = 1;

    public static final int TAG_CODE_COUNT = 1;

    public static final String TAG_CODE_DISPLAY_NAME = "Tag Code DisplayName";

    public static final String TAG_CODE_DISPLAY_NAME_2 = "Tag Code DisplayName 2";

    @Autowired
    ElasticsearchHelper elasticSearchHelper;

    @Test
    public void testGetFacetsResponseWithEmptyResponseDto()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new  ElasticsearchResponseDto();
        List<Facet> facets = elasticSearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        Assert.isTrue(facets.size() == 0 );
        facets = elasticSearchHelper.getFacetsResponse(elasticsearchResponseDto, true);
        Assert.isTrue(facets.size() == 0 );
    }

    @Test
    public void testGetFacetsResponse()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new  ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2, Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);
        
        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticSearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME,(long)TAG_TYPE_CODE_COUNT, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE, new ArrayList<Facet>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, TagIndexSearchResponseDto.getFacetType(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2,(long)TAG_TYPE_CODE_COUNT_2, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTag()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new  ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2, Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2, Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticSearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME,(long)TAG_TYPE_CODE_COUNT, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE, new ArrayList<Facet>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, TagIndexSearchResponseDto.getFacetType(), TAG_CODE, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2,(long)TAG_TYPE_CODE_COUNT_2, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }

    @Test
    public void testGetFacetsResponseIncludingTagWithNoAssociatedBdefs()
    {
        ElasticsearchResponseDto elasticsearchResponseDto = new  ElasticsearchResponseDto();

        List<TagTypeIndexSearchResponseDto> nestTagTypeIndexSearchResponseDtos = new ArrayList<>();
        TagTypeIndexSearchResponseDto tagType1 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE, TAG_TYPE_CODE_COUNT, null, TAG_TYPE_DISPLAY_NAME);
        TagTypeIndexSearchResponseDto tagType2 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2, Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE, 1, TAG_CODE_DISPLAY_NAME)), TAG_TYPE_DISPLAY_NAME_2);
        nestTagTypeIndexSearchResponseDtos.add(tagType1);
        nestTagTypeIndexSearchResponseDtos.add(tagType2);

        elasticsearchResponseDto.setNestTagTypeIndexSearchResponseDtos(nestTagTypeIndexSearchResponseDtos);
        TagTypeIndexSearchResponseDto tagType3 = new TagTypeIndexSearchResponseDto(TAG_TYPE_CODE_2, TAG_TYPE_CODE_COUNT_2, Arrays.asList(new TagIndexSearchResponseDto(TAG_CODE_2, 1, TAG_CODE_DISPLAY_NAME_2)), TAG_TYPE_DISPLAY_NAME_2);
        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        tagTypeIndexSearchResponseDtos.add(tagType3);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        List<Facet> facets = elasticSearchHelper.getFacetsResponse(elasticsearchResponseDto, false);
        List<Facet> expectedFacets = new ArrayList<>();
        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME,(long)TAG_TYPE_CODE_COUNT, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE, new ArrayList<Facet>()));

        List<Facet> tagFacet = new ArrayList<>();
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME, (long) TAG_CODE_COUNT, TagIndexSearchResponseDto.getFacetType(), TAG_CODE, null));
        tagFacet.add(new Facet(TAG_CODE_DISPLAY_NAME_2, (long) TAG_CODE_COUNT_2, TagIndexSearchResponseDto.getFacetType(), TAG_CODE_2, null));

        expectedFacets.add(new Facet(TAG_TYPE_DISPLAY_NAME_2,(long)TAG_TYPE_CODE_COUNT_2, TagTypeIndexSearchResponseDto.getFacetType(),TAG_TYPE_CODE_2, tagFacet));
        assertEquals(expectedFacets, facets);
    }
    
}
