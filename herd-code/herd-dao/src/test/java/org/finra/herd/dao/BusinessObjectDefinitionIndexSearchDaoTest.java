package org.finra.herd.dao;

import static org.finra.herd.dao.SearchFilterType.EXCLUSION_SEARCH_FILTER;
import static org.finra.herd.dao.SearchFilterType.INCLUSION_SEARCH_FILTER;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.ElasticsearchHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JestClientHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.BusinessObjectDefinitionIndexSearchDaoImpl;
import org.finra.herd.model.dto.BusinessObjectDefinitionIndexSearchResponseDto;
import org.finra.herd.model.dto.DataProvider;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.Namespace;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;


public class BusinessObjectDefinitionIndexSearchDaoTest extends AbstractDaoTest
{
    @InjectMocks
    private BusinessObjectDefinitionIndexSearchDaoImpl businessObjectDefinitionIndexSearchDao;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private ElasticsearchHelper elasticsearchHelper;

    @Mock
    private JestClientHelper jestClientHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testFindAllBusinessObjectDefinitionsFunction() throws Exception
    {
        SearchResult searchResult = mock(SearchResult.class);
        JestResult jestResult = mock(JestResult.class);

        Terms.Bucket tagTypeCodeBucket = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeCodeBucketList = new ArrayList<>();
        tagTypeCodeBucketList.add(tagTypeCodeBucket);

        // Mock the call to external methods
        when(jestClientHelper.searchExecute(any(Search.class))).thenReturn(searchResult);

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtoList = new ArrayList<>();

        when(elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(any(SearchResult.class))).thenReturn(tagTypeIndexSearchResponseDtoList);
        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList
          = Arrays.asList(new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider("data provider"), "description 1", "display name", "bdefname", new Namespace("namespace")));

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_scroll_id", "100");
        when(searchResult.getSourceAsObjectList(BusinessObjectDefinitionIndexSearchResponseDto.class)).thenReturn(businessObjectDefinitionIndexSearchResponseDtoList);
        when(jestClientHelper.searchScrollExecute(any())).thenReturn(jestResult);
        when(searchResult.getJsonObject()).thenReturn(jsonObject);
        List<BusinessObjectDefinitionIndexSearchResponseDto> emptyBusinessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();

        ElasticsearchResponseDto elasticsearchResponseDto =  businessObjectDefinitionIndexSearchDao.findAllBusinessObjectDefinitions("INDEX_NAME", "DOCUMENT_TYPE", new HashSet<>());

        verify(jestClientHelper).searchExecute(any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionByTagsFunction() throws Exception
    {
        SearchResult searchResult = mock(SearchResult.class);
        JestResult jestResult = mock(JestResult.class);

        Terms.Bucket tagTypeCodeBucket = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeCodeBucketList = new ArrayList<>();
        tagTypeCodeBucketList.add(tagTypeCodeBucket);

        // Mock the call to external methods
        when(jestClientHelper.searchExecute(any(Search.class))).thenReturn(searchResult);

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtoList = new ArrayList<>();

        when(elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(any(SearchResult.class))).thenReturn(tagTypeIndexSearchResponseDtoList);
        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList
            = Arrays.asList(new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider("data provider"), "description 1", "display name", "bdefname", new Namespace("namespace")));

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_scroll_id", "100");
        when(searchResult.getSourceAsObjectList(BusinessObjectDefinitionIndexSearchResponseDto.class)).thenReturn(businessObjectDefinitionIndexSearchResponseDtoList);
        when(jestClientHelper.searchScrollExecute(any())).thenReturn(jestResult);
        when(searchResult.getJsonObject()).thenReturn(jsonObject);
        List<BusinessObjectDefinitionIndexSearchResponseDto> emptyBusinessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();

        // Get test tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode("TAG_CODE");

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode("TAG_TYPE_CODE");
        tagTypeEntity.setDisplayName("DISPLAY_NAME");
        tagTypeEntity.setOrderNumber(1);

        tagEntity.setTagType(tagTypeEntity);

        List<TagEntity> tagEntities = new ArrayList<>();
        tagEntities.add(tagEntity);

        // List<Map<SearchFilterType, List<TagEntity>>>
        Map<SearchFilterType, List<TagEntity>> searchFilterTypeListMap = new HashMap<>();
        searchFilterTypeListMap.put(INCLUSION_SEARCH_FILTER, tagEntities);
        List<Map<SearchFilterType, List<TagEntity>>> tagEnLists = Collections.singletonList(searchFilterTypeListMap);

        ElasticsearchResponseDto elasticsearchResponseDto = businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags("INDEX_NAME", "DOCUMENT_TYPE", tagEnLists, new HashSet<>());

        verify(jestClientHelper).searchExecute(any());
    }

    @Test
    public void testSearchBusinessObjectDefinitionByTagsFunctionExclusionAndTag() throws Exception
    {
        SearchResult searchResult = mock(SearchResult.class);
        JestResult jestResult = mock(JestResult.class);

        Terms.Bucket tagTypeCodeBucket = mock(Terms.Bucket.class);
        List<Terms.Bucket> tagTypeCodeBucketList = new ArrayList<>();
        tagTypeCodeBucketList.add(tagTypeCodeBucket);

        // Mock the call to external methods
        when(jestClientHelper.searchExecute(any(Search.class))).thenReturn(searchResult);

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtoList = new ArrayList<>();

        when(elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(any(SearchResult.class))).thenReturn(tagTypeIndexSearchResponseDtoList);
        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList
            = Arrays.asList(new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider("data provider"), "description 1", "display name", "bdefname", new Namespace("namespace")));

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_scroll_id", "100");
        when(searchResult.getSourceAsObjectList(BusinessObjectDefinitionIndexSearchResponseDto.class)).thenReturn(businessObjectDefinitionIndexSearchResponseDtoList);
        when(jestClientHelper.searchScrollExecute(any())).thenReturn(jestResult);
        when(searchResult.getJsonObject()).thenReturn(jsonObject);
        List<BusinessObjectDefinitionIndexSearchResponseDto> emptyBusinessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();

        // Get test tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode("TAG_CODE");

        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode("TAG_TYPE_CODE");
        tagTypeEntity.setDisplayName("DISPLAY_NAME");
        tagTypeEntity.setOrderNumber(1);

        tagEntity.setTagType(tagTypeEntity);

        List<TagEntity> tagEntities = new ArrayList<>();
        tagEntities.add(tagEntity);

        // List<Map<SearchFilterType, List<TagEntity>>>
        Map<SearchFilterType, List<TagEntity>> searchFilterTypeListMap = new HashMap<>();
        searchFilterTypeListMap.put(EXCLUSION_SEARCH_FILTER, tagEntities);
        List<Map<SearchFilterType, List<TagEntity>>> tagEnLists = Collections.singletonList(searchFilterTypeListMap);

        when(elasticsearchHelper.getNestedTagTagIndexSearchResponseDto(searchResult)).thenReturn(tagTypeIndexSearchResponseDtoList);
        ElasticsearchResponseDto elasticsearchResponseDto = businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags("INDEX_NAME", "DOCUMENT_TYPE", tagEnLists, new HashSet<>(Arrays.asList("tag")));

        verify(jestClientHelper, times(2)).searchExecute(any());
        verify(jestClientHelper).searchScrollExecute(any());
    }
}
