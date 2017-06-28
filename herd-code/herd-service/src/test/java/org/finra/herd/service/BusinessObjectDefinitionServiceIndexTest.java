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
package org.finra.herd.service;

import static org.finra.herd.model.dto.SearchIndexUpdateDto.MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_CREATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_DELETE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionIndexSearchDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionIndexSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionIndexSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.BusinessObjectDefinitionIndexSearchResponseDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.DataProvider;
import org.finra.herd.model.dto.ElasticsearchResponseDto;
import org.finra.herd.model.dto.Namespace;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.impl.BusinessObjectDefinitionServiceImpl;

/**
 * This class tests various functionality related to Elasticsearch within the business object definition service.
 */
public class BusinessObjectDefinitionServiceIndexTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private BusinessObjectDefinitionIndexSearchDao businessObjectDefinitionIndexSearchDao;

    @InjectMocks
    private BusinessObjectDefinitionServiceImpl businessObjectDefinitionService;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private IndexFunctionsDao indexFunctionsDao;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private TagDaoHelper tagDaoHelper;

    @Mock
    private TagHelper tagHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexValidateBusinessObjectDefinitions() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<String> businessObjectDefinitionEntityIdList = new ArrayList<>();
        businessObjectDefinitionEntityIdList.add("123456");
        businessObjectDefinitionEntityIdList.add("654321");
        businessObjectDefinitionEntityIdList.add("789012");

        // Mock the call to external methods
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions()).thenReturn(businessObjectDefinitionEntityList);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);

        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getIdsInIndex(any(), any())).thenReturn(businessObjectDefinitionEntityIdList);

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitions();
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getIdsInIndex(any(), any());
        verify(businessObjectDefinitionHelper)
            .executeFunctionForBusinessObjectDefinitionEntities(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), eq(businessObjectDefinitionEntityList),
                any());
        verify(indexFunctionsDao, times(businessObjectDefinitionEntityIdList.size())).deleteDocumentById(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitions() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(100L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index size validation is false when it should have been true.", isIndexSizeValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(businessObjectDefinitionDao).getCountOfAllBusinessObjectDefinitions();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitionsFalse() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(200L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index size validation is true when it should have been false.", isIndexSizeValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(businessObjectDefinitionDao).getCountOfAllBusinessObjectDefinitions();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitions() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.05);
        when(businessObjectDefinitionDao.getPercentageOfAllBusinessObjectDefinitions(0.05)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is false when it should have been true.", isSpotCheckPercentageValid,
            is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitionsFalse() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.05);
        when(businessObjectDefinitionDao.getPercentageOfAllBusinessObjectDefinitions(0.05)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitionsObjectMappingException() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.05);
        when(businessObjectDefinitionDao.getPercentageOfAllBusinessObjectDefinitions(0.05)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitions() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(100);
        when(businessObjectDefinitionDao.getMostRecentBusinessObjectDefinitions(100)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is false when it should have been true.",
            isSpotCheckPercentageValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitionsFalse() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(100);
        when(businessObjectDefinitionDao.getMostRecentBusinessObjectDefinitions(100)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitionsObjectMappingException() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(100);
        when(businessObjectDefinitionDao.getMostRecentBusinessObjectDefinitions(100)).thenReturn(businessObjectDefinitionEntityList);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsIncludeTagHierarchy()
    {
        // Create a new tag key with a tag type and a tag code
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create  a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with the tag key and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(TAG_CODE);

        // Create a tag child entity to enter into the tag children entities list
        TagEntity tagChildEntity = new TagEntity();
        tagChildEntity.setTagCode(TAG_CODE_2);

        // Create a tag children entity list to return from the tag dao helper tag children entities method
        List<TagEntity> tagChildrenEntityList = new ArrayList<>();
        tagChildrenEntityList.add(tagChildEntity);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey())).thenReturn(tagEntity);
        when(tagDaoHelper.getTagChildrenEntities(tagEntity)).thenReturn(tagChildrenEntityList);

        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagDaoHelper).getTagChildrenEntities(tagEntity);
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsDoNotIncludeTagHierarchy()
    {
        // Create a new tag key with a tag type and a tag code
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create  a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, NOT_INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with the tag key and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(TAG_CODE);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey())).thenReturn(tagEntity);
        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKey.getTagKey());
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionWithFacetFieldTag()
    {
        //Create a list of facet fields
        List<String> facetFields = new ArrayList<>();
        facetFields.add("TAG");
        indexSearchBusinessObjectDefinitionsFacetFields(facetFields);
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionWithFacetFieldTagWhiteSpace()
    {
        //Create a list of facet fields
        List<String> facetFields = new ArrayList<>();
        facetFields.add(addWhitespace("TAG"));
        indexSearchBusinessObjectDefinitionsFacetFields(facetFields);
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionWithFacetFieldTagMixedCase()
    {
        //Create a list of facet fields
        List<String> facetFields = new ArrayList<>();
        facetFields.add(("TaG"));
        indexSearchBusinessObjectDefinitionsFacetFields(facetFields);
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionReturnsEmptyResponseForEmptyTagEntities()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag entity.
        TagEntity tagEntity = new TagEntity();

        // Create  a new business object definition search key for use in the business object definition search key list with an empty tag key
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, NOT_INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with the tag key and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);
        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any()))
            .thenReturn(new ElasticsearchResponseDto());

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, new HashSet<>());

        assertThat("Expected empty response", CollectionUtils.isEmpty(businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions()));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(tagKey);
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionWithFacetsReturnsEmptyResponseForEmptyTagEntities()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag entity.
        TagEntity tagEntity = new TagEntity();

        // Create  a new business object definition search key for use in the business object definition search key list with an empty tag key
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, NOT_INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with the tag key and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, Collections.singletonList("TAG"));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(tagKey)).thenReturn(tagEntity);
        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any()))
            .thenReturn(new ElasticsearchResponseDto());

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, new HashSet<>());

        assertThat("Expected empty response", CollectionUtils.isEmpty(businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions()));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(tagKey);
        verify(tagDaoHelper).getTagEntity(tagKey);
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsWithMultipleTagsIncludeTagHierarchy()
    {
        // Create new tag keys
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        TagKey tagKeyTwo = new TagKey(TAG_TYPE_2, TAG_CODE_2);

        // Create a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, INCLUDE_TAG_HIERARCHY);

        // Create another new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKeyTwo = new BusinessObjectDefinitionSearchKey(tagKeyTwo, INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with both the tag keys and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKeyTwo);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(TAG_CODE);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntityTwo = new TagEntity();
        tagEntity.setTagCode(TAG_CODE_2);

        // Create a tag child entity to enter into the tag children entities list
        TagEntity tagChildEntity = new TagEntity();
        tagChildEntity.setTagCode(TAG_CODE_2);

        // Create a tag children entity list to return from the tag dao helper tag children entities method
        List<TagEntity> tagChildrenEntityList = new ArrayList<>();
        tagChildrenEntityList.add(tagChildEntity);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey())).thenReturn(tagEntity);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey())).thenReturn(tagEntityTwo);
        when(tagDaoHelper.getTagChildrenEntities(tagEntity)).thenReturn(tagChildrenEntityList);

        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(tagDaoHelper).getTagChildrenEntities(tagEntity);
        verify(tagDaoHelper).getTagChildrenEntities(tagEntityTwo);
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsWithMultipleTagsDoNotIncludeTagHierarchy()
    {
        // Create new tag keys
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        TagKey tagKeyTwo = new TagKey(TAG_TYPE_2, TAG_CODE_2);

        // Create a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, NOT_INCLUDE_TAG_HIERARCHY);

        // Create another new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKeyTwo = new BusinessObjectDefinitionSearchKey(tagKeyTwo, NOT_INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with both the tag keys and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKeyTwo);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList
            .add(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(TAG_CODE);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntityTwo = new TagEntity();
        tagEntity.setTagCode(TAG_CODE_2);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey())).thenReturn(tagEntity);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey())).thenReturn(tagEntityTwo);

        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsWithMultipleTagsWithIsExclusionSearchFilter()
    {
        // Create new tag keys
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        TagKey tagKeyTwo = new TagKey(TAG_TYPE_2, TAG_CODE_2);

        // Create a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, NOT_INCLUDE_TAG_HIERARCHY);

        // Create another new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKeyTwo = new BusinessObjectDefinitionSearchKey(tagKeyTwo, NOT_INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with both the tag keys and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKeyTwo);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList.add(new BusinessObjectDefinitionSearchFilter(true, businessObjectDefinitionSearchKeyList));

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, new ArrayList<>());

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagCode(TAG_CODE);

        // Create a tag entity to return from the tag dao helper get tag entity method
        TagEntity tagEntityTwo = new TagEntity();
        tagEntity.setTagCode(TAG_CODE_2);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey())).thenReturn(tagEntity);
        when(tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey())).thenReturn(tagEntityTwo);
        when(businessObjectDefinitionIndexSearchDao.searchBusinessObjectDefinitionsByTags(any(), any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagHelper).validateTagKey(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKey.getTagKey());
        verify(tagDaoHelper).getTagEntity(businessObjectDefinitionSearchKeyTwo.getTagKey());
        verify(businessObjectDefinitionIndexSearchDao).searchBusinessObjectDefinitionsByTags(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitionsInvalidFacet()
    {
        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();

        //Create a list of facet fields
        List<String> facetFields = new ArrayList<>();
        facetFields.add("Invalid");

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, facetFields);

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);

        // Call the method under test
        try
        {
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);
            fail("Should have caught an exception.");
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            assertThat("The IllegalArgumentException message is not correct.", illegalArgumentException.getMessage(),
                is("Facet field \"invalid\" is not supported."));
        }
        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verifyNoMoreInteractionsHelper();
    }

    private void indexSearchBusinessObjectDefinitionsFacetFields(List<String> facetFields)
    {
        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();

        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionIndexSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, facetFields);

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        List<BusinessObjectDefinitionIndexSearchResponseDto> businessObjectDefinitionIndexSearchResponseDtoList = new ArrayList<>();
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto1 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, BDEF_NAME,
                new Namespace(NAMESPACE));
        BusinessObjectDefinitionIndexSearchResponseDto businessObjectDefinitionIndexSearchResponseDto2 =
            new BusinessObjectDefinitionIndexSearchResponseDto(new DataProvider(DATA_PROVIDER_NAME_2), BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, BDEF_NAME_2,
                new Namespace(NAMESPACE));
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto1);
        businessObjectDefinitionIndexSearchResponseDtoList.add(businessObjectDefinitionIndexSearchResponseDto2);

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos = new ArrayList<>();
        tagIndexSearchResponseDtos.add(new TagIndexSearchResponseDto(TAG_CODE, TAG_COUNT, TAG_DISPLAY_NAME));
        tagIndexSearchResponseDtos.add(new TagIndexSearchResponseDto(TAG_CODE_2, TAG_COUNT, TAG_DISPLAY_NAME_2));
        TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto =
            new TagTypeIndexSearchResponseDto(TAG_TYPE, TAG_TYPE_COUNT, tagIndexSearchResponseDtos, TAG_TYPE_DISPLAY_NAME);
        tagTypeIndexSearchResponseDtos.add(tagTypeIndexSearchResponseDto);

        ElasticsearchResponseDto elasticsearchResponseDto = new ElasticsearchResponseDto();
        elasticsearchResponseDto.setBusinessObjectDefinitionIndexSearchResponseDtos(businessObjectDefinitionIndexSearchResponseDtoList);
        elasticsearchResponseDto.setTagTypeIndexSearchResponseDtos(tagTypeIndexSearchResponseDtos);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class))
            .thenReturn(SHORT_DESCRIPTION_LENGTH);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);

        when(businessObjectDefinitionIndexSearchDao.findAllBusinessObjectDefinitions(any(), any(), any())).thenReturn(elasticsearchResponseDto);

        // Call the method under test
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse =
            businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionIndexSearchRequest, fields);

        assertThat("Business object definition service index search business object definitions method response is null, but it should not be.",
            businessObjectDefinitionSearchResponse, not(nullValue()));

        assertThat("The first business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(0).getBusinessObjectDefinitionName(), is(BDEF_NAME));

        assertThat("The second business object definition name in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions().get(1).getBusinessObjectDefinitionName(), is(BDEF_NAME_2));

        assertThat("The tag type code in the search response is not correct.", businessObjectDefinitionSearchResponse.getFacets().get(0).getFacetId(),
            is(TAG_TYPE));

        assertThat("The tag code in the search response is not correct.",
            businessObjectDefinitionSearchResponse.getFacets().get(0).getFacets().get(0).getFacetId(), is(TAG_CODE));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionIndexSearchDao).findAllBusinessObjectDefinitions(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionCreate() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao).createIndexDocuments(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionCreateEmpty() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(jsonHelper.objectToJson(any())).thenReturn(EMPTY_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao).createIndexDocuments(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdate() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Update a document in the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(indexFunctionsDao).updateIndexDocuments(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), Matchers.<Map<String, String>>any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdateEmpty() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Update a document in the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(jsonHelper.objectToJson(any())).thenReturn("");

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(indexFunctionsDao).updateIndexDocuments(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), Matchers.<Map<String, String>>any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdateIdListSizeGreaterThanChunkSize() throws Exception
    {
        // Create two lists of business object definition entities.
        List<List<BusinessObjectDefinitionEntity>> businessObjectDefinitionEntities = Arrays.asList(Collections.singletonList(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes())), Collections.singletonList(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2())));

        // Create a list of business object definition ids that would require to be processed in chunks.
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.addAll(Collections.nCopies(BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE + 1, ID));

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(
            businessObjectDefinitionIds.subList(0, BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE)))
            .thenReturn(businessObjectDefinitionEntities.get(0));
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(businessObjectDefinitionIds
            .subList(BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE, businessObjectDefinitionIds.size())))
            .thenReturn(businessObjectDefinitionEntities.get(1));
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(0).get(0))).thenReturn(JSON_STRING);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(1).get(0))).thenReturn(JSON_STRING);

        // Call the method under test.
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE));

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(
            businessObjectDefinitionIds.subList(0, BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(businessObjectDefinitionIds
            .subList(BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE, businessObjectDefinitionIds.size()));
        verify(businessObjectDefinitionHelper).safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(0).get(0));
        verify(businessObjectDefinitionHelper).safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(1).get(0));
        verify(indexFunctionsDao, times(2)).updateIndexDocuments(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), Matchers.<Map<String, String>>any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionDelete() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);


        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).deleteIndexDocuments(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUnknown() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, "UNKNOWN");

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verifyNoMoreInteractionsHelper();
    }

    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, businessObjectDefinitionIndexSearchDao, configurationDaoHelper,
            configurationHelper, indexFunctionsDao, jsonHelper, tagDaoHelper, tagHelper);
    }
}
