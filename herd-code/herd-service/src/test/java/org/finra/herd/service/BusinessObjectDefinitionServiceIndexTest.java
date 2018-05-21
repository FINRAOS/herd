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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
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
    public void testIndexValidateBusinessObjectDefinitions()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getIdsInIndex(any(), any())).thenReturn(businessObjectDefinitionEntityIdList);

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitions();
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getIdsInIndex(any(), any());
        verify(businessObjectDefinitionHelper)
            .executeFunctionForBusinessObjectDefinitionEntities(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), eq(businessObjectDefinitionEntityList),
                any());
        verify(indexFunctionsDao, times(businessObjectDefinitionEntityIdList.size())).deleteDocumentById(any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitions()
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(100L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index size validation is false when it should have been true.", isIndexSizeValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(businessObjectDefinitionDao).getCountOfAllBusinessObjectDefinitions();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitionsFalse()
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(200L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index size validation is true when it should have been false.", isIndexSizeValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(businessObjectDefinitionDao).getCountOfAllBusinessObjectDefinitions();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitions()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check random validation is false when it should have been true.", isSpotCheckPercentageValid,
            is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitionsFalse()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckPercentageValidationBusinessObjectDefinitionsObjectMappingException()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitions()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check most recent validation is false when it should have been true.",
            isSpotCheckPercentageValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitionsFalse()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationBusinessObjectDefinitionsObjectMappingException()
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions(SEARCH_INDEX_NAME);

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionCreate()
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionCreateEmpty()
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdate()
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdateEmpty()
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionDelete()
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUnknown()
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
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper, indexFunctionsDao,
            jsonHelper, tagDaoHelper, tagHelper);
    }
}
