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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.impl.BusinessObjectDefinitionServiceImpl;

/**
 * This class tests various functionality within the business object definition REST controller.
 */
public class BusinessObjectDefinitionServiceIndexTest extends AbstractServiceTest
{
    @InjectMocks
    private BusinessObjectDefinitionServiceImpl businessObjectDefinitionService;

    @Mock
    private SearchFunctions searchFunctions;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexAllBusinessObjectDefinitions() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey())).thenReturn("MAPPING");
        when(searchFunctions.getIndexExistsFunction()).thenReturn(indexName -> true);
        when(searchFunctions.getDeleteIndexFunction()).thenReturn(indexName -> {
        });
        when(searchFunctions.getCreateIndexFunction()).thenReturn((indexName, documentType, mapping) -> {
        });
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions()).thenReturn(businessObjectDefinitionEntityList);
        when(searchFunctions.getIndexFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 2L);
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(configurationDaoHelper, times(1)).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        verify(searchFunctions, times(1)).getIndexExistsFunction();
        verify(searchFunctions, times(1)).getDeleteIndexFunction();
        verify(searchFunctions, times(1)).getCreateIndexFunction();
        verify(businessObjectDefinitionDao, times(1)).getAllBusinessObjectDefinitions();
        verify(searchFunctions, times(1)).getIndexFunction();
        verify(searchFunctions, times(1)).getNumberOfTypesInIndexFunction();
        verify(jsonHelper, times(2)).objectToJson(any());
    }

    @Test
    public void testIndexAllBusinessObjectDefinitionsSizeInvalid() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey())).thenReturn("MAPPING");
        when(searchFunctions.getIndexExistsFunction()).thenReturn(indexName -> true);
        when(searchFunctions.getDeleteIndexFunction()).thenReturn(indexName -> {
        });
        when(searchFunctions.getCreateIndexFunction()).thenReturn((indexName, documentType, mapping) -> {
        });
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions()).thenReturn(businessObjectDefinitionEntityList);
        when(searchFunctions.getIndexFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 1L);
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(configurationDaoHelper, times(1)).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        verify(searchFunctions, times(1)).getIndexExistsFunction();
        verify(searchFunctions, times(1)).getDeleteIndexFunction();
        verify(searchFunctions, times(1)).getCreateIndexFunction();
        verify(businessObjectDefinitionDao, times(1)).getAllBusinessObjectDefinitions();
        verify(searchFunctions, times(1)).getIndexFunction();
        verify(searchFunctions, times(1)).getNumberOfTypesInIndexFunction();
        verify(jsonHelper, times(2)).objectToJson(any());
    }

    @Test
    public void testIndexAllBusinessObjectDefinitionsObjectMappingException() throws Exception
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey())).thenReturn("MAPPING");
        when(searchFunctions.getIndexExistsFunction()).thenReturn(indexName -> true);
        when(searchFunctions.getDeleteIndexFunction()).thenReturn(indexName -> {
        });
        when(searchFunctions.getCreateIndexFunction()).thenReturn((indexName, documentType, mapping) -> {
        });
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions()).thenReturn(businessObjectDefinitionEntityList);
        when(searchFunctions.getIndexFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 2L);
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(configurationDaoHelper, times(1)).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        verify(searchFunctions, times(1)).getIndexExistsFunction();
        verify(searchFunctions, times(1)).getDeleteIndexFunction();
        verify(searchFunctions, times(1)).getCreateIndexFunction();
        verify(businessObjectDefinitionDao, times(1)).getAllBusinessObjectDefinitions();
        verify(searchFunctions, times(1)).getIndexFunction();
        verify(searchFunctions, times(1)).getNumberOfTypesInIndexFunction();
        verify(jsonHelper, times(2)).objectToJson(any());
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
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");
        when(searchFunctions.getValidateFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(searchFunctions.getIdsInIndexFunction()).thenReturn((indexName, documentType) -> businessObjectDefinitionEntityIdList);
        when(searchFunctions.getDeleteDocumentByIdFunction()).thenReturn((indexName, documentType, id) -> {
        });

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(businessObjectDefinitionDao, times(1)).getAllBusinessObjectDefinitions();
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(1)).getValidateFunction();
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(searchFunctions, times(1)).getIdsInIndexFunction();
        verify(searchFunctions, times(3)).getDeleteDocumentByIdFunction();
    }

    @Test
    public void testIndexValidateBusinessObjectDefinitionsObjectMappingException() throws Exception
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
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(searchFunctions.getValidateFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(searchFunctions.getIdsInIndexFunction()).thenReturn((indexName, documentType) -> businessObjectDefinitionEntityIdList);
        when(searchFunctions.getDeleteDocumentByIdFunction()).thenReturn((indexName, documentType, id) -> {
        });

        // Call the method under test
        Future<Void> future = businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions();

        assertThat("Business object definition service index all business object definitions method returned null value.", future, not(nullValue()));
        assertThat("Business object definition service index all business object definitions method return value is not instance of future.", future,
            instanceOf(Future.class));

        // Verify the calls to external methods
        verify(businessObjectDefinitionDao, times(1)).getAllBusinessObjectDefinitions();
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(1)).getValidateFunction();
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(searchFunctions, times(1)).getIdsInIndexFunction();
        verify(searchFunctions, times(3)).getDeleteDocumentByIdFunction();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitions() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(100L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index size validation is false when it should have been true.", isIndexSizeValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(searchFunctions, times(1)).getNumberOfTypesInIndexFunction();
        verify(businessObjectDefinitionDao, times(1)).getCountOfAllBusinessObjectDefinitions();
    }

    @Test
    public void testIndexSizeCheckValidationBusinessObjectDefinitionsFalse() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 100L);
        when(businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions()).thenReturn(200L);

        // Call the method under test
        boolean isIndexSizeValid = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index size validation is true when it should have been false.", isIndexSizeValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(searchFunctions, times(1)).getNumberOfTypesInIndexFunction();
        verify(businessObjectDefinitionDao, times(1)).getCountOfAllBusinessObjectDefinitions();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is false when it should have been true.", isSpotCheckPercentageValid,
            is(true));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao, times(1)).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao, times(1)).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid,
            is(false));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(businessObjectDefinitionDao, times(1)).getPercentageOfAllBusinessObjectDefinitions(0.05);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is false when it should have been true.",
            isSpotCheckPercentageValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao, times(1)).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any())).thenReturn("JSON_STRING");
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao, times(1)).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
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
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn("INDEX_NAME");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(searchFunctions.getIsValidFunction()).thenReturn((indexName, documentType, id, json) -> false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();

        assertThat("Business object definition service index spot check most recent validation is true when it should have been false.",
            isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(businessObjectDefinitionDao, times(1)).getMostRecentBusinessObjectDefinitions(100);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(jsonHelper, times(2)).objectToJson(any());
        verify(searchFunctions, times(2)).getIsValidFunction();
    }
}