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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.ArgumentMatchers;
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
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionCreate()
    {
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao).createIndexDocuments(any(), any());
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

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(jsonHelper.objectToJson(any())).thenReturn(EMPTY_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(indexFunctionsDao).createIndexDocuments(eq(SEARCH_INDEX_NAME), any());
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

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Update a document in the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class))).thenReturn(JSON_STRING);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(indexFunctionsDao).updateIndexDocuments(eq(SEARCH_INDEX_NAME), ArgumentMatchers.<Map<String, String>>any());
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

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Update a document in the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(any())).thenReturn(businessObjectDefinitionEntityList);
        when(jsonHelper.objectToJson(any())).thenReturn("");

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(businessObjectDefinitionHelper, times(2)).safeObjectMapperWriteValueAsString(any(BusinessObjectDefinitionEntity.class));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(any());
        verify(indexFunctionsDao).updateIndexDocuments(eq(SEARCH_INDEX_NAME), ArgumentMatchers.<Map<String, String>>any());
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
        List<Long> businessObjectDefinitionIds =
            new ArrayList<>(Collections.nCopies(BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE + 1, ID));

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);
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
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(
            businessObjectDefinitionIds.subList(0, BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE));
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitionsByIds(businessObjectDefinitionIds
            .subList(BusinessObjectDefinitionServiceImpl.UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE, businessObjectDefinitionIds.size()));
        verify(businessObjectDefinitionHelper).safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(0).get(0));
        verify(businessObjectDefinitionHelper).safeObjectMapperWriteValueAsString(businessObjectDefinitionEntities.get(1).get(0));
        verify(indexFunctionsDao, times(2)).updateIndexDocuments(eq(SEARCH_INDEX_NAME), ArgumentMatchers.<Map<String, String>>any());
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

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);


        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(indexFunctionsDao).deleteIndexDocuments(any(), any());
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

        List<Long> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, "UNKNOWN");

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_NAME);

        // Call the method under test
        businessObjectDefinitionService.updateSearchIndexDocumentBusinessObjectDefinition(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verifyNoMoreInteractionsHelper();
    }

    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper, indexFunctionsDao,
            jsonHelper, tagDaoHelper, tagHelper);
    }
}
