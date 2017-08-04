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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.concurrent.Future;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexStatistics;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.SearchIndexHelperService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexStatusDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;

/**
 * This class tests functionality within the search index service implementation.
 */
public class SearchIndexServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private IndexFunctionsDao indexFunctionDao;

    @Mock
    private SearchIndexDao searchIndexDao;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Mock
    private SearchIndexHelperService searchIndexHelperService;

    @InjectMocks
    private SearchIndexServiceImpl searchIndexServiceImpl;

    @Mock
    private SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    @Mock
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Mock
    private TagDao tagDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSearchIndexEntity()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a search index create request.
        SearchIndexCreateRequest searchIndexCreateRequest = new SearchIndexCreateRequest(searchIndexKey, SEARCH_INDEX_TYPE);

        // Creates a test search index type entity.
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(SEARCH_INDEX_TYPE);

        // Creates a test search index status entity.
        SearchIndexStatusEntity searchIndexStatusEntity = new SearchIndexStatusEntity();
        searchIndexStatusEntity.setCode(SEARCH_INDEX_STATUS);

        // Create a search index entity from the search index create request.
        SearchIndexEntity searchIndexEntity =
            searchIndexServiceImpl.createSearchIndexEntity(searchIndexCreateRequest, searchIndexTypeEntity, searchIndexStatusEntity);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertNotNull(searchIndexEntity);
        assertEquals(SEARCH_INDEX_NAME, searchIndexEntity.getName());
        assertNotNull(searchIndexEntity.getType());
        assertEquals(SEARCH_INDEX_TYPE, searchIndexEntity.getType().getCode());
        assertNotNull(searchIndexEntity.getStatus());
        assertEquals(SEARCH_INDEX_STATUS, searchIndexEntity.getStatus().getCode());
        assertNull(searchIndexEntity.getCreatedBy());
        assertNull(searchIndexEntity.getCreatedOn());
        assertNull(searchIndexEntity.getUpdatedBy());
        assertNull(searchIndexEntity.getUpdatedOn());
    }

    @Test
    public void testCreateSearchIndexFromEntity()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Creates a test search index type entity.
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(SEARCH_INDEX_TYPE);

        // Creates a test search index status entity.
        SearchIndexStatusEntity searchIndexStatusEntity = new SearchIndexStatusEntity();
        searchIndexStatusEntity.setCode(SEARCH_INDEX_STATUS);

        // Create a test search index entity.
        SearchIndexEntity searchIndexEntity = new SearchIndexEntity();
        searchIndexEntity.setName(SEARCH_INDEX_NAME);
        searchIndexEntity.setType(searchIndexTypeEntity);
        searchIndexEntity.setStatus(searchIndexStatusEntity);
        searchIndexEntity.setCreatedBy(USER_ID);
        searchIndexEntity.setCreatedOn(new Timestamp(CREATED_ON.toGregorianCalendar().getTimeInMillis()));
        searchIndexEntity.setUpdatedOn(new Timestamp(UPDATED_ON.toGregorianCalendar().getTimeInMillis()));

        // Create a search index object from the search index entity.
        SearchIndex searchIndex = searchIndexServiceImpl.createSearchIndexFromEntity(searchIndexEntity);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, NO_SEARCH_INDEX_STATISTICS, USER_ID, CREATED_ON, UPDATED_ON),
            searchIndex);
    }

    @Test
    public void testCreateSearchIndexHelper()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();

        // Mock some of the external call responses.
        @SuppressWarnings("unchecked")
        Future<Void> mockedFuture = mock(Future.class);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey())).thenReturn(SEARCH_INDEX_MAPPING);
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SETTINGS_JSON.getKey())).thenReturn(SEARCH_INDEX_SETTINGS);
        when(indexFunctionDao.isIndexExists(SEARCH_INDEX_NAME)).thenReturn(true);
        doNothing().when(indexFunctionDao).deleteIndex(SEARCH_INDEX_NAME);

        when(searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE)).thenReturn(mockedFuture);

        // Create a search index.
        searchIndexServiceImpl.createSearchIndexHelper(searchIndexKey, searchIndexType);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(configurationDaoHelper).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        verify(configurationDaoHelper).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SETTINGS_JSON.getKey());
        verify(indexFunctionDao).isIndexExists(SEARCH_INDEX_NAME);
        verify(indexFunctionDao).deleteIndex(SEARCH_INDEX_NAME);
        verify(indexFunctionDao).createIndex(any(), any() , any(), any());
        verify(searchIndexHelperService).indexAllBusinessObjectDefinitions(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);
    }

    @Test
    public void testCreateSearchIndexHelperInvalidSearchIndexType()
    {
        // Try to create a search index using an invalid search index type.
        try
        {
            searchIndexServiceImpl.createSearchIndexHelper(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_TYPE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Search index type with code \"%s\" is not supported.", SEARCH_INDEX_TYPE), e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);
    }

    @Test
    public void testCreateSearchIndexStatisticsNoIndexCreationDate()
    {
        // Create a search index get settings response without the index creation date setting.
        ImmutableOpenMap<String, Settings> getIndexResponseSettings = ImmutableOpenMap.<String, Settings>builder()
            .fPut(SEARCH_INDEX_NAME, Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, SEARCH_INDEX_STATISTICS_INDEX_UUID).build()).build();

        // Mock an index docs stats object.
        DocsStats mockedDocsStats = mock(DocsStats.class);
        when(mockedDocsStats.getCount()).thenReturn(SEARCH_INDEX_STATISTICS_NUMBER_OF_ACTIVE_DOCUMENTS);
        when(mockedDocsStats.getDeleted()).thenReturn(SEARCH_INDEX_STATISTICS_NUMBER_OF_DELETED_DOCUMENTS);

        // Get a search index settings created.
        SearchIndexStatistics response = searchIndexServiceImpl.createSearchIndexStatistics(getIndexResponseSettings.get(SEARCH_INDEX_NAME), mockedDocsStats, 0l);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndexStatistics(NO_SEARCH_INDEX_STATISTICS_CREATION_DATE, SEARCH_INDEX_STATISTICS_NUMBER_OF_ACTIVE_DOCUMENTS,
            SEARCH_INDEX_STATISTICS_NUMBER_OF_DELETED_DOCUMENTS, SEARCH_INDEX_STATISTICS_INDEX_UUID, 0l), response);
    }

    @Test
    public void testDeleteSearchIndexHelperIndexAlreadyExists()
    {
        // Mock the external calls.
        when(indexFunctionDao.isIndexExists(SEARCH_INDEX_NAME)).thenReturn(true);
        doNothing().when(indexFunctionDao).deleteIndex(SEARCH_INDEX_NAME);

        // Call a delete index method when a search index exists.
        searchIndexServiceImpl.deleteSearchIndexHelper(SEARCH_INDEX_NAME);

        verify(indexFunctionDao).isIndexExists(SEARCH_INDEX_NAME);
        verify(indexFunctionDao).deleteIndex(SEARCH_INDEX_NAME);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);
    }

    @Test
    public void testDeleteSearchIndexHelperIndexNoExists()
    {
        // Mock the external calls.
        when(indexFunctionDao.isIndexExists(SEARCH_INDEX_NAME)).thenReturn(false);

        // Call a delete index method when a search index does not exists.
        searchIndexServiceImpl.deleteSearchIndexHelper(SEARCH_INDEX_NAME);

        verify(indexFunctionDao).isIndexExists(SEARCH_INDEX_NAME);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);
    }
}
