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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;
import org.finra.herd.model.api.xml.SearchIndexSettings;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexStatusDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;
import org.finra.herd.service.impl.SearchIndexServiceImpl;

/**
 * This class tests search index functionality within the search index service.
 */
public class SearchIndexServiceTest extends AbstractServiceTest
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
    private SearchFunctions searchFunctions;

    @Mock
    private SearchIndexDao searchIndexDao;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Mock
    private SearchIndexHelperService searchIndexHelperService;

    @InjectMocks
    private SearchIndexServiceImpl searchIndexService;

    @Mock
    private SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    @Mock
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();

        // Get the search index status value.
        String searchIndexStatus = SearchIndexStatusEntity.SearchIndexStatuses.BUILDING.name();

        // Create a search index create request.
        SearchIndexCreateRequest searchIndexCreateRequest = new SearchIndexCreateRequest(searchIndexKey, searchIndexType);

        // Creates a test search index type entity.
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(searchIndexType);

        // Creates a test search index status entity.
        SearchIndexStatusEntity searchIndexStatusEntity = new SearchIndexStatusEntity();
        searchIndexStatusEntity.setCode(searchIndexStatus);

        // Creates a test search index entity.
        SearchIndexEntity searchIndexEntity = new SearchIndexEntity();
        searchIndexEntity.setName(SEARCH_INDEX_NAME);
        searchIndexEntity.setType(searchIndexTypeEntity);
        searchIndexEntity.setStatus(searchIndexStatusEntity);
        searchIndexEntity.setCreatedBy(USER_ID);
        searchIndexEntity.setCreatedOn(new Timestamp(CREATED_ON.toGregorianCalendar().getTimeInMillis()));
        searchIndexEntity.setUpdatedOn(new Timestamp(UPDATED_ON.toGregorianCalendar().getTimeInMillis()));

        // Mock some of the external call responses.
        @SuppressWarnings("unchecked")
        Future<Void> mockedFuture = mock(Future.class);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(alternateKeyHelper.validateStringParameter("Search index type", searchIndexType)).thenReturn(searchIndexType);
        when(searchIndexDao.getSearchIndexByKey(searchIndexKey)).thenReturn(null);
        when(searchIndexTypeDaoHelper.getSearchIndexTypeEntity(searchIndexType)).thenReturn(searchIndexTypeEntity);
        when(searchIndexStatusDaoHelper.getSearchIndexStatusEntity(searchIndexStatus)).thenReturn(searchIndexStatusEntity);
        when(searchIndexDao.saveAndRefresh(any(SearchIndexEntity.class))).thenReturn(searchIndexEntity);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn(SEARCH_INDEX_DOCUMENT_TYPE);
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey())).thenReturn(SEARCH_INDEX_MAPPING);
        when(searchFunctions.getIndexExistsFunction()).thenReturn(indexName -> true);
        when(searchFunctions.getDeleteIndexFunction()).thenReturn(indexName -> {
        });
        when(searchFunctions.getCreateIndexFunction()).thenReturn((indexName, documentType, mapping) -> {
        });
        when(searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE)).thenReturn(mockedFuture);

        // Create a search index.
        SearchIndex response = searchIndexService.createSearchIndex(searchIndexCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(alternateKeyHelper).validateStringParameter("Search index type", searchIndexType);
        verify(searchIndexDao).getSearchIndexByKey(searchIndexKey);
        verify(searchIndexTypeDaoHelper).getSearchIndexTypeEntity(searchIndexType);
        verify(searchIndexStatusDaoHelper).getSearchIndexStatusEntity(searchIndexStatus);
        verify(searchIndexDao).saveAndRefresh(any(SearchIndexEntity.class));
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(configurationDaoHelper).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        verify(searchFunctions).getIndexExistsFunction();
        verify(searchFunctions).getDeleteIndexFunction();
        verify(searchFunctions).getCreateIndexFunction();
        verify(searchIndexHelperService).indexAllBusinessObjectDefinitions(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchFunctions, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, searchIndexType, searchIndexStatus, NO_SEARCH_INDEX_SETTINGS, USER_ID, CREATED_ON, UPDATED_ON), response);
    }

    @Test
    public void testCreateSearchIndexSearchIndexAlreadyExists()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(alternateKeyHelper.validateStringParameter("Search index type", SEARCH_INDEX_TYPE)).thenReturn(SEARCH_INDEX_TYPE);
        when(searchIndexDao.getSearchIndexByKey(searchIndexKey)).thenReturn(new SearchIndexEntity());

        // Try to create a search index when search index entity already exists.
        try
        {
            searchIndexService.createSearchIndex(new SearchIndexCreateRequest(searchIndexKey, SEARCH_INDEX_TYPE));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create Search index with name \"%s\" because it already exists.", SEARCH_INDEX_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(alternateKeyHelper).validateStringParameter("Search index type", SEARCH_INDEX_TYPE);
        verify(searchIndexDao).getSearchIndexByKey(searchIndexKey);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchFunctions, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);
    }

    @Test
    public void testDeleteSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create the search index entity.
        SearchIndexEntity searchIndexEntity = createTestSearchIndexEntity();

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);
        when(searchFunctions.getIndexExistsFunction()).thenReturn(SEARCH_INDEX_NAME -> true);
        when(searchFunctions.getDeleteIndexFunction()).thenReturn(SEARCH_INDEX_NAME -> {
        });

        // Delete a search index.
        SearchIndex response = searchIndexService.deleteSearchIndex(searchIndexKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);
        verify(searchFunctions).getIndexExistsFunction();
        verify(searchFunctions).getDeleteIndexFunction();
        verify(searchIndexDao).delete(searchIndexEntity);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchFunctions, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS, NO_SEARCH_INDEX_SETTINGS, USER_ID, CREATED_ON, UPDATED_ON),
            response);
    }

    @Test
    public void testGetSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create the search index entity.
        SearchIndexEntity searchIndexEntity = createTestSearchIndexEntity();

        // Mock some of the external call responses.
        AdminClient mockedAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockedIndiciesAdminClient = mock(IndicesAdminClient.class);
        GetIndexRequestBuilder mockedGetIndexRequestBuilder = mock(GetIndexRequestBuilder.class);
        @SuppressWarnings("unchecked")
        ListenableActionFuture<GetIndexResponse> mockedListenableActionFutureActionResponse = mock(ListenableActionFuture.class);
        GetIndexResponse mockedGetIndexResponse = mock(GetIndexResponse.class);

        // Create a search index get settings response.
        ImmutableOpenMap<String, Settings> getIndexResponseSettings = ImmutableOpenMap.<String, Settings>builder().fPut(SEARCH_INDEX_NAME,
            Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, SEARCH_INDEX_SETTING_CREATION_DATE)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, SEARCH_INDEX_SETTING_NUMBER_OF_REPLICAS)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, SEARCH_INDEX_SETTING_NUMBER_OF_SHARDS)
                .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, SEARCH_INDEX_SETTING_INDEX_PROVIDED_NAME)
                .put(IndexMetaData.SETTING_INDEX_UUID, SEARCH_INDEX_SETTING_INDEX_UUID).build()).build();

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);
        when(searchIndexHelperService.getAdminClient()).thenReturn(mockedAdminClient);
        when(mockedAdminClient.indices()).thenReturn(mockedIndiciesAdminClient);
        when(mockedIndiciesAdminClient.prepareGetIndex()).thenReturn(mockedGetIndexRequestBuilder);
        when(mockedGetIndexRequestBuilder.setIndices(SEARCH_INDEX_NAME)).thenReturn(mockedGetIndexRequestBuilder);
        when(mockedGetIndexRequestBuilder.execute()).thenReturn(mockedListenableActionFutureActionResponse);
        when(mockedListenableActionFutureActionResponse.actionGet()).thenReturn(mockedGetIndexResponse);
        when(mockedGetIndexResponse.getSettings()).thenReturn(getIndexResponseSettings);

        // Get a search index.
        SearchIndex response = searchIndexService.getSearchIndex(searchIndexKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);
        verify(searchIndexHelperService).getAdminClient();
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchFunctions, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS,
            new SearchIndexSettings(SEARCH_INDEX_SETTING_CREATION_DATE, SEARCH_INDEX_SETTING_NUMBER_OF_REPLICAS, SEARCH_INDEX_SETTING_NUMBER_OF_SHARDS,
                SEARCH_INDEX_SETTING_INDEX_PROVIDED_NAME, SEARCH_INDEX_SETTING_INDEX_UUID), USER_ID, CREATED_ON, UPDATED_ON), response);
    }

    @Test
    public void testGetSearchIndexes()
    {
        // Create a list of search index keys.
        List<SearchIndexKey> searchIndexKeys = Arrays.asList(new SearchIndexKey(SEARCH_INDEX_NAME), new SearchIndexKey(SEARCH_INDEX_NAME_2));

        // Mock the external calls.
        when(searchIndexDao.getSearchIndexes()).thenReturn(searchIndexKeys);

        // Get search indexes.
        SearchIndexKeys response = searchIndexService.getSearchIndexes();

        // Verify the external calls.
        verify(searchIndexDao).getSearchIndexes();
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            searchFunctions, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndexKeys(searchIndexKeys), response);
    }

    /**
     * Creates a test search index entity along with the relative database entities.
     *
     * @return the search index entity
     */
    private SearchIndexEntity createTestSearchIndexEntity()
    {
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

        return searchIndexEntity;
    }
}
