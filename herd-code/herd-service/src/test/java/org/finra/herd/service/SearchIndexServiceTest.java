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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import org.elasticsearch.cluster.metadata.IndexMetaData;
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
import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;
import org.finra.herd.model.api.xml.SearchIndexStatistics;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
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
    private IndexFunctionsDao indexFunctionsDao;

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
        SearchIndexCreateRequest searchIndexCreateRequest = new SearchIndexCreateRequest(searchIndexType);

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
        searchIndexEntity.setActive(Boolean.FALSE);

        // Mock some of the external call responses.
        @SuppressWarnings("unchecked")
        Future<Void> mockedFuture = mock(Future.class);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index type", searchIndexType)).thenReturn(searchIndexType);
        when(searchIndexTypeDaoHelper.getSearchIndexTypeEntity(searchIndexType)).thenReturn(searchIndexTypeEntity);
        when(searchIndexStatusDaoHelper.getSearchIndexStatusEntity(searchIndexStatus)).thenReturn(searchIndexStatusEntity);
        when(searchIndexDao.saveAndRefresh(any(SearchIndexEntity.class))).thenReturn(searchIndexEntity);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class)).thenReturn(SEARCH_INDEX_ALIAS_BDEF);
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON_V2.getKey())).thenReturn(SEARCH_INDEX_MAPPING);
        when(configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SETTINGS_JSON_V2.getKey())).thenReturn(SEARCH_INDEX_SETTINGS);

        when(searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey)).thenReturn(mockedFuture);

        // Create a search index.
        SearchIndex response = searchIndexService.createSearchIndex(searchIndexCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index type", searchIndexType);
        verify(searchIndexTypeDaoHelper).getSearchIndexTypeEntity(searchIndexType);
        verify(searchIndexStatusDaoHelper).getSearchIndexStatusEntity(searchIndexStatus);
        verify(searchIndexDao).saveAndRefresh(any(SearchIndexEntity.class));
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        verify(configurationDaoHelper).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON_V2.getKey());
        verify(configurationDaoHelper).getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SETTINGS_JSON_V2.getKey());
        verify(indexFunctionsDao).createIndex(any(), any(), any(), any());

        verify(searchIndexHelperService).indexAllBusinessObjectDefinitions(searchIndexKey);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionsDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, searchIndexType, searchIndexStatus, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG, NO_SEARCH_INDEX_STATISTICS, USER_ID,
                CREATED_ON, UPDATED_ON),
            response);
    }

    @Test
    public void testDeleteSearchIndex()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create the search index entity.
        SearchIndexEntity searchIndexEntity = createTestSearchIndexEntity();

        // Mock the external calls.
        when(indexFunctionsDao.isIndexExists(any())).thenReturn(true);
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);


        // Delete a search index.
        SearchIndex response = searchIndexService.deleteSearchIndex(searchIndexKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);
        verify(indexFunctionsDao).isIndexExists(any());
        verify(indexFunctionsDao).deleteIndex(any());

        verify(searchIndexDao).delete(searchIndexEntity);
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionsDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        // Validate the returned object.
        assertEquals(
            new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE_BDEF, SEARCH_INDEX_STATUS, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG, NO_SEARCH_INDEX_STATISTICS, USER_ID,
                CREATED_ON, UPDATED_ON),
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
        @SuppressWarnings("unchecked")
        DocsStats mockedDocsStats = mock(DocsStats.class);

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, SEARCH_INDEX_STATISTICS_INDEX_UUID)
            .put(IndexMetaData.SETTING_CREATION_DATE,  Long.toString(SEARCH_INDEX_STATISTICS_CREATION_DATE.toGregorianCalendar().getTimeInMillis())).build();
        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);

        when(indexFunctionsDao.getIndexSettings(SEARCH_INDEX_NAME)).thenReturn(settings);
        when(indexFunctionsDao.getIndexStats(SEARCH_INDEX_NAME)).thenReturn(mockedDocsStats);
        when(mockedDocsStats.getCount()).thenReturn(SEARCH_INDEX_STATISTICS_NUMBER_OF_ACTIVE_DOCUMENTS);
        when(mockedDocsStats.getDeleted()).thenReturn(SEARCH_INDEX_STATISTICS_NUMBER_OF_DELETED_DOCUMENTS);
        when(indexFunctionsDao.getNumberOfTypesInIndex(any())).thenReturn(0L);

        // Get a search index.
        SearchIndex response = searchIndexService.getSearchIndex(searchIndexKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);
        verify(mockedDocsStats).getCount();
        verify(mockedDocsStats).getDeleted();
        verify(indexFunctionsDao).getIndexSettings(SEARCH_INDEX_NAME);
        verify(indexFunctionsDao).getIndexStats(SEARCH_INDEX_NAME);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any());

        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionHelper, configurationDaoHelper, configurationHelper,
            indexFunctionsDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

        //response.getSearchIndexStatistics().setIndexCreationDate(SEARCH_INDEX_STATISTICS_CREATION_DATE);
        // Validate the returned object.
        assertEquals(new SearchIndex(searchIndexKey, SEARCH_INDEX_TYPE_BDEF, SEARCH_INDEX_STATUS, SEARCH_INDEX_DEFAULT_ACTIVE_FLAG,
            new SearchIndexStatistics(SEARCH_INDEX_STATISTICS_CREATION_DATE, SEARCH_INDEX_STATISTICS_NUMBER_OF_ACTIVE_DOCUMENTS,
                SEARCH_INDEX_STATISTICS_NUMBER_OF_DELETED_DOCUMENTS, SEARCH_INDEX_STATISTICS_INDEX_UUID, 0L), USER_ID, CREATED_ON, UPDATED_ON), response);
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
            indexFunctionsDao, searchIndexDao, searchIndexDaoHelper, searchIndexHelperService, searchIndexStatusDaoHelper, searchIndexTypeDaoHelper);

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
        searchIndexTypeEntity.setCode(SEARCH_INDEX_TYPE_BDEF);

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
        searchIndexEntity.setActive(Boolean.FALSE);

        return searchIndexEntity;
    }
}
