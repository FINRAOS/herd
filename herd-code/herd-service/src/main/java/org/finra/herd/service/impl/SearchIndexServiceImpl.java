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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexKeys;
import org.finra.herd.model.api.xml.SearchIndexStatistics;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.SearchIndexHelperService;
import org.finra.herd.service.SearchIndexService;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexStatusDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;

/**
 * The search index service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SearchIndexServiceImpl implements SearchIndexService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationDaoHelper configurationDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private SearchFunctions searchFunctions;

    @Autowired
    private SearchIndexDao searchIndexDao;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private SearchIndexHelperService searchIndexHelperService;

    @Autowired
    private SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    @Autowired
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Override
    public SearchIndex createSearchIndex(SearchIndexCreateRequest request)
    {
        // Perform validation and trim.
        validateSearchIndexCreateRequest(request);

        // Ensure that search index with the specified search index key doesn't already exist.
        SearchIndexEntity searchIndexEntity = searchIndexDao.getSearchIndexByKey(request.getSearchIndexKey());
        if (searchIndexEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create search index with name \"%s\" because it already exists.", request.getSearchIndexKey().getSearchIndexName()));
        }

        // Get the search index type and ensure it exists.
        SearchIndexTypeEntity searchIndexTypeEntity = searchIndexTypeDaoHelper.getSearchIndexTypeEntity(request.getSearchIndexType());

        // Get the BUILDING search index status entity and ensure it exists.
        SearchIndexStatusEntity searchIndexStatusEntity =
            searchIndexStatusDaoHelper.getSearchIndexStatusEntity(SearchIndexStatusEntity.SearchIndexStatuses.BUILDING.name());

        // Creates the relative search index entity from the request information.
        searchIndexEntity = createSearchIndexEntity(request, searchIndexTypeEntity, searchIndexStatusEntity);

        // Persist the new entity.
        searchIndexEntity = searchIndexDao.saveAndRefresh(searchIndexEntity);

        // Create the search index.
        createSearchIndexHelper(request.getSearchIndexKey(), searchIndexTypeEntity.getCode());

        // Create and return the search index object from the persisted entity.
        return createSearchIndexFromEntity(searchIndexEntity);
    }

    @Override
    public SearchIndex deleteSearchIndex(SearchIndexKey searchIndexKey)
    {
        // Perform validation and trim.
        validateSearchIndexKey(searchIndexKey);

        // Retrieve and ensure that a search index already exists with the specified key.
        SearchIndexEntity searchIndexEntity = searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey);

        // If the index exists delete it.
        deleteSearchIndexHelper(searchIndexEntity.getName());

        // Delete the search index.
        searchIndexDao.delete(searchIndexEntity);

        // Create and return the search index object from the deleted entity.
        return createSearchIndexFromEntity(searchIndexEntity);
    }

    @Override
    public SearchIndex getSearchIndex(SearchIndexKey searchIndexKey)
    {
        // Perform validation and trim.
        validateSearchIndexKey(searchIndexKey);

        // Retrieve and ensure that a search index already exists with the specified key.
        SearchIndexEntity searchIndexEntity = searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey);

        // Create the search index object from the persisted entity.
        SearchIndex searchIndex = createSearchIndexFromEntity(searchIndexEntity);

        // Retrieve index settings from the actual search index. A non-existing search index name results in a "no such index" internal server error.
        Settings settings =
            searchIndexHelperService.getAdminClient().indices().prepareGetIndex().setIndices(searchIndexKey.getSearchIndexName()).execute().actionGet()
                .getSettings().get(searchIndexKey.getSearchIndexName());

        // Retrieve indices level docs stats.
        DocsStats docsStats =
            searchIndexHelperService.getAdminClient().indices().prepareStats(searchIndexKey.getSearchIndexName()).clear().setDocs(true).execute().actionGet()
                .getIndex(searchIndexKey.getSearchIndexName()).getPrimaries().getDocs();

        // Update the search index statistics.
        searchIndex.setSearchIndexStatistics(createSearchIndexStatistics(settings, docsStats));

        return searchIndex;
    }

    @Override
    public SearchIndexKeys getSearchIndexes()
    {
        SearchIndexKeys searchIndexKeys = new SearchIndexKeys();
        searchIndexKeys.getSearchIndexKeys().addAll(searchIndexDao.getSearchIndexes());
        return searchIndexKeys;
    }

    /**
     * Creates a new search index entity from the request information.
     *
     * @param request the information needed to create a search index
     * @param searchIndexTypeEntity the search index type entity
     * @param searchIndexStatusEntity the search index status entity
     *
     * @return the newly created search index entity
     */
    protected SearchIndexEntity createSearchIndexEntity(SearchIndexCreateRequest request, SearchIndexTypeEntity searchIndexTypeEntity,
        SearchIndexStatusEntity searchIndexStatusEntity)
    {
        SearchIndexEntity searchIndexEntity = new SearchIndexEntity();
        searchIndexEntity.setName(request.getSearchIndexKey().getSearchIndexName());
        searchIndexEntity.setType(searchIndexTypeEntity);
        searchIndexEntity.setStatus(searchIndexStatusEntity);
        return searchIndexEntity;
    }

    /**
     * Creates a search index object from the persisted entity.
     *
     * @param searchIndexEntity the search index entity
     *
     * @return the search index
     */
    protected SearchIndex createSearchIndexFromEntity(SearchIndexEntity searchIndexEntity)
    {
        SearchIndex searchIndex = new SearchIndex();
        searchIndex.setSearchIndexKey(new SearchIndexKey(searchIndexEntity.getName()));
        searchIndex.setSearchIndexType(searchIndexEntity.getType().getCode());
        searchIndex.setSearchIndexStatus(searchIndexEntity.getStatus().getCode());
        searchIndex.setCreatedByUserId(searchIndexEntity.getCreatedBy());
        searchIndex.setCreatedOn(HerdDateUtils.getXMLGregorianCalendarValue(searchIndexEntity.getCreatedOn()));
        searchIndex.setLastUpdatedOn(HerdDateUtils.getXMLGregorianCalendarValue(searchIndexEntity.getUpdatedOn()));
        return searchIndex;
    }

    /**
     * Creates a search index.
     *
     * @param searchIndexKey the key of the search index
     * @param searchIndexType the type of the search index
     */
    protected void createSearchIndexHelper(SearchIndexKey searchIndexKey, String searchIndexType)
    {
        String documentType;
        String mapping;

        // Currently, only search index for business object definitions and tag are supported.
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
            mapping = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());

        }
        else if (SearchIndexTypeEntity.SearchIndexTypes.TAG.name().equalsIgnoreCase(searchIndexType))
        {

            documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
            mapping = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_TAG_MAPPINGS_JSON.getKey());
        }
        else
        {
            throw new IllegalArgumentException(String.format("Search index type with code \"%s\" is not supported.", searchIndexType));
        }

        // If the index exists delete it.
        deleteSearchIndexHelper(searchIndexKey.getSearchIndexName());

        // Create the mapping for the index
        searchFunctions.getCreateIndexFunction().accept(searchIndexKey.getSearchIndexName(), documentType, mapping);

        //Fetch data from database and index them
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            // Asynchronously index all business object definitions.
            searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey, documentType);

        }
        else
        {
            // Asynchronously index all tags. If we got to this point, it is tags
            searchIndexHelperService.indexAllTags(searchIndexKey, documentType);
        }
    }

    /**
     * Creates a new search index statistics objects per specified parameters.
     *
     * @param settings the search index settings
     * @param docsStats the search index docs stats
     *
     * @return the newly created search index statistics object
     */
    protected SearchIndexStatistics createSearchIndexStatistics(Settings settings, DocsStats docsStats)
    {
        SearchIndexStatistics searchIndexStatistics = new SearchIndexStatistics();

        Long creationDate = settings.getAsLong(IndexMetaData.SETTING_CREATION_DATE, -1L);
        if (creationDate.longValue() != -1L)
        {
            DateTime creationDateTime = new DateTime(creationDate, DateTimeZone.UTC);
            searchIndexStatistics.setIndexCreationDate(HerdDateUtils.getXMLGregorianCalendarValue(creationDateTime.toDate()));
        }

        searchIndexStatistics.setIndexNumberOfActiveDocuments(docsStats.getCount());
        searchIndexStatistics.setIndexNumberOfDeletedDocuments(docsStats.getDeleted());
        searchIndexStatistics.setIndexUuid(settings.get(IndexMetaData.SETTING_INDEX_UUID));

        return searchIndexStatistics;
    }

    /**
     * Deletes a search index if it exists.
     *
     * @param searchIndexName the name of the search index
     */
    protected void deleteSearchIndexHelper(String searchIndexName)
    {
        // If the index exists delete it.
        if (searchFunctions.getIndexExistsFunction().test(searchIndexName))
        {
            searchFunctions.getDeleteIndexFunction().accept(searchIndexName);
        }
    }

    /**
     * Validates the search index create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateSearchIndexCreateRequest(SearchIndexCreateRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "A search create request must be specified.");
        validateSearchIndexKey(request.getSearchIndexKey());
        request.setSearchIndexType(alternateKeyHelper.validateStringParameter("Search index type", request.getSearchIndexType()));
    }

    /**
     * Validates a search index key. This method also trims the key parameters.
     *
     * @param key the search index key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateSearchIndexKey(SearchIndexKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A search index key must be specified.");
        key.setSearchIndexName(alternateKeyHelper.validateStringParameter("Search index name", key.getSearchIndexName()));
    }
}
