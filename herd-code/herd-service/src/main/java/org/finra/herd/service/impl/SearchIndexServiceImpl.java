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

import java.sql.Timestamp;

import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
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
    private SearchIndexDao searchIndexDao;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private SearchIndexHelperService searchIndexHelperService;

    @Autowired
    private SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    @Autowired
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Autowired
    private IndexFunctionsDao indexFunctionsDao;

    @Override
    public SearchIndex createSearchIndex(SearchIndexCreateRequest request)
    {
        // Perform validation and trim.
        validateSearchIndexCreateRequest(request);

        // Get the search index type and ensure it exists.
        SearchIndexTypeEntity searchIndexTypeEntity = searchIndexTypeDaoHelper.getSearchIndexTypeEntity(request.getSearchIndexType());

        // Get the BUILDING search index status entity and ensure it exists.
        SearchIndexStatusEntity searchIndexStatusEntity =
            searchIndexStatusDaoHelper.getSearchIndexStatusEntity(SearchIndexStatusEntity.SearchIndexStatuses.BUILDING.name());

        // Creates the relative search index entity from the request information.
        SearchIndexEntity searchIndexEntity = createSearchIndexEntity(request, searchIndexTypeEntity, searchIndexStatusEntity);

        // Persist the new entity.
        searchIndexEntity = searchIndexDao.saveAndRefresh(searchIndexEntity);

        // Create search index key with the auto-generated search index name
        SearchIndexKey searchIndexKey = new SearchIndexKey();
        searchIndexKey.setSearchIndexName(searchIndexEntity.getName());

        // Create the search index.
        createSearchIndexHelper(searchIndexKey, searchIndexTypeEntity.getCode());

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
        DocsStats docsStats = indexFunctionsDao.getIndexStats(searchIndexKey.getSearchIndexName());
        // Retrieve index settings from the actual search index. A non-existing search index name results in a "no such index" internal server error.
        Settings settings = indexFunctionsDao.getIndexSettings(searchIndexKey.getSearchIndexName());

        long indexCount = indexFunctionsDao.getNumberOfTypesInIndex(searchIndexKey.getSearchIndexName());

        // Update the search index statistics.
        searchIndex.setSearchIndexStatistics(createSearchIndexStatistics(settings, docsStats, indexCount));

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
        searchIndexEntity.setName(setSearchIndexName(request.getSearchIndexType()));
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
        if (searchIndexEntity.getActive() != null)
        {
            searchIndex.setActive(searchIndexEntity.getActive());
        }
        else
        {
            searchIndex.setActive(Boolean.FALSE);
        }
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
        String mapping;
        String settings;
        String alias;

        // Currently, only search index for business object definitions and tag are supported.
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            mapping = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON_V2.getKey());
            settings = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SETTINGS_JSON_V2.getKey());
            alias = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        }
        else if (SearchIndexTypeEntity.SearchIndexTypes.TAG.name().equalsIgnoreCase(searchIndexType))
        {
            mapping = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_TAG_MAPPINGS_JSON_V2.getKey());
            settings = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_TAG_SETTINGS_JSON_V2.getKey());
            alias = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_INDEX_NAME, String.class);
        }
        else
        {
            throw new IllegalArgumentException(String.format("Search index type with code \"%s\" is not supported.", searchIndexType));
        }

        // Create the index.
        indexFunctionsDao.createIndex(searchIndexKey.getSearchIndexName(), mapping, settings, alias);

        //Fetch data from database and index them
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            // Asynchronously index all business object definitions.
            searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey);
        }
        else
        {
            // Asynchronously index all tags. If we got to this point, it is tags
            searchIndexHelperService.indexAllTags(searchIndexKey);
        }
    }


    /**
     * Creates a new search index statistics objects per specified parameters.
     *
     * @param settings the search index settings
     * @param docsStats the search index docs stats
     * @param indexCount the count of index
     *
     * @return the newly created search index statistics object
     */
    protected SearchIndexStatistics createSearchIndexStatistics(Settings settings, DocsStats docsStats, long indexCount)
    {
        SearchIndexStatistics searchIndexStatistics = new SearchIndexStatistics();

        Long creationDate = settings.getAsLong(IndexMetadata.SETTING_CREATION_DATE, -1L);
        if (creationDate != -1L)
        {
            DateTime creationDateTime = new DateTime(creationDate, DateTimeZone.UTC);
            searchIndexStatistics.setIndexCreationDate(HerdDateUtils.getXMLGregorianCalendarValue(creationDateTime.toDate()));
        }

        searchIndexStatistics.setIndexNumberOfActiveDocuments(docsStats.getCount());
        searchIndexStatistics.setIndexNumberOfDeletedDocuments(docsStats.getDeleted());
        searchIndexStatistics.setIndexUuid(settings.get(IndexMetadata.SETTING_INDEX_UUID));
        searchIndexStatistics.setIndexCount(indexCount);

        return searchIndexStatistics;
    }

    /**
     * Deletes a search index if it exists.
     *
     * @param searchIndexName the name of the search index
     */
    protected void deleteSearchIndexHelper(String searchIndexName)
    {
        if (indexFunctionsDao.isIndexExists(searchIndexName))
        {
            indexFunctionsDao.deleteIndex(searchIndexName);
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
        Assert.notNull(request, "A search index create request must be specified.");
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

    /**
     * Constructs a search index key with name followed by current timestamp
     *
     * @param searchIndexType the type of the search index
     *
     * @return indexName the name of the index
     */
    private String setSearchIndexName(String searchIndexType)
    {

        String indexName;
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            indexName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        }
        else
        {
            indexName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_INDEX_NAME, String.class);
        }
        indexName += "_" + new Timestamp(System.currentTimeMillis()).getTime();
        return indexName;
    }
}
