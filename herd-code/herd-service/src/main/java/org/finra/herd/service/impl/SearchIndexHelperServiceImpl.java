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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.SearchIndexHelperService;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.TagHelper;

/**
 * An implementation of the helper service class for the search index service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SearchIndexHelperServiceImpl implements SearchIndexHelperService
{
    public static final int BUSINESS_OBJECT_DEFINITIONS_CHUNK_SIZE = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexHelperServiceImpl.class);

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagHelper tagHelper;

    @Autowired
    private IndexFunctionsDao indexFunctionsDao;

    @Override
    @Async
    public Future<Void> indexAllBusinessObjectDefinitions(SearchIndexKey searchIndexKey)
    {
        // Index all business object definitions defined in the system using pagination.
        int startPosition = 0;
        int processedBusinessObjectDefinitionsCount = 0;
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities;
        while ((businessObjectDefinitionEntities =
            businessObjectDefinitionDao.getAllBusinessObjectDefinitions(startPosition, BUSINESS_OBJECT_DEFINITIONS_CHUNK_SIZE)).size() > 0)
        {
            // Index business object definitions selected for processing.
            businessObjectDefinitionHelper
                .executeFunctionForBusinessObjectDefinitionEntities(searchIndexKey.getSearchIndexName(), businessObjectDefinitionEntities,
                    indexFunctionsDao::createIndexDocument);

            // Increment the offset.
            startPosition += BUSINESS_OBJECT_DEFINITIONS_CHUNK_SIZE;

            // Increment the total count of processed business object definition entities.
            processedBusinessObjectDefinitionsCount += businessObjectDefinitionEntities.size();
        }

        // Perform a simple count validation, index size should equal entity list size.
        validateSearchIndexSize(searchIndexKey.getSearchIndexName(), processedBusinessObjectDefinitionsCount);

        // Update search index status to READY.
        searchIndexDaoHelper.updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they can call
        // "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    @Override
    @Async
    public Future<Void> indexAllTags(SearchIndexKey searchIndexKey)
    {
        // Get a list of all tags
        final List<TagEntity> tagEntities = Collections.unmodifiableList(tagDao.getTags());

        // Index all tags.
        tagHelper.executeFunctionForTagEntities(searchIndexKey.getSearchIndexName(), tagEntities, indexFunctionsDao::createIndexDocument);

        // Simple count validation, index size should equal entity list size.
        validateSearchIndexSize(searchIndexKey.getSearchIndexName(), tagEntities.size());

        // Update search index status to READY.
        searchIndexDaoHelper.updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they can call
        // "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * Performs a simple count validation on the specified search index.
     *
     * @param indexName the name of the index
     * @param expectedIndexSize the expected index size
     *
     * @return true if index size matches the expected size, false otherwise
     */
    protected boolean validateSearchIndexSize(String indexName, int expectedIndexSize)
    {
        final long indexSize = indexFunctionsDao.getNumberOfTypesInIndex(indexName);

        boolean result = true;
        if (indexSize != expectedIndexSize)
        {
            LOGGER.error("Index validation failed, expected index size {}, does not equal actual index size {}.", expectedIndexSize, indexSize);
            result = false;
        }

        return result;
    }
}
