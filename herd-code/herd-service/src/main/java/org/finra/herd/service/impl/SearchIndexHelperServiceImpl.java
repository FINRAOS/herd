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

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.SearchIndexHelperService;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.TagDaoHelper;

/**
 * An implementation of the helper service class for the search index service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SearchIndexHelperServiceImpl implements SearchIndexHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexHelperServiceImpl.class);

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private SearchFunctions searchFunctions;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private TransportClient transportClient;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Override
    public AdminClient getAdminClient()
    {
        return transportClient.admin();
    }

    @Override
    @Async
    public Future<Void> indexAllBusinessObjectDefinitions(SearchIndexKey searchIndexKey, String documentType)
    {
        // Get a list of all business object definitions.
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities =
            Collections.unmodifiableList(businessObjectDefinitionDao.getAllBusinessObjectDefinitions());

        // Index all business object definitions.
        businessObjectDefinitionHelper
            .executeFunctionForBusinessObjectDefinitionEntities(searchIndexKey.getSearchIndexName(), documentType, businessObjectDefinitionEntities,
                searchFunctions.getIndexFunction());

        // Simple count validation, index size should equal entity list size.
        final long indexSize = searchFunctions.getNumberOfTypesInIndexFunction().apply(searchIndexKey.getSearchIndexName(), documentType);
        final long businessObjectDefinitionDatabaseTableSize = businessObjectDefinitionEntities.size();
        if (businessObjectDefinitionDatabaseTableSize != indexSize)
        {
            LOGGER.error("Index validation failed, business object definition database table size {}, does not equal index size {}.",
                businessObjectDefinitionDatabaseTableSize, indexSize);
        }

        // Update search index status to READY.
        searchIndexDaoHelper.updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they can call
        // "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    @Override
    @Async
    public Future<Void> indexAllTags(SearchIndexKey searchIndexKey, String documentType)
    {
        // Get a list of all tags
        final List<TagEntity> tagEntities = Collections.unmodifiableList(tagDao.getTags());

        // Index all tags.
        tagDaoHelper.executeFunctionForTagEntities(searchIndexKey.getSearchIndexName(), documentType, tagEntities, searchFunctions.getIndexFunction());

        // Simple count validation, index size should equal entity list size.
        final long indexSize = searchFunctions.getNumberOfTypesInIndexFunction().apply(searchIndexKey.getSearchIndexName(), documentType);
        final long tagDatabaseTableSize = tagEntities.size();
        if (tagDatabaseTableSize != indexSize)
        {
            LOGGER.error("Index validation failed, tag database table size {}, does not equal index size {}.", tagDatabaseTableSize, indexSize);
        }

        // Update search index status to READY.
        searchIndexDaoHelper.updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they can call
        // "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }
}
