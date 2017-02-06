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

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.ElasticsearchIndexDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.ElasticsearchIndex;
import org.finra.herd.model.api.xml.ElasticsearchIndexCreateRequest;
import org.finra.herd.model.api.xml.ElasticsearchIndexKey;
import org.finra.herd.model.api.xml.ElasticsearchIndexKeys;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ElasticsearchIndexEntity;
import org.finra.herd.model.jpa.ElasticsearchIndexTypeEntity;
import org.finra.herd.service.ElasticsearchIndexService;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.ElasticsearchIndexDaoHelper;
import org.finra.herd.service.helper.ElasticsearchIndexTypeDaoHelper;

/**
 * The Elasticsearch index service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ElasticsearchIndexServiceImpl implements ElasticsearchIndexService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIndexServiceImpl.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationDaoHelper configurationDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private ElasticsearchIndexDao elasticsearchIndexDao;

    @Autowired
    private ElasticsearchIndexDaoHelper elasticsearchIndexDaoHelper;

    @Autowired
    private ElasticsearchIndexTypeDaoHelper elasticsearchIndexTypeDaoHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private SearchFunctions searchFunctions;

    /**
     * The transport client is a connection to the elasticsearch index
     */
    @Autowired
    private TransportClient transportClient;

    @Override
    public ElasticsearchIndex createElasticsearchIndex(ElasticsearchIndexCreateRequest request)
    {
        // Perform validation and trim.
        validateElasticsearchIndexCreateRequest(request);

        // Ensure that Elasticsearch index with the specified Elasticsearch index key doesn't already exist.
        ElasticsearchIndexEntity elasticsearchIndexEntity = elasticsearchIndexDao.getElasticsearchIndexByKey(request.getElasticsearchIndexKey());
        if (elasticsearchIndexEntity != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create Elasticsearch index with name \"%s\" because it already exists.",
                request.getElasticsearchIndexKey().getElasticsearchIndexName()));
        }

        // Get the Elasticsearch index type and ensure it exists.
        ElasticsearchIndexTypeEntity elasticsearchIndexTypeEntity =
            elasticsearchIndexTypeDaoHelper.getElasticsearchIndexTypeEntity(request.getElasticsearchIndexType());

        // Create the Elasticsearch index.
        createElasticsearchIndex(request.getElasticsearchIndexKey().getElasticsearchIndexName(), elasticsearchIndexTypeEntity.getCode());

        // Creates the relative Elasticsearch index entity from the request information.
        elasticsearchIndexEntity = createElasticsearchIndexEntity(request, elasticsearchIndexTypeEntity);

        // Persist the new entity.
        elasticsearchIndexEntity = elasticsearchIndexDao.saveAndRefresh(elasticsearchIndexEntity);

        // Create and return the Elasticsearch index object from the persisted entity.
        return createElasticsearchIndexFromEntity(elasticsearchIndexEntity);
    }

    @Override
    public ElasticsearchIndex deleteElasticsearchIndex(ElasticsearchIndexKey elasticsearchIndexKey)
    {
        // Perform validation and trim.
        validateElasticsearchIndexKey(elasticsearchIndexKey);

        // Retrieve and ensure that an Elasticsearch index already exists with the specified key.
        ElasticsearchIndexEntity elasticsearchIndexEntity = elasticsearchIndexDaoHelper.getElasticsearchIndexEntity(elasticsearchIndexKey);

        // If the index exists delete it.
        deleteElasticsearchIndex(elasticsearchIndexEntity.getName());

        // Delete the Elasticsearch index.
        elasticsearchIndexDao.delete(elasticsearchIndexEntity);

        // Create and return the Elasticsearch index object from the deleted entity.
        return createElasticsearchIndexFromEntity(elasticsearchIndexEntity);
    }

    @Override
    public ElasticsearchIndex getElasticsearchIndex(ElasticsearchIndexKey elasticsearchIndexKey)
    {
        // Perform validation and trim.
        validateElasticsearchIndexKey(elasticsearchIndexKey);

        /*
        // Retrieve and ensure that an Elasticsearch index already exists with the specified key.
        ElasticsearchIndexEntity elasticsearchIndexEntity = elasticsearchIndexDaoHelper.getElasticsearchIndexEntity(elasticsearchIndexKey);

        // Create and return the Elasticsearch index object from the persisted entity.
        return createElasticsearchIndexFromEntity(elasticsearchIndexEntity);
        */

        ClusterStatsIndices clusterStatsIndices = transportClient.admin().cluster().prepareClusterStats().execute().actionGet().getIndicesStats();

        LOGGER.info("clusterStatsIndices={}", jsonHelper.objectToJson(clusterStatsIndices));

        ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();
        elasticsearchIndex.setElasticsearchIndexKey(elasticsearchIndexKey);
        elasticsearchIndex.setElasticsearchIndexType("");

        return elasticsearchIndex;
    }

    @Override
    public ElasticsearchIndexKeys getElasticsearchIndexes()
    {
        ElasticsearchIndexKeys elasticsearchIndexKeys = new ElasticsearchIndexKeys();
        elasticsearchIndexKeys.getElasticsearchIndexKeys().addAll(elasticsearchIndexDao.getElasticsearchIndexes());
        return elasticsearchIndexKeys;
    }

    /**
     * Creates an Elasticsearch index.
     *
     * @param elasticsearchIndexName the name of the Elasticsearch index
     * @param elasticsearchIndexType the type of the Elasticsearch index
     */
    private void createElasticsearchIndex(String elasticsearchIndexName, String elasticsearchIndexType)
    {
        String documentType;
        String mapping;

        // Currently, only Elasticsearch index for business object definitions is supported.
        if (ElasticsearchIndexTypeEntity.ElasticsearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(elasticsearchIndexType))
        {
            documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
            mapping = configurationDaoHelper.getClobProperty(ConfigurationValue.ELASTICSEARCH_BDEF_MAPPINGS_JSON.getKey());
        }
        else
        {
            throw new IllegalArgumentException(String.format("Elasticsearch index type with code \"%s\" is not supported.", elasticsearchIndexType));
        }

        // If the index exists delete it.
        if (searchFunctions.getIndexExistsFunction().test(elasticsearchIndexName))
        {
            searchFunctions.getDeleteIndexFunction().accept(elasticsearchIndexName);
        }

        // Create the index.
        searchFunctions.getCreateIndexFunction().accept(elasticsearchIndexName, documentType, mapping);
    }

    /**
     * Creates a new Elasticsearch index entity from the request information.
     *
     * @param request the information needed to create an Elasticsearch index
     * @param elasticsearchIndexTypeEntity the Elasticsearch index type entity
     *
     * @return the newly created Elasticsearch index entity
     */
    private ElasticsearchIndexEntity createElasticsearchIndexEntity(ElasticsearchIndexCreateRequest request,
        ElasticsearchIndexTypeEntity elasticsearchIndexTypeEntity)
    {
        ElasticsearchIndexEntity elasticsearchIndexEntity = new ElasticsearchIndexEntity();
        elasticsearchIndexEntity.setName(request.getElasticsearchIndexKey().getElasticsearchIndexName());
        elasticsearchIndexEntity.setType(elasticsearchIndexTypeEntity);
        return elasticsearchIndexEntity;
    }

    /**
     * Creates an Elasticsearch index object from the persisted entity.
     *
     * @param elasticsearchIndexEntity the Elasticsearch index entity
     *
     * @return the Elasticsearch index
     */
    private ElasticsearchIndex createElasticsearchIndexFromEntity(ElasticsearchIndexEntity elasticsearchIndexEntity)
    {
        ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();
        elasticsearchIndex.setElasticsearchIndexKey(new ElasticsearchIndexKey(elasticsearchIndexEntity.getName()));
        elasticsearchIndex.setElasticsearchIndexType(elasticsearchIndexEntity.getType().getCode());
        return elasticsearchIndex;
    }

    /**
     * Deletes an Elasticsearch index if it exists.
     *
     * @param elasticsearchIndexName the name of the Elasticsearch index
     */
    private void deleteElasticsearchIndex(String elasticsearchIndexName)
    {
        // If the index exists delete it.
        if (searchFunctions.getIndexExistsFunction().test(elasticsearchIndexName))
        {
            searchFunctions.getDeleteIndexFunction().accept(elasticsearchIndexName);
        }
    }

    /**
     * Validates the Elasticsearch index create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateElasticsearchIndexCreateRequest(ElasticsearchIndexCreateRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "An Elasticsearch create request must be specified.");
        validateElasticsearchIndexKey(request.getElasticsearchIndexKey());
        request.setElasticsearchIndexType(alternateKeyHelper.validateStringParameter("An", "Elasticsearch index type", request.getElasticsearchIndexType()));
    }

    /**
     * Validates an Elasticsearch index key. This method also trims the key parameters.
     *
     * @param key the Elasticsearch index key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateElasticsearchIndexKey(ElasticsearchIndexKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "An Elasticsearch index key must be specified.");
        key.setElasticsearchIndexName(alternateKeyHelper.validateStringParameter("An", "Elasticsearch index name", key.getElasticsearchIndexName()));
    }
}
