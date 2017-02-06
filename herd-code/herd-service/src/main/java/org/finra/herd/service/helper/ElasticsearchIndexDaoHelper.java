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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.ElasticsearchIndexDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ElasticsearchIndexKey;
import org.finra.herd.model.jpa.ElasticsearchIndexEntity;

/**
 * Helper for Elasticsearch index related operations which require DAO.
 */
@Component
public class ElasticsearchIndexDaoHelper
{
    @Autowired
    private ElasticsearchIndexDao elasticsearchIndexDao;

    /**
     * Gets an Elasticsearch index entity by it's key and ensure it exists.
     *
     * @param elasticsearchIndexKey the Elasticsearch index key (case insensitive)
     *
     * @return the Elasticsearch index entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the Elasticsearch index entity doesn't exist
     */
    public ElasticsearchIndexEntity getElasticsearchIndexEntity(ElasticsearchIndexKey elasticsearchIndexKey) throws ObjectNotFoundException
    {
        ElasticsearchIndexEntity elasticsearchIndexEntity = elasticsearchIndexDao.getElasticsearchIndexByKey(elasticsearchIndexKey);

        if (elasticsearchIndexEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Elasticsearch index with name \"%s\" doesn't exist.", elasticsearchIndexKey.getElasticsearchIndexName()));
        }

        return elasticsearchIndexEntity;
    }
}
