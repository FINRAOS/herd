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

import org.finra.herd.dao.ElasticsearchIndexTypeDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.ElasticsearchIndexTypeEntity;

/**
 * Helper for Elasticsearch index type operations which require DAO.
 */
@Component
public class ElasticsearchIndexTypeDaoHelper
{
    @Autowired
    private ElasticsearchIndexTypeDao elasticsearchIndexTypeDao;

    /**
     * Gets a Elasticsearch index type entity by it's code and ensure it exists.
     *
     * @param code the Elasticsearch index type code (case insensitive)
     *
     * @return the Elasticsearch index type entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the Elasticsearch index type entity doesn't exist
     */
    public ElasticsearchIndexTypeEntity getElasticsearchIndexTypeEntity(String code) throws ObjectNotFoundException
    {
        ElasticsearchIndexTypeEntity elasticsearchIndexTypeEntity = elasticsearchIndexTypeDao.getElasticsearchIndexTypeByCode(code);

        if (elasticsearchIndexTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Elasticsearch index type with code \"%s\" doesn't exist.", code));
        }

        return elasticsearchIndexTypeEntity;
    }
}
