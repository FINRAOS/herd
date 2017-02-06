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
package org.finra.herd.dao.impl;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.ElasticsearchIndexDao;
import org.finra.herd.model.api.xml.ElasticsearchIndexKey;
import org.finra.herd.model.jpa.ElasticsearchIndexEntity;
import org.finra.herd.model.jpa.ElasticsearchIndexEntity_;

@Repository
public class ElasticsearchIndexDaoImpl extends AbstractHerdDao implements ElasticsearchIndexDao
{
    @Override
    public ElasticsearchIndexEntity getElasticsearchIndexByKey(ElasticsearchIndexKey elasticsearchIndexKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ElasticsearchIndexEntity> criteria = builder.createQuery(ElasticsearchIndexEntity.class);

        // The criteria root is the Elasticsearch index.
        Root<ElasticsearchIndexEntity> elasticsearchIndexEntityRoot = criteria.from(ElasticsearchIndexEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(builder.upper(elasticsearchIndexEntityRoot.get(ElasticsearchIndexEntity_.name)),
            elasticsearchIndexKey.getElasticsearchIndexName().toUpperCase());

        // Add all clauses to the query.
        criteria.select(elasticsearchIndexEntityRoot).where(predicate);

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria,
            String.format("Found more than one Elasticsearch index with name \"%s\".", elasticsearchIndexKey.getElasticsearchIndexName()));
    }

    /**
     * Gets a list of Elasticsearch index keys for all Elasticsearch indexes defined in the system.
     *
     * @return the list of Elasticsearch index keys
     */
    public List<ElasticsearchIndexKey> getElasticsearchIndexes()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the Elasticsearch index.
        Root<ElasticsearchIndexEntity> elasticsearchIndexEntityRoot = criteria.from(ElasticsearchIndexEntity.class);

        // Get the columns.
        Path<String> elasticsearchIndexNameColumn = elasticsearchIndexEntityRoot.get(ElasticsearchIndexEntity_.name);

        // Add all clauses to the query.
        criteria.select(elasticsearchIndexNameColumn).orderBy(builder.asc(elasticsearchIndexNameColumn));

        // Run the query to get a list of Elasticsearch index names back.
        List<String> elasticsearchIndexNames = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned Elasticsearch index names.
        List<ElasticsearchIndexKey> elasticsearchIndexKeys = new ArrayList<>();
        for (String elasticsearchIndexName : elasticsearchIndexNames)
        {
            elasticsearchIndexKeys.add(new ElasticsearchIndexKey(elasticsearchIndexName));
        }

        // Returned the list of keys.
        return elasticsearchIndexKeys;
    }
}
