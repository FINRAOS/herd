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

import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexEntity_;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;

@Repository
public class SearchIndexDaoImpl extends AbstractHerdDao implements SearchIndexDao
{
    @Override
    public SearchIndexEntity getSearchIndexByKey(SearchIndexKey searchIndexKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SearchIndexEntity> criteria = builder.createQuery(SearchIndexEntity.class);

        // The criteria root is the search index.
        Root<SearchIndexEntity> elasticsearchIndexEntityRoot = criteria.from(SearchIndexEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(elasticsearchIndexEntityRoot.get(SearchIndexEntity_.name), searchIndexKey.getSearchIndexName());

        // Add all clauses to the query.
        criteria.select(elasticsearchIndexEntityRoot).where(predicate);

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String.format("Found more than one search index with name \"%s\".", searchIndexKey.getSearchIndexName()));
    }

    @Override
    public List<SearchIndexKey> getSearchIndexes()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the search index.
        Root<SearchIndexEntity> searchIndexEntityRoot = criteria.from(SearchIndexEntity.class);

        // Get the columns.
        Path<String> searchIndexNameColumn = searchIndexEntityRoot.get(SearchIndexEntity_.name);

        // Add all clauses to the query.
        criteria.select(searchIndexNameColumn).orderBy(builder.asc(searchIndexNameColumn));

        // Run the query to get a list of search index names back.
        List<String> searchIndexNames = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned search index names.
        List<SearchIndexKey> searchIndexKeys = new ArrayList<>();
        for (String searchIndexName : searchIndexNames)
        {
            searchIndexKeys.add(new SearchIndexKey(searchIndexName));
        }

        // Returned the list of keys.
        return searchIndexKeys;
    }


    @Override
    public List<SearchIndexEntity> getSearchIndexEntities(SearchIndexTypeEntity searchIndexTypeEntity)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SearchIndexEntity> criteria = builder.createQuery(SearchIndexEntity.class);

        // The criteria root is the search index.
        Root<SearchIndexEntity> searchIndexEntityRoot = criteria.from(SearchIndexEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(searchIndexEntityRoot.get(SearchIndexEntity_.type), searchIndexTypeEntity);

        // Add all clauses to the query.
        criteria.select(searchIndexEntityRoot).where(predicate);

        // Return the list of entities.
        return entityManager.createQuery(criteria).getResultList();
    }
}
