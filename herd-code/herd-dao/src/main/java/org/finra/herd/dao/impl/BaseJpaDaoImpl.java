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

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;

import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BaseJpaDao;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * A generic JPA DAO that can be used directly or as a base class for more specialized DAO's.
 */
@Repository
public class BaseJpaDaoImpl implements BaseJpaDao
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @PersistenceContext
    protected EntityManager entityManager;

    @Override
    public EntityManager getEntityManager()
    {
        return entityManager;
    }

    @Override
    public <T> T findById(Class<T> entityClass, Object entityId)
    {
        Validate.notNull(entityClass);
        Validate.notNull(entityId);
        return entityManager.find(entityClass, entityId);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass)
    {
        Validate.notNull(entityClass);
        return query("select type from " + StringUtils.unqualify(entityClass.getName()) + " type");
    }

    @Override
    public <T> List<T> findByNamedProperties(Class<T> entityClass, Map<String, ?> params)
    {
        Validate.notNull(entityClass);
        Validate.notEmpty(params);
        StringBuilder queryStringBuilder = new StringBuilder("select type from " + StringUtils.unqualify(entityClass.getName()) + " type where ");

        Iterator<String> iterator = params.keySet().iterator();
        int index = 0;
        while (iterator.hasNext())
        {
            String propertyName = iterator.next();
            if (index > 0)
            {
                queryStringBuilder.append(" and ");
            }
            queryStringBuilder.append("(type.").append(propertyName).append(" = :").append(propertyName).append(')');
            index++;
        }

        return queryByNamedParams(queryStringBuilder.toString(), params);
    }

    @Override
    public <T> T findUniqueByNamedProperties(Class<T> entityClass, Map<String, ?> params)
    {
        Validate.notNull(entityClass);
        Validate.notEmpty(params);
        List<T> resultList = findByNamedProperties(entityClass, params);
        Validate.isTrue(resultList.isEmpty() || resultList.size() == 1,
            "Found more than one persistent instance of type " + StringUtils.unqualify(entityClass.getName() + " with parameters " + params.toString()));
        return resultList.size() == 1 ? resultList.get(0) : null;
    }

    @Override
    public <T> List<T> queryByNamedParams(String queryString, Map<String, ?> params)
    {
        Validate.notEmpty(queryString);
        Validate.notEmpty(params);
        return executeQueryWithNamedParams(entityManager.createQuery(queryString), params);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> List<T> query(String queryString)
    {
        Validate.notEmpty(queryString);
        return entityManager.createQuery(queryString).getResultList();
    }

    @Override
    public <T> T save(T entity)
    {
        Validate.notNull(entity);
        entityManager.persist(entity);
        return entity;
    }

    @Override
    public <T> T saveAndRefresh(T entity)
    {
        // Save the entity.
        save(entity);

        // Flush (i.e. persist) the entity and re-load it to retrieve the create/update date that was populated by the database.
        entityManager.flush();
        entityManager.refresh(entity);

        // Return the persisted entity.
        return entity;
    }

    @Override
    public <T> void delete(T entity)
    {
        Validate.notNull(entity);
        entityManager.remove(entity);

        // Flush to persist so we ensure that data integrity violations, etc. are thrown here rather than at the time of transaction commit.
        entityManager.flush();
    }

    @Override
    public <T> void detach(T entity)
    {
        Validate.notNull(entity);
        entityManager.detach(entity);
    }

    /**
     * Executes a query with named parameters and returns the result list.
     *
     * @param query the query to execute.
     * @param params the named parameters.
     *
     * @return the list of results.
     */
    @SuppressWarnings({"unchecked"})
    private <T> List<T> executeQueryWithNamedParams(Query query, Map<String, ?> params)
    {
        Validate.notNull(query);
        Validate.notNull(params);
        for (Map.Entry<String, ?> entry : params.entrySet())
        {
            query.setParameter(entry.getKey(), entry.getValue());
        }
        return query.getResultList();
    }

    /**
     * Gets an "in" clause predicate for a list of values. This will take care of breaking the list of values into a group of sub-lists where each sub-list is
     * placed in a separate "in" clause and all "in" clauses are "or"ed together. The size of each sub-list is obtained through an environment configuration.
     *
     * @param builder the criteria builder.
     * @param path the path to the field that is being filtered.
     * @param values the list of values to place in the in clause.
     * @param <T> the type referenced by the path.
     *
     * @return the predicate for the in clause.
     */
    protected <T> Predicate getPredicateForInClause(CriteriaBuilder builder, Path<T> path, List<T> values)
    {
        // Get the chunk size from the environment and use a default as necessary.
        int inClauseChunkSize = configurationHelper.getProperty(ConfigurationValue.DB_IN_CLAUSE_CHUNK_SIZE, Integer.class);

        // Initializes the returned predicate and the value list size.
        Predicate predicate = null;
        int listSize = values.size();

        // Loop through each chunk of values until we have reached the end of the values.
        for (int i = 0; i < listSize; i += inClauseChunkSize)
        {
            // Get a sub-list for the current chunk of data.
            List<T> valuesSubList = values.subList(i, (listSize > (i + inClauseChunkSize) ? (i + inClauseChunkSize) : listSize));

            // Get an updated predicate which will be the "in" clause of the sub-list on the first loop or the "in" clause of the sub-list "or"ed with the\
            // previous sub-list "in" clause.
            predicate = (predicate == null ? path.in(valuesSubList) : builder.or(predicate, path.in(valuesSubList)));
        }

        // Return the "in" clause predicate.
        return predicate;
    }

    /**
     * Executes query, validates if result list contains no more than record and returns the query result.
     *
     * @param <T> The type of the root entity class
     * @param criteria the criteria select query to be executed
     * @param message the exception message to use if the query returns fails
     *
     * @return the query result or null if 0 records were selected
     */
    protected <T> T executeSingleResultQuery(CriteriaQuery<T> criteria, String message)
    {
        List<T> resultList = entityManager.createQuery(criteria).getResultList();

        // Validate that the query returned no more than one record.
        Validate.isTrue(resultList.size() < 2, message);

        return resultList.size() == 1 ? resultList.get(0) : null;
    }

    @Override
    public Timestamp getCurrentTimestamp()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Timestamp> criteria = builder.createQuery(Timestamp.class);

        // Add the clauses for the query.
        criteria.select(builder.currentTimestamp()).from(ConfigurationEntity.class);

        return entityManager.createQuery(criteria).getSingleResult();
    }
}
