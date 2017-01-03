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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.TagEntity;

@Repository
public class BusinessObjectDefinitionDaoImpl extends AbstractHerdDao implements BusinessObjectDefinitionDao
{
    @Override
    public List<BusinessObjectDefinitionEntity> getAllBusinessObjectDefinitions()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity =
            businessObjectDefinitionEntityRoot.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.name);

        criteria.select(businessObjectDefinitionEntityRoot).orderBy(builder.asc(businessObjectDefinitionNameColumn), builder.asc(namespaceCodeColumn));

        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectDefinitionKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName().toUpperCase()));

        criteria.select(businessObjectDefinitionEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one business object definition with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"}.",
                businessObjectDefinitionKey.getNamespace(), businessObjectDefinitionKey.getBusinessObjectDefinitionName()));
    }

    @Override
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitionKeys()
    {
        return getBusinessObjectDefinitionKeysByNamespace(null);
    }

    @Override
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitionKeysByNamespace(String namespaceCode)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn);

        // If namespace code is specified, add the where clause.
        if (StringUtils.isNotBlank(namespaceCode))
        {
            criteria.where(builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespaceCode.toUpperCase()));
        }

        // Add the order by clause.
        if (StringUtils.isNotBlank(namespaceCode))
        {
            criteria.orderBy(builder.asc(businessObjectDefinitionNameColumn));
        }
        else
        {
            criteria.orderBy(builder.asc(businessObjectDefinitionNameColumn), builder.asc(namespaceCodeColumn));
        }

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey();
            businessObjectDefinitionKeys.add(businessObjectDefinitionKey);
            businessObjectDefinitionKey.setNamespace(tuple.get(namespaceCodeColumn));
            businessObjectDefinitionKey.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNameColumn));
        }

        return businessObjectDefinitionKeys;
    }

    @Override
    public List<BusinessObjectDefinitionEntity> getBusinessObjectDefinitions(List<TagEntity> tagEntities)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity =
            businessObjectDefinitionEntityRoot.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.name);

        Predicate predicate;

        if (!CollectionUtils.isEmpty(tagEntities))
        {
            //join the business object definition tags
            Join<BusinessObjectDefinitionEntity, BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntityJoin =
                businessObjectDefinitionEntityRoot.join(BusinessObjectDefinitionEntity_.businessObjectDefinitionTags);

            // Create the standard restrictions (i.e. the standard where clauses).
            predicate = getPredicateForInClause(builder, businessObjectDefinitionTagEntityJoin.get(BusinessObjectDefinitionTagEntity_.tag), tagEntities);

            // Add all clauses to the query.
            criteria.select(businessObjectDefinitionEntityRoot).where(predicate)
                .orderBy(builder.asc(businessObjectDefinitionNameColumn), builder.asc(namespaceCodeColumn));
        }
        else
        {
            criteria.select(businessObjectDefinitionEntityRoot).orderBy(builder.asc(businessObjectDefinitionNameColumn), builder.asc(namespaceCodeColumn));
        }

        //Returns duplicate business object definition. When a bdef is associated with multiple tags.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<BusinessObjectDefinitionEntity> getPercentageOfAllBusinessObjectDefinitions(double percentage)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Integer> criteria = builder.createQuery(Integer.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Get the columns.
        Path<Integer> idColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.id);

        criteria.select(idColumn);

        List<Integer> allBusinessObjectDefinitionIdsList = entityManager.createQuery(criteria).getResultList();
        List<Integer> percentageOfBusinessObjectDefinitionIdsList = new ArrayList<>();

        /*
        * Gets a percentage of all business object definition entities.
        * The percentage is randomly selected from all the business object definitions.
        *
        * For each business object id in the list of all business object definition ids, get a random double value between 0 and 1.
        * If that value is below the percentage double value, also a number between 0 and 1 (inclusive),
        * then add the business object id to the list of business object definition ids that will be used to return a random percentage
        * of business object definition entities retrieved from the database.
        */
        allBusinessObjectDefinitionIdsList.forEach(id -> {
            if (ThreadLocalRandom.current().nextDouble() < percentage)
            {
                percentageOfBusinessObjectDefinitionIdsList.add(id);
            }
        });

        return getAllBusinessObjectDefinitionsByIds(percentageOfBusinessObjectDefinitionIdsList);
    }

    private List<BusinessObjectDefinitionEntity> getAllBusinessObjectDefinitionsByIds(List<Integer> ids)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Expression<Integer> expression = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.id);
        Predicate queryRestriction = expression.in(ids);

        criteria.select(businessObjectDefinitionEntityRoot).where(queryRestriction);

        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<BusinessObjectDefinitionEntity> getMostRecentBusinessObjectDefinitions(int numberOfResults)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Get the columns.
        Path<Timestamp> businessObjectDefinitionUpdatedOnColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.updatedOn);

        // Select the business object definitions and order descending by the updated on column
        criteria.select(businessObjectDefinitionEntityRoot).orderBy(builder.desc(businessObjectDefinitionUpdatedOnColumn));

        return entityManager.createQuery(criteria).setMaxResults(numberOfResults).getResultList();
    }

    @Override
    public long getCountOfAllBusinessObjectDefinitions()
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);
        criteria.select(builder.count(businessObjectDefinitionEntityRoot));
        return entityManager.createQuery(criteria).getSingleResult();
    }
}
