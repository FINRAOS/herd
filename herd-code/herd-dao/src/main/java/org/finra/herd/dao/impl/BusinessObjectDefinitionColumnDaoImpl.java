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
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDefinitionColumnDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class BusinessObjectDefinitionColumnDaoImpl extends AbstractHerdDao implements BusinessObjectDefinitionColumnDao
{
    @Override
    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String businessObjectDefinitionColumnName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionColumnEntity> criteria = builder.createQuery(BusinessObjectDefinitionColumnEntity.class);

        // The criteria root is the business object definition column.
        Root<BusinessObjectDefinitionColumnEntity> businessObjectDefinitionColumnEntityRoot = criteria.from(BusinessObjectDefinitionColumnEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(businessObjectDefinitionColumnEntityRoot.get(BusinessObjectDefinitionColumnEntity_.businessObjectDefinition),
            businessObjectDefinitionEntity));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionColumnEntityRoot.get(BusinessObjectDefinitionColumnEntity_.name)),
            businessObjectDefinitionColumnName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionColumnEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object definition column instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectDefinitionColumnName=\"%s\"}.", businessObjectDefinitionEntity.getNamespace().getCode(),
            businessObjectDefinitionEntity.getName(), businessObjectDefinitionColumnName));
    }

    @Override
    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnByKey(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionColumnEntity> criteria = builder.createQuery(BusinessObjectDefinitionColumnEntity.class);

        // The criteria root is the business object definition column.
        Root<BusinessObjectDefinitionColumnEntity> businessObjectDefinitionColumnEntityRoot = criteria.from(BusinessObjectDefinitionColumnEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionColumnEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectDefinitionColumnEntityRoot.join(BusinessObjectDefinitionColumnEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates
            .add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), businessObjectDefinitionColumnKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionColumnEntityRoot.get(BusinessObjectDefinitionColumnEntity_.name)),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionColumnName().toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionColumnEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object definition column instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectDefinitionColumnName=\"%s\"}.", businessObjectDefinitionColumnKey.getNamespace(),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName(), businessObjectDefinitionColumnKey.getBusinessObjectDefinitionColumnName()));
    }

    @Override
    public List<BusinessObjectDefinitionColumnEntity> getBusinessObjectDefinitionColumnsByBusinessObjectDefinition(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionColumnEntity> criteria = builder.createQuery(BusinessObjectDefinitionColumnEntity.class);

        // The criteria root is the business object definition column.
        Root<BusinessObjectDefinitionColumnEntity> businessObjectDefinitionColumnEntityRoot = criteria.from(BusinessObjectDefinitionColumnEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionColumnEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectDefinitionColumnEntityRoot.join(BusinessObjectDefinitionColumnEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(businessObjectDefinitionEntityJoin, businessObjectDefinitionEntity));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionColumnEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Order by business object definition column name.
        criteria.orderBy(builder.asc(businessObjectDefinitionColumnEntityRoot.get(BusinessObjectDefinitionColumnEntity_.name)));

        return entityManager.createQuery(criteria).getResultList();
    }
}
