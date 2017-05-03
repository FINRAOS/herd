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
import java.util.Collection;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class AttributeValueListDaoImpl extends AbstractHerdDao implements AttributeValueListDao
{
    @Override
    public AttributeValueListEntity getAttributeValueListByKey(AttributeValueListKey attributeValueListKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AttributeValueListEntity> criteria = builder.createQuery(AttributeValueListEntity.class);

        // The criteria root is the attribute value list.
        Root<AttributeValueListEntity> attributeValueListEntityRoot = criteria.from(AttributeValueListEntity.class);

        // Join to the other tables we can filter on.
        Join<AttributeValueListEntity, NamespaceEntity> namespaceEntityJoin = attributeValueListEntityRoot.join(AttributeValueListEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), attributeValueListKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(attributeValueListEntityRoot.get(AttributeValueListEntity_.attributeValueListName)),
            attributeValueListKey.getAttributeValueListName().toUpperCase()));

        // Add all clauses to the query.
        criteria.select(attributeValueListEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the results.
        return executeSingleResultQuery(criteria, String
            .format("Found more than one attribute value list with parameters {namespace=\"%s\", attribute_value_name=\"%s\"}.",
                attributeValueListKey.getNamespace(), attributeValueListKey.getAttributeValueListName()));
    }

    @Override
    public List<AttributeValueListKey> getAttributeValueLists(Collection<String> namespaces)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AttributeValueListEntity> criteria = builder.createQuery(AttributeValueListEntity.class);

        // The criteria root is the attribute value list entity.
        Root<AttributeValueListEntity> attributeValueListEntityRoot = criteria.from(AttributeValueListEntity.class);

        // Join to the other tables we can filter on.
        Join<AttributeValueListEntity, NamespaceEntity> namespaceEntityJoin = attributeValueListEntityRoot.join(AttributeValueListEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(namespaces))
        {
            predicates.add(namespaceEntityJoin.get(NamespaceEntity_.code).in(namespaces));
        }

        // Order the results by namespace and job name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntityJoin.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(attributeValueListEntityRoot.get(AttributeValueListEntity_.attributeValueListName)));

        // Add all clauses to the query.
        criteria.select(attributeValueListEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and build a list of keys.
        List<AttributeValueListKey> attributeValueListKeys = new ArrayList<>();
        for (AttributeValueListEntity attributeValueListEntity : entityManager.createQuery(criteria).getResultList())
        {
            attributeValueListKeys
                .add(new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));
        }

        return attributeValueListKeys;
    }
}
