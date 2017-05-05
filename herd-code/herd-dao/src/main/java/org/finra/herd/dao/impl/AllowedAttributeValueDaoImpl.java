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
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.AllowedAttributeValueDao;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity_;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class AllowedAttributeValueDaoImpl extends AbstractHerdDao implements AllowedAttributeValueDao
{
    @Override
    public List<AllowedAttributeValueEntity> getAllowedAttributeValuesByAttributeValueListKey(AttributeValueListKey attributeValueListKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AllowedAttributeValueEntity> criteria = builder.createQuery(AllowedAttributeValueEntity.class);

        // The criteria root is the allowed attribute value.
        Root<AllowedAttributeValueEntity> allowedAttributeValueEntityRoot = criteria.from(AllowedAttributeValueEntity.class);

        // Join to the other tables we can filter on.
        Join<AllowedAttributeValueEntity, AttributeValueListEntity> attributeValueListEntityJoin =
            allowedAttributeValueEntityRoot.join(AllowedAttributeValueEntity_.attributeValueList);
        Join<AttributeValueListEntity, NamespaceEntity> namespaceEntityJoin = attributeValueListEntityJoin.join(AttributeValueListEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(attributeValueListEntityJoin.get(AttributeValueListEntity_.name)),
            attributeValueListKey.getAttributeValueListName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), attributeValueListKey.getNamespace().toUpperCase()));

        // Order the results by allowed attribute value.
        Order orderByAllowedAttributeValue = builder.asc(allowedAttributeValueEntityRoot.get(AllowedAttributeValueEntity_.allowedAttributeValue));

        // Add the clauses for the query.
        criteria.select(allowedAttributeValueEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])))
            .orderBy(orderByAllowedAttributeValue);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
