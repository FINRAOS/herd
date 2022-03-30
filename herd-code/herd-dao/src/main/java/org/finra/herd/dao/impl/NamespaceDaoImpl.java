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

import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class NamespaceDaoImpl extends AbstractHerdDao implements NamespaceDao
{
    @Override
    public NamespaceEntity getNamespaceByKey(NamespaceKey namespaceKey)
    {
        return getNamespaceByCd(namespaceKey.getNamespaceCode());
    }

    @Override
    public NamespaceEntity getNamespaceByCd(String namespaceCode)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceEntity> criteria = builder.createQuery(NamespaceEntity.class);

        // The criteria root is the namespace.
        Root<NamespaceEntity> namespaceEntity = criteria.from(NamespaceEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespaceCode.toUpperCase());

        criteria.select(namespaceEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one namespace with namespaceCode=\"%s\".", namespaceCode));
    }

    @Override
    public List<NamespaceKey> getNamespaceKeys()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object definition.
        Root<NamespaceEntity> namespaceEntity = criteria.from(NamespaceEntity.class);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);

        // Add the select clause.
        criteria.select(namespaceCodeColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(namespaceCodeColumn));

        // Run the query to get a list of namespace codes back.
        List<String> namespaceCodes = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned namespace codes.
        List<NamespaceKey> namespaceKeys = new ArrayList<>();
        for (String namespaceCode : namespaceCodes)
        {
            namespaceKeys.add(new NamespaceKey(namespaceCode));
        }

        return namespaceKeys;
    }

    @Override
    public List<NamespaceEntity> getNamespaces()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceEntity> criteria = builder.createQuery(NamespaceEntity.class);

        // The criteria root is the business object definition.
        Root<NamespaceEntity> namespaceEntityRoot = criteria.from(NamespaceEntity.class);

        // Add all clauses to the query.
        criteria.select(namespaceEntityRoot).orderBy(builder.asc(namespaceEntityRoot.get(NamespaceEntity_.code)));

        // Run the query and return the result.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<NamespaceEntity> getNamespacesByChargeCode(String chargeCode)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceEntity> criteria = builder.createQuery(NamespaceEntity.class);

        // The criteria root is the namespace entity.
        Root<NamespaceEntity> namespaceEntityRoot = criteria.from(NamespaceEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Path<String> namespaceChargeCodeColumn = namespaceEntityRoot.get(NamespaceEntity_.chargeCode);
        Predicate queryRestriction = namespaceChargeCodeColumn.in(chargeCode);

        // Add the select clause.
        criteria.select(namespaceEntityRoot).where(queryRestriction).
            orderBy(builder.asc(namespaceEntityRoot.get(NamespaceEntity_.code)));

        return entityManager.createQuery(criteria).getResultList();
    }
}
