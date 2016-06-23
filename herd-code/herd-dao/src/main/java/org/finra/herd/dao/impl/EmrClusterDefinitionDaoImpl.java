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

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class EmrClusterDefinitionDaoImpl extends AbstractHerdDao implements EmrClusterDefinitionDao
{
    @Override
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(EmrClusterDefinitionKey emrClusterDefinitionKey)
    {
        return getEmrClusterDefinitionByAltKey(emrClusterDefinitionKey.getNamespace(), emrClusterDefinitionKey.getEmrClusterDefinitionName());
    }

    @Override
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(String namespaceCd, String definitionName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<EmrClusterDefinitionEntity> criteria = builder.createQuery(EmrClusterDefinitionEntity.class);

        // The criteria root is the EMR cluster definition.
        Root<EmrClusterDefinitionEntity> emrClusterDefinition = criteria.from(EmrClusterDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<EmrClusterDefinitionEntity, NamespaceEntity> namespace = emrClusterDefinition.join(EmrClusterDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate namespaceRestriction = builder.equal(builder.upper(namespace.get(NamespaceEntity_.code)), namespaceCd.toUpperCase());
        Predicate definitionNameRestriction =
            builder.equal(builder.upper(emrClusterDefinition.get(EmrClusterDefinitionEntity_.name)), definitionName.toUpperCase());

        criteria.select(emrClusterDefinition).where(builder.and(namespaceRestriction, definitionNameRestriction));

        return executeSingleResultQuery(criteria, String
            .format("Found more than one EMR cluster definition with parameters {namespace=\"%s\", clusterDefinitionName=\"%s\"}.", namespace, definitionName));
    }

    @Override
    public List<EmrClusterDefinitionKey> getEmrClusterDefinitionsByNamespace(String namespace)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the EMR cluster definition.
        Root<EmrClusterDefinitionEntity> emrClusterDefinitionEntityRoot = criteria.from(EmrClusterDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<EmrClusterDefinitionEntity, NamespaceEntity> namespaceEntityJoin = emrClusterDefinitionEntityRoot.join(EmrClusterDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntityJoin.get(NamespaceEntity_.code);
        Path<String> emrClusterDefinitionNameColumn = emrClusterDefinitionEntityRoot.get(EmrClusterDefinitionEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), namespace.toUpperCase());

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, emrClusterDefinitionNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(emrClusterDefinitionNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<EmrClusterDefinitionKey> emrClusterDefinitionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey();
            emrClusterDefinitionKeys.add(emrClusterDefinitionKey);
            emrClusterDefinitionKey.setNamespace(tuple.get(namespaceCodeColumn));
            emrClusterDefinitionKey.setEmrClusterDefinitionName(tuple.get(emrClusterDefinitionNameColumn));
        }

        return emrClusterDefinitionKeys;
    }
}
