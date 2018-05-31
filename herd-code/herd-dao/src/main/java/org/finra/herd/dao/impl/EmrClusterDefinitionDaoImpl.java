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

import com.google.common.collect.Lists;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;

@Repository
public class EmrClusterDefinitionDaoImpl extends AbstractHerdDao implements EmrClusterDefinitionDao
{
    @Override
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByNamespaceAndName(NamespaceEntity namespaceEntity, String emrClusterDefinitionName)
    {
        // Create criteria builder and a top-level query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<EmrClusterDefinitionEntity> criteria = builder.createQuery(EmrClusterDefinitionEntity.class);

        // The criteria root is the EMR cluster definition.
        Root<EmrClusterDefinitionEntity> emrClusterDefinitionEntityRoot = criteria.from(EmrClusterDefinitionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(emrClusterDefinitionEntityRoot.get(EmrClusterDefinitionEntity_.namespace), namespaceEntity));
        predicates
            .add(builder.equal(builder.upper(emrClusterDefinitionEntityRoot.get(EmrClusterDefinitionEntity_.name)), emrClusterDefinitionName.toUpperCase()));

        // Add all clauses for the query.
        criteria.select(emrClusterDefinitionEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute query and return result.
        return executeSingleResultQuery(criteria, String
            .format("Found more than one EMR cluster definition with parameters {namespace=\"%s\", emrClusterDefinitionName=\"%s\"}.",
                namespaceEntity.getCode(), emrClusterDefinitionName));
    }

    @Override
    public List<EmrClusterDefinitionKey> getEmrClusterDefinitionKeysByNamespace(NamespaceEntity namespaceEntity)
    {
        // Create criteria builder and a top-level query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the EMR cluster definition.
        Root<EmrClusterDefinitionEntity> emrClusterDefinitionEntityRoot = criteria.from(EmrClusterDefinitionEntity.class);

        // Get the EMR cluster definition name column.
        Path<String> emrClusterDefinitionNameColumn = emrClusterDefinitionEntityRoot.get(EmrClusterDefinitionEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(emrClusterDefinitionEntityRoot.get(EmrClusterDefinitionEntity_.namespace), namespaceEntity);

        // Add all clauses for the query.
        criteria.select(emrClusterDefinitionNameColumn).where(predicate).orderBy(builder.asc(emrClusterDefinitionNameColumn));

        // Execute the query to get a list of EMR cluster definition names back.
        List<String> emrClusterDefinitionNames = entityManager.createQuery(criteria).getResultList();

        // Build a list of EMR cluster definition keys.
        List<EmrClusterDefinitionKey> emrClusterDefinitionKeys = Lists.newArrayList();
        for (String emrClusterDefinitionName : emrClusterDefinitionNames)
        {
            emrClusterDefinitionKeys.add(new EmrClusterDefinitionKey(namespaceEntity.getCode(), emrClusterDefinitionName));
        }

        // Return the list of keys.
        return emrClusterDefinitionKeys;
    }
}
