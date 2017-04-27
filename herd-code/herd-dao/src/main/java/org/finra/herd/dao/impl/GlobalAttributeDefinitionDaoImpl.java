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

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity_;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity_;

@Repository
public class GlobalAttributeDefinitionDaoImpl extends AbstractHerdDao implements GlobalAttributeDefinitionDao
{
    @Override
    public List<GlobalAttributeDefinitionKey> getAllGlobalAttributeDefinitionKeys()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the global attribute definition.
        Root<GlobalAttributeDefinitionEntity> globalAttributeDefinitionEntityRoot = criteria.from(GlobalAttributeDefinitionEntity.class);

        //Join on the other tables that we filter on
        Join<GlobalAttributeDefinitionEntity, GlobalAttributeDefinitionLevelEntity> globalAttributeDefinitionLevelEntityJoin =
            globalAttributeDefinitionEntityRoot.join(GlobalAttributeDefinitionEntity_.globalAttributeDefinitionLevel);

        //Get the columns
        Path<String> globalAttributeDefinitionLevel =
            globalAttributeDefinitionLevelEntityJoin.get(GlobalAttributeDefinitionLevelEntity_.globalAttributeDefinitionLevel);
        Path<String> globalAttributeDefinitionName = globalAttributeDefinitionEntityRoot.get(GlobalAttributeDefinitionEntity_.globalAttributeDefinitionName);

        //Add all clauses to the query
        criteria.multiselect(globalAttributeDefinitionLevel, globalAttributeDefinitionName)
            .orderBy(builder.asc(globalAttributeDefinitionLevel), builder.asc(globalAttributeDefinitionName));

        // Populate the "keys" objects from the returned tuples
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        List<GlobalAttributeDefinitionKey> globalAttributeDefinitionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            globalAttributeDefinitionKeys
                .add(new GlobalAttributeDefinitionKey(tuple.get(globalAttributeDefinitionLevel), tuple.get(globalAttributeDefinitionName)));
        }

        return globalAttributeDefinitionKeys;

    }

    @Override
    public GlobalAttributeDefinitionEntity getGlobalAttributeDefinitionByKey(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<GlobalAttributeDefinitionEntity> criteria = builder.createQuery(GlobalAttributeDefinitionEntity.class);

        // The criteria root is the global attribute definition entity.
        Root<GlobalAttributeDefinitionEntity> globalAttributeDefinitionEntityRoot = criteria.from(GlobalAttributeDefinitionEntity.class);

        //Join on the other tables that we filter on
        Join<GlobalAttributeDefinitionEntity, GlobalAttributeDefinitionLevelEntity> globalAttributeDefinitionLevelEntityJoin =
            globalAttributeDefinitionEntityRoot.join(GlobalAttributeDefinitionEntity_.globalAttributeDefinitionLevel);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder
            .equal(builder.upper(globalAttributeDefinitionLevelEntityJoin.get(GlobalAttributeDefinitionLevelEntity_.globalAttributeDefinitionLevel)),
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel().toUpperCase()));
        predicates.add(builder.equal(builder.upper(globalAttributeDefinitionEntityRoot.get(GlobalAttributeDefinitionEntity_.globalAttributeDefinitionName)),
            globalAttributeDefinitionKey.getGlobalAttributeDefinitionName().toUpperCase()));

        // Add all clauses to the query.
        criteria.select(globalAttributeDefinitionEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one global attribute definition with parameters {globalAttributeDefinitionLevel=\"%s\", globalAttributeDefinitionLevel=\"%s\"}.",
            globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()));

    }
}
