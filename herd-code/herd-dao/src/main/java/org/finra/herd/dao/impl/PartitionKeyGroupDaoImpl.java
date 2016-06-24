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
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.PartitionKeyGroupDao;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity_;

@Repository
public class PartitionKeyGroupDaoImpl extends AbstractHerdDao implements PartitionKeyGroupDao
{
    @Override
    public PartitionKeyGroupEntity getPartitionKeyGroupByKey(PartitionKeyGroupKey partitionKeyGroupKey)
    {
        return getPartitionKeyGroupByName(partitionKeyGroupKey.getPartitionKeyGroupName());
    }

    @Override
    public PartitionKeyGroupEntity getPartitionKeyGroupByName(String partitionKeyGroupName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PartitionKeyGroupEntity> criteria = builder.createQuery(PartitionKeyGroupEntity.class);

        // The criteria root is the partition key group.
        Root<PartitionKeyGroupEntity> partitionKeyGroupEntity = criteria.from(PartitionKeyGroupEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate partitionKeyGroupRestriction = builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)),
            partitionKeyGroupName.toUpperCase());

        criteria.select(partitionKeyGroupEntity).where(partitionKeyGroupRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one \"%s\" partition key group.", partitionKeyGroupName));
    }

    @Override
    public List<PartitionKeyGroupKey> getPartitionKeyGroups()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PartitionKeyGroupEntity> criteria = builder.createQuery(PartitionKeyGroupEntity.class);

        // The criteria root is the partition key group.
        Root<PartitionKeyGroupEntity> partitionKeyGroupEntity = criteria.from(PartitionKeyGroupEntity.class);

        criteria.select(partitionKeyGroupEntity);

        List<PartitionKeyGroupKey> partitionKeyGroupKeys = new ArrayList<>();
        for (PartitionKeyGroupEntity entity : entityManager.createQuery(criteria).getResultList())
        {
            PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
            partitionKeyGroupKey.setPartitionKeyGroupName(entity.getPartitionKeyGroupName());
            partitionKeyGroupKeys.add(partitionKeyGroupKey);
        }

        return partitionKeyGroupKeys;
    }
}
