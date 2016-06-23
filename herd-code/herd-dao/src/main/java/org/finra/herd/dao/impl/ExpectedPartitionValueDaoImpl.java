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

import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.ExpectedPartitionValueDao;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity_;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity_;

@Repository
public class ExpectedPartitionValueDaoImpl extends AbstractHerdDao implements ExpectedPartitionValueDao
{
    @Override
    public ExpectedPartitionValueEntity getExpectedPartitionValue(ExpectedPartitionValueKey expectedPartitionValueKey, int offset)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExpectedPartitionValueEntity> criteria = builder.createQuery(ExpectedPartitionValueEntity.class);

        // The criteria root is the expected partition value.
        Root<ExpectedPartitionValueEntity> expectedPartitionValueEntity = criteria.from(ExpectedPartitionValueEntity.class);

        // Join to the other tables we can filter on.
        Join<ExpectedPartitionValueEntity, PartitionKeyGroupEntity> partitionKeyGroupEntity =
            expectedPartitionValueEntity.join(ExpectedPartitionValueEntity_.partitionKeyGroup);

        // Add a restriction to filter case insensitive groups that match the user specified group.
        Predicate whereRestriction = builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)),
            expectedPartitionValueKey.getPartitionKeyGroupName().toUpperCase());

        // Depending on the offset, we might need to order the records in the query.
        Order orderByExpectedPartitionValue = null;

        // Add additional restrictions to handle expected partition value and an optional offset.
        if (offset == 0)
        {
            // Since there is no offset, we need to match the expected partition value exactly.
            whereRestriction = builder.and(whereRestriction, builder
                .equal(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue), expectedPartitionValueKey.getExpectedPartitionValue()));
        }
        else if (offset > 0)
        {
            // For a positive offset value, add a restriction to filter expected partition values that are >= the user specified expected partition value.
            whereRestriction = builder.and(whereRestriction, builder
                .greaterThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                    expectedPartitionValueKey.getExpectedPartitionValue()));

            // Order by expected partition value in ascending order.
            orderByExpectedPartitionValue = builder.asc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));
        }
        else
        {
            // For a negative offset value, add a restriction to filter expected partition values that are <= the user specified expected partition value.
            whereRestriction = builder.and(whereRestriction, builder
                .lessThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                    expectedPartitionValueKey.getExpectedPartitionValue()));

            // Order by expected partition value in descending order.
            orderByExpectedPartitionValue = builder.desc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));
        }

        // Add the clauses for the query and execute the query.
        if (offset == 0)
        {
            criteria.select(expectedPartitionValueEntity).where(whereRestriction);

            return executeSingleResultQuery(criteria, String
                .format("Found more than one expected partition value with parameters {partitionKeyGroupName=\"%s\", expectedPartitionValue=\"%s\"}.",
                    expectedPartitionValueKey.getPartitionKeyGroupName(), expectedPartitionValueKey.getExpectedPartitionValue()));
        }
        else
        {
            criteria.select(expectedPartitionValueEntity).where(whereRestriction).orderBy(orderByExpectedPartitionValue);

            List<ExpectedPartitionValueEntity> resultList =
                entityManager.createQuery(criteria).setFirstResult(Math.abs(offset)).setMaxResults(1).getResultList();

            return resultList.size() > 0 ? resultList.get(0) : null;
        }
    }

    @Override
    public List<ExpectedPartitionValueEntity> getExpectedPartitionValuesByGroupAndRange(String partitionKeyGroupName, PartitionValueRange partitionValueRange)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExpectedPartitionValueEntity> criteria = builder.createQuery(ExpectedPartitionValueEntity.class);

        // The criteria root is the expected partition value.
        Root<ExpectedPartitionValueEntity> expectedPartitionValueEntity = criteria.from(ExpectedPartitionValueEntity.class);

        // Join to the other tables we can filter on.
        Join<ExpectedPartitionValueEntity, PartitionKeyGroupEntity> partitionKeyGroupEntity =
            expectedPartitionValueEntity.join(ExpectedPartitionValueEntity_.partitionKeyGroup);

        // Add a restriction to filter case insensitive groups that match the user specified group.
        Predicate whereRestriction =
            builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)), partitionKeyGroupName.toUpperCase());

        // If we have a possible partition value range, we need to add additional restrictions.
        if (partitionValueRange != null)
        {
            // Add a restriction to filter values that are >= the user specified range start value.
            if (StringUtils.isNotBlank(partitionValueRange.getStartPartitionValue()))
            {
                whereRestriction = builder.and(whereRestriction, builder
                    .greaterThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                        partitionValueRange.getStartPartitionValue()));
            }

            // Add a restriction to filter values that are <= the user specified range end value.
            if (StringUtils.isNotBlank(partitionValueRange.getEndPartitionValue()))
            {
                whereRestriction = builder.and(whereRestriction, builder
                    .lessThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                        partitionValueRange.getEndPartitionValue()));
            }
        }

        // Order the results by partition value.
        Order orderByValue = builder.asc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));

        // Add the clauses for the query.
        criteria.select(expectedPartitionValueEntity).where(whereRestriction).orderBy(orderByValue);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
