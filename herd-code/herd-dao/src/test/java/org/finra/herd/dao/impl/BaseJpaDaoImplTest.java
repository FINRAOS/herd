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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.hibernate.query.criteria.internal.predicate.InPredicate;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity_;

/**
 * Tests methods on the BaseJpaDaoImpl class that aren't available on the interface.
 */
public class BaseJpaDaoImplTest extends AbstractDaoTest
{
    // Provide easy access to the herd DAO for all test methods.
    @Autowired
    protected BaseJpaDaoImpl baseJpaDaoImpl;

    @Test
    public void testGetPredicateForInClauseOneChunk()
    {
        // Create the JPA builder, query, and entity root.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Get the predicate for the "in" clause with 1000 values.
        Predicate predicate = baseJpaDaoImpl
            .getPredicateForInClause(builder, businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), getPartitionValueList(1000));

        // We expect to get back an "in" predicate with a single chunk of 1000 partition values.
        assertTrue(predicate instanceof InPredicate);
        assertEquals(1000, ((InPredicate) predicate).getValues().size());
    }

    @Test
    public void testGetPredicateForInClauseTwoChunks()
    {
        // Create the JPA builder, query, and entity root.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Get the predicate for the "in" clause with 1001 values (1 greater than the default chunking size).
        Predicate predicate = baseJpaDaoImpl
            .getPredicateForInClause(builder, businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), getPartitionValueList(1001));

        // We expect to get back an "or" of 2 "in" expressions where each "in" expression is a list of chunked partition values.
        assertEquals(Predicate.BooleanOperator.OR, predicate.getOperator());

        List<Expression<Boolean>> expressions = predicate.getExpressions();
        assertEquals(2, expressions.size());

        // The first "in" clause will have the first 1000 elements and the second "in" clause will have the extra "1" element.
        assertEquals(1000, ((InPredicate) expressions.get(0)).getValues().size());
        assertEquals(1, ((InPredicate) expressions.get(1)).getValues().size());
    }

    /**
     * Gets a list of partition values of the specified size.
     *
     * @param size the size.
     *
     * @return the list of partition values.
     */
    private List<String> getPartitionValueList(int size)
    {
        // Build
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            partitionValues.add("VALUE " + i);
        }
        return partitionValues;
    }
}
