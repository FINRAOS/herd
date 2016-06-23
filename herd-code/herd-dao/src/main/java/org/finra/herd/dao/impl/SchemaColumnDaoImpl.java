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

import org.finra.herd.dao.SchemaColumnDao;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity_;

@Repository
public class SchemaColumnDaoImpl extends AbstractHerdDao implements SchemaColumnDao
{
    @Override
    public List<SchemaColumnEntity> getSchemaColumns(BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String schemaColumnName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SchemaColumnEntity> criteria = builder.createQuery(SchemaColumnEntity.class);

        // The criteria root is the schema column.
        Root<SchemaColumnEntity> schemaColumnEntityRoot = criteria.from(SchemaColumnEntity.class);

        // Join to the other tables we can filter on.
        Join<SchemaColumnEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            schemaColumnEntityRoot.join(SchemaColumnEntity_.businessObjectFormat);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinition), businessObjectDefinitionEntity));
        predicates.add(builder.equal(builder.upper(schemaColumnEntityRoot.get(SchemaColumnEntity_.name)), schemaColumnName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(schemaColumnEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
