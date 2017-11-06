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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.RetentionTypeDao;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity_;

@Repository
public class RetentionTypeDaoImpl extends AbstractHerdDao implements RetentionTypeDao
{
    @Override
    public RetentionTypeEntity getRetentionTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<RetentionTypeEntity> criteria = builder.createQuery(RetentionTypeEntity.class);

        // The criteria root is the retention type
        Root<RetentionTypeEntity> retentionType = criteria.from(RetentionTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate businessObjectDataStatusCodeRestriction = builder.equal(builder.upper(retentionType.get(RetentionTypeEntity_.code)),
            code.toUpperCase());

        criteria.select(retentionType).where(businessObjectDataStatusCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one retention type with code \"%s\".", code));
    }
}