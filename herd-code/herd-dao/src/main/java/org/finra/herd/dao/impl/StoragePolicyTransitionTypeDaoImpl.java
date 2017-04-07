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

import org.finra.herd.dao.StoragePolicyTransitionTypeDao;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity_;

@Repository
public class StoragePolicyTransitionTypeDaoImpl extends AbstractHerdDao implements StoragePolicyTransitionTypeDao
{
    @Override
    public StoragePolicyTransitionTypeEntity getStoragePolicyTransitionTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StoragePolicyTransitionTypeEntity> criteria = builder.createQuery(StoragePolicyTransitionTypeEntity.class);

        // The criteria root is the storage policy transition type.
        Root<StoragePolicyTransitionTypeEntity> storagePolicyTransitionTypeEntity = criteria.from(StoragePolicyTransitionTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(storagePolicyTransitionTypeEntity.get(StoragePolicyTransitionTypeEntity_.code)), code.toUpperCase());

        criteria.select(storagePolicyTransitionTypeEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one storage policy transition type with code \"%s\".", code));
    }
}
