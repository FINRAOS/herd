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

import org.finra.herd.dao.StoragePlatformDao;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity_;

@Repository
public class StoragePlatformDaoImpl extends AbstractHerdDao implements StoragePlatformDao
{
    @Override
    public StoragePlatformEntity getStoragePlatformByName(String name)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StoragePlatformEntity> criteria = builder.createQuery(StoragePlatformEntity.class);

        // The criteria root is the storage platform.
        Root<StoragePlatformEntity> storagePlatformEntity = criteria.from(StoragePlatformEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(storagePlatformEntity.get(StoragePlatformEntity_.name)), name.toUpperCase());

        // Add all clauses for the query.
        criteria.select(storagePlatformEntity).where(queryRestriction);

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String.format("Found more than one storage platform with \"%s\" name.", name));
    }
}
