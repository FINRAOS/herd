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
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.StoragePolicyDao;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity_;

@Repository
public class StoragePolicyDaoImpl extends AbstractHerdDao implements StoragePolicyDao
{
    @Override
    public StoragePolicyEntity getStoragePolicyByAltKey(StoragePolicyKey key)
    {
        return getStoragePolicyByAltKeyAndVersion(key, null);
    }

    @Override
    public StoragePolicyEntity getStoragePolicyByAltKeyAndVersion(StoragePolicyKey key, Integer storagePolicyVersion)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StoragePolicyEntity> criteria = builder.createQuery(StoragePolicyEntity.class);

        // The criteria root is the storage policy entity.
        Root<StoragePolicyEntity> storagePolicyEntity = criteria.from(StoragePolicyEntity.class);

        // Join to the other tables we can filter on.
        Join<StoragePolicyEntity, NamespaceEntity> namespaceEntity = storagePolicyEntity.join(StoragePolicyEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), key.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(storagePolicyEntity.get(StoragePolicyEntity_.name)), key
            .getStoragePolicyName().toUpperCase()));

        // Add a restriction on storage policy version.
        if (storagePolicyVersion != null)
        {
            // Storage policy version is specified, use it.
            queryRestriction = builder.and(queryRestriction, builder.equal(storagePolicyEntity.get(StoragePolicyEntity_.version), storagePolicyVersion));
        }
        else
        {
            // Storage policy version is not specified, use the latest one.
            queryRestriction = builder.and(queryRestriction, builder.isTrue(storagePolicyEntity.get(StoragePolicyEntity_.latestVersion)));
        }

        criteria.select(storagePolicyEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one storage policy with with parameters {namespace=\"%s\", storagePolicyName=\"%s\", storagePolicyVersion=\"%d\"}.", key
                .getNamespace(), key.getStoragePolicyName(), storagePolicyVersion));
    }
}
