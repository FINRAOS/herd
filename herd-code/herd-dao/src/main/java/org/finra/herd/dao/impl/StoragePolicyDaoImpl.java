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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import com.google.common.collect.Lists;
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

    @Override
    public List<StoragePolicyKey> getStoragePolicyKeysByNamespace(NamespaceEntity namespaceEntity)
    {
        // Create criteria builder and a top-level query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the Storage policy.
        Root<StoragePolicyEntity> emrClusterDefinitionEntityRoot = criteria.from(StoragePolicyEntity.class);

        // Get the Storage Policy name column.
        Path<String> storagePolicyName = emrClusterDefinitionEntityRoot.get(StoragePolicyEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(emrClusterDefinitionEntityRoot.get(StoragePolicyEntity_.namespace), namespaceEntity);

        // Add all clauses for the query.
        criteria.select(storagePolicyName).where(predicate).orderBy(builder.asc(storagePolicyName));

        // Execute the query to get a list of storage policy names back.
        List<String> storagePolicyNames = entityManager.createQuery(criteria).getResultList();

        // Build a list of storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Lists.newArrayList();
        for (String emrClusterDefinitionName : storagePolicyNames)
        {
            storagePolicyKeys.add(new StoragePolicyKey(namespaceEntity.getCode(), emrClusterDefinitionName));
        }

        // Return the list of keys.
        return storagePolicyKeys;
    }
}
