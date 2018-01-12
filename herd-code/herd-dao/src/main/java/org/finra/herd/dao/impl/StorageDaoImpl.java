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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.StorageDao;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageEntity_;

@Repository
public class StorageDaoImpl extends AbstractHerdDao implements StorageDao
{
    @Override
    public List<StorageKey> getAllStorage()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the storage.
        Root<StorageEntity> storageEntity = criteria.from(StorageEntity.class);

        // Get the columns.
        Path<String> storageNameColumn = storageEntity.get(StorageEntity_.name);

        // Add the select clause.
        criteria.select(storageNameColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(storageNameColumn));

        // Run the query to get a list of storage names back.
        List<String> storageNames = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned storage names.
        List<StorageKey> storageKeys = new ArrayList<>();
        for (String storageName : storageNames)
        {
            storageKeys.add(new StorageKey(storageName));
        }

        return storageKeys;
    }

    @Override
    public StorageEntity getStorageByName(String storageName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageEntity> criteria = builder.createQuery(StorageEntity.class);

        // The criteria root is the namespace.
        Root<StorageEntity> storageEntity = criteria.from(StorageEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());

        criteria.select(storageEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one storage with \"%s\" name.", storageName));
    }
}
