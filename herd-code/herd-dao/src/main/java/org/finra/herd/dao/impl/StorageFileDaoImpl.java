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

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageEntity_;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageFileEntity_;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitEntity_;

@Repository
public class StorageFileDaoImpl extends AbstractHerdDao implements StorageFileDao
{
    @Override
    public StorageFileEntity getStorageFileByStorageNameAndFilePath(String storageName, String filePath)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageFileEntity> criteria = builder.createQuery(StorageFileEntity.class);

        // The criteria root is the storage files.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate filePathRestriction = builder.equal(storageFileEntity.get(StorageFileEntity_.path), filePath);
        Predicate storageNameRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());

        criteria.select(storageFileEntity).where(builder.and(filePathRestriction, storageNameRestriction));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one storage file with parameters {storageName=\"%s\"," + " filePath=\"%s\"}.", storageName, filePath));
    }

    @Override
    public StorageFileEntity getStorageFileByStorageUnitEntityAndFilePath(StorageUnitEntity storageUnitEntity, String filePath)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageFileEntity> criteria = builder.createQuery(StorageFileEntity.class);

        // The criteria root is the storage files.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate filePathRestriction = builder.equal(storageFileEntity.get(StorageFileEntity_.path), filePath);
        Predicate storageUnitRestriction = builder.equal(storageFileEntity.get(StorageFileEntity_.storageUnitId), storageUnitEntity.getId());

        criteria.select(storageFileEntity).where(builder.and(filePathRestriction, storageUnitRestriction));

        return executeSingleResultQuery(criteria, String
            .format("Found more than one storage file with parameters {storageUnitId=\"%s\"," + " filePath=\"%s\"}.", storageUnitEntity.getId(), filePath));
    }

    @Override
    public Long getStorageFileCount(String storageName, String filePathPrefix)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);

        // The criteria root is the storage files.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create path.
        Expression<Long> storageFileCount = builder.count(storageFileEntity.get(StorageFileEntity_.id));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate storageNameRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());
        Predicate filePathRestriction = builder.like(storageFileEntity.get(StorageFileEntity_.path), String.format("%s%%", filePathPrefix));

        // Add the clauses for the query.
        criteria.select(storageFileCount).where(builder.and(storageNameRestriction, filePathRestriction));

        return entityManager.createQuery(criteria).getSingleResult();
    }

    @Override
    public MultiValuedMap<Integer, String> getStorageFilePathsByStorageUnitIds(List<Integer> storageUnitIds)
    {
        // Create a map that can hold a collection of values against each key.
        MultiValuedMap<Integer, String> result = new ArrayListValuedHashMap<>();

        // Retrieve the pagination size for the storage file paths query configured in the system.
        Integer paginationSize = configurationHelper.getProperty(ConfigurationValue.STORAGE_FILE_PATHS_QUERY_PAGINATION_SIZE, Integer.class);

        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage file.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Get the columns.
        Path<Integer> storageUnitIdColumn = storageFileEntity.get(StorageFileEntity_.storageUnitId);
        Path<String> storageFilePathColumn = storageFileEntity.get(StorageFileEntity_.path);
        Path<Integer> storageFileIdColumn = storageFileEntity.get(StorageFileEntity_.id);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = getPredicateForInClause(builder, storageUnitIdColumn, storageUnitIds);

        // Add the select clause.
        criteria.multiselect(storageUnitIdColumn, storageFilePathColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the orderBy to the query so we can get consistent pagination results
        criteria.orderBy(builder.asc(storageFileIdColumn));

        // Execute the query using pagination and populate the result map.
        int startPosition = 0;
        while (true)
        {
            // Run the query to get a list of tuples back.
            List<Tuple> tuples = entityManager.createQuery(criteria).setFirstResult(startPosition).setMaxResults(paginationSize).getResultList();

            // Populate the result map from the returned tuples (i.e. 1 tuple for each row).
            for (Tuple tuple : tuples)
            {
                // Extract the tuple values.
                Integer storageUnitId = tuple.get(storageUnitIdColumn);
                String storageFilePath = tuple.get(storageFilePathColumn);

                // Update the result map.
                result.put(storageUnitId, storageFilePath);
            }

            // Break out of the while loop if we got less results than the pagination size.
            if (tuples.size() < paginationSize)
            {
                break;
            }

            // Increment the start position.
            startPosition += paginationSize;
        }

        return result;
    }

    @Override
    public List<String> getStorageFilesByStorageAndFilePathPrefix(String storageName, String filePathPrefix)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageFileEntity> criteria = builder.createQuery(StorageFileEntity.class);

        // The criteria root is the storage files.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate filePathRestriction = builder.like(storageFileEntity.get(StorageFileEntity_.path), String.format("%s%%", filePathPrefix));
        Predicate storageNameRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());

        // Order the results by file path.
        Order orderByFilePath = builder.asc(storageFileEntity.get(StorageFileEntity_.path));

        criteria.select(storageFileEntity).where(builder.and(filePathRestriction, storageNameRestriction)).orderBy(orderByFilePath);

        // Retrieve the storage files.
        List<StorageFileEntity> storageFileEntities = entityManager.createQuery(criteria).getResultList();

        // Build the result list.
        List<String> storageFilePaths = new ArrayList<>();
        for (StorageFileEntity storageFile : storageFileEntities)
        {
            storageFilePaths.add(storageFile.getPath());
        }

        return storageFilePaths;
    }
}
