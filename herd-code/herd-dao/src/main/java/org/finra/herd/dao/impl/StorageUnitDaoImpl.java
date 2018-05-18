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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageEntity_;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity_;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitEntity_;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity_;

@Repository
public class StorageUnitDaoImpl extends AbstractHerdDao implements StorageUnitDao
{
    @Override
    public List<StorageUnitEntity> getLatestVersionStorageUnitsByStoragePlatformAndFileType(String storagePlatform, String businessObjectFormatFileType)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntityJoin = storageEntityJoin.join(StorageEntity_.storagePlatform);
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.businessObjectData);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(storagePlatformEntityJoin.get(StoragePlatformEntity_.name)), storagePlatform.toUpperCase()));
        predicates.add(builder.equal(builder.upper(fileTypeEntityJoin.get(FileTypeEntity_.code)), businessObjectFormatFileType.toUpperCase()));
        predicates.add(builder.isTrue(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.latestVersion)));
        predicates.add(builder.isTrue(businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.latestVersion)));

        // Order by storage unit created on timestamp.
        Order orderBy = builder.asc(storageUnitEntityRoot.get(StorageUnitEntity_.createdOn));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<StorageUnitEntity> getS3StorageUnitsToCleanup(int maxResult)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntityJoin = storageEntityJoin.join(StorageEntity_.storagePlatform);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.status);
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.businessObjectData);
        Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> businessObjectDataStatusEntity =
            businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.status);


        // Get the current time.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());

        // Create the standard restrictions (i.e. the standard where clauses).
        // Restrictions include:
        //      - Storage platform is set to S3 storage
        //      - Storage unit status is DISABLED
        //      - Associated BData has a DELETED status
        //      - Final destroy on timestamp < current time
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storagePlatformEntityJoin.get(StoragePlatformEntity_.name), StoragePlatformEntity.S3));
        predicates.add(builder.equal(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.code), StorageUnitStatusEntity.DISABLED));
        predicates.add(builder.equal(businessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code), BusinessObjectDataStatusEntity.DELETED));
        predicates.add(builder.lessThan(storageUnitEntityRoot.get(StorageUnitEntity_.finalDestroyOn), currentTime));

        // Order the results.
        Order orderBy = builder.asc(storageUnitEntityRoot.get(StorageUnitEntity_.finalDestroyOn));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).setMaxResults(maxResult).getResultList();
    }

    @Override
    public List<StorageUnitEntity> getS3StorageUnitsToExpire(int maxResult)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntityJoin = storageEntityJoin.join(StorageEntity_.storagePlatform);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.status);

        // Get the current time.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storagePlatformEntityJoin.get(StoragePlatformEntity_.name), StoragePlatformEntity.S3));
        predicates.add(builder.equal(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.code), StorageUnitStatusEntity.RESTORED));
        predicates.add(builder.lessThan(storageUnitEntityRoot.get(StorageUnitEntity_.restoreExpirationOn), currentTime));

        // Order the results.
        Order orderBy = builder.asc(storageUnitEntityRoot.get(StorageUnitEntity_.restoreExpirationOn));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).setMaxResults(maxResult).getResultList();
    }

    @Override
    public List<StorageUnitEntity> getS3StorageUnitsToRestore(int maxResult)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntityJoin = storageEntityJoin.join(StorageEntity_.storagePlatform);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.status);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storagePlatformEntityJoin.get(StoragePlatformEntity_.name), StoragePlatformEntity.S3));
        predicates.add(builder.equal(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.code), StorageUnitStatusEntity.RESTORING));

        // Order the results by storage unit updated on timestamp.
        Order orderBy = builder.asc(storageUnitEntityRoot.get(StorageUnitEntity_.updatedOn));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).setMaxResults(maxResult).getResultList();
    }

    @Override
    public StorageUnitEntity getStorageUnitByBusinessObjectDataAndStorage(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity storageEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storageUnitEntityRoot.get(StorageUnitEntity_.businessObjectData), businessObjectDataEntity));
        predicates.add(builder.equal(storageUnitEntityRoot.get(StorageUnitEntity_.storage), storageEntity));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query.
        List<StorageUnitEntity> resultList = entityManager.createQuery(criteria).getResultList();

        // Return single result or null.
        return resultList.size() >= 1 ? resultList.get(0) : null;
    }

    @Override
    public StorageUnitEntity getStorageUnitByBusinessObjectDataAndStorageName(BusinessObjectDataEntity businessObjectDataEntity, String storageName)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);
        Root<StorageUnitEntity> storageUnitEntity = criteria.from(StorageUnitEntity.class);

        // join storage unit to storage to retrieve name
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // where business object data equals
        Predicate businessObjectDataRestriction = builder.equal(storageUnitEntity.get(StorageUnitEntity_.businessObjectData), businessObjectDataEntity);
        // where storage name equals, ignoring case
        Predicate storageNameRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());

        criteria.select(storageUnitEntity).where(builder.and(businessObjectDataRestriction, storageNameRestriction));

        List<StorageUnitEntity> resultList = entityManager.createQuery(criteria).getResultList();

        // return single result or null
        return resultList.size() >= 1 ? resultList.get(0) : null;
    }

    @Override
    public StorageUnitEntity getStorageUnitByKey(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.businessObjectData);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates
            .add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), businessObjectDataStorageUnitKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataStorageUnitKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataStorageUnitKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatUsage().toUpperCase()));
        predicates.add(builder.equal(builder.upper(fileTypeEntityJoin.get(FileTypeEntity_.code)),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatFileType().toUpperCase()));
        predicates.add(builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatVersion()));
        predicates.add(getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntityJoin, businessObjectDataStorageUnitKey.getPartitionValue(),
            businessObjectDataStorageUnitKey.getSubPartitionValues()));
        predicates.add(builder
            .equal(businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.version), businessObjectDataStorageUnitKey.getBusinessObjectDataVersion()));
        predicates
            .add(builder.equal(builder.upper(storageEntityJoin.get(StorageEntity_.name)), businessObjectDataStorageUnitKey.getStorageName().toUpperCase()));

        // Add the clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object data storage unit instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\"," +
                " businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", businessObjectDataVersion=\"%d\"," +
                " storageName=\"%s\"}.", businessObjectDataStorageUnitKey.getNamespace(), businessObjectDataStorageUnitKey.getBusinessObjectDefinitionName(),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatUsage(), businessObjectDataStorageUnitKey.getBusinessObjectFormatFileType(),
            businessObjectDataStorageUnitKey.getBusinessObjectFormatVersion(), businessObjectDataStorageUnitKey.getPartitionValue(),
            CollectionUtils.isEmpty(businessObjectDataStorageUnitKey.getSubPartitionValues()) ? "" :
                StringUtils.join(businessObjectDataStorageUnitKey.getSubPartitionValues(), ","),
            businessObjectDataStorageUnitKey.getBusinessObjectDataVersion(), businessObjectDataStorageUnitKey.getStorageName()));
    }

    @Override
    public StorageUnitEntity getStorageUnitByStorageAndDirectoryPath(StorageEntity storageEntity, String directoryPath)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storageUnitEntityRoot.get(StorageUnitEntity_.storage), storageEntity));
        predicates.add(builder.equal(storageUnitEntityRoot.get(StorageUnitEntity_.directoryPath), directoryPath));

        // Add all clauses for the query.
        criteria.select(storageUnitEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the first found storage unit or null if none were found.
        List<StorageUnitEntity> storageUnitEntities = entityManager.createQuery(criteria).getResultList();
        return storageUnitEntities.size() >= 1 ? storageUnitEntities.get(0) : null;
    }

    @Override
    public List<StorageUnitEntity> getStorageUnitsByPartitionFiltersAndStorages(BusinessObjectFormatKey businessObjectFormatKey,
        List<List<String>> partitionFilters, Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames,
        String storagePlatformType, String excludedStoragePlatformType, boolean selectOnlyAvailableStorageUnits)
    {
        List<StorageUnitEntity> resultStorageUnitEntities = new ArrayList<>();

        // Loop through each chunk of partition filters until we have reached the end of the list.
        for (int i = 0; i < partitionFilters.size(); i += MAX_PARTITION_FILTERS_PER_REQUEST)
        {
            // Get a sub-list for the current chunk of partition filters.
            List<StorageUnitEntity> storageUnitEntitiesSubset =
                getStorageUnitsByPartitionFiltersAndStorages(businessObjectFormatKey, partitionFilters, businessObjectDataVersion, businessObjectDataStatus,
                    storageNames, storagePlatformType, excludedStoragePlatformType, selectOnlyAvailableStorageUnits, i,
                    (i + MAX_PARTITION_FILTERS_PER_REQUEST) > partitionFilters.size() ? partitionFilters.size() - i : MAX_PARTITION_FILTERS_PER_REQUEST);

            // Add the sub-list to the result.
            resultStorageUnitEntities.addAll(storageUnitEntitiesSubset);
        }

        return resultStorageUnitEntities;
    }

    @Override
    public List<StorageUnitEntity> getStorageUnitsByStorageAndBusinessObjectData(StorageEntity storageEntity,
        List<BusinessObjectDataEntity> businessObjectDataEntities)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntity = criteria.from(StorageUnitEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(storageUnitEntity.get(StorageUnitEntity_.storage), storageEntity);
        queryRestriction = builder
            .and(queryRestriction, getPredicateForInClause(builder, storageUnitEntity.get(StorageUnitEntity_.businessObjectData), businessObjectDataEntities));

        // Add the clauses for the query.
        criteria.select(storageUnitEntity).where(queryRestriction);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<StorageUnitEntity> getStorageUnitsByStoragePlatformAndBusinessObjectData(String storagePlatform,
        BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntity = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntity = storageEntity.join(StorageEntity_.storagePlatform);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(storagePlatformEntity.get(StoragePlatformEntity_.name)), storagePlatform.toUpperCase()));
        predicates.add(builder.equal(storageUnitEntity.get(StorageUnitEntity_.businessObjectData), businessObjectDataEntity));

        // Order by storage name.
        Order orderBy = builder.asc(storageEntity.get(StorageEntity_.name));

        // Add the clauses for the query.
        criteria.select(storageUnitEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    /**
     * Builds a sub-query to select the maximum business object data version.
     *
     * @param builder the criteria builder
     * @param criteria the criteria query
     * @param businessObjectDefinitionEntity the business object definition entity that appears in the from clause of the main query
     * @param businessObjectFormatEntity the business object format entity that appears in the from clause of the main query
     * @param fileTypeEntity the file type entity that appears in the from clause of the main query
     * @param businessObjectDataEntity the business object data entity that appears in the from clause of the main query
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status is returned
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified. When
     * business object data version and business object data status both are not specified, the latest data version for each set of partition values will be
     * used regardless of the status
     * @param storageNames the list of storage names where the business object data storage units should be looked for (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     *
     * @return the sub-query to select the maximum business object data version
     */
    private Subquery<Integer> getMaximumBusinessObjectFormatVersionSubQuery(CriteriaBuilder builder, CriteriaQuery<?> criteria,
        From<?, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        From<?, FileTypeEntity> fileTypeEntity, From<?, BusinessObjectDataEntity> businessObjectDataEntity, Integer businessObjectDataVersion,
        String businessObjectDataStatus, List<String> storageNames, String storagePlatformType, String excludedStoragePlatformType,
        boolean selectOnlyAvailableStorageUnits)
    {
        // Business object format version is not specified, so just use the latest available for this set of partition values.
        Subquery<Integer> subQuery = criteria.subquery(Integer.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> subStorageUnitEntity = subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> subStorageEntity = subStorageUnitEntity.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> subStoragePlatformEntity = subStorageEntity.join(StorageEntity_.storagePlatform);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
            subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> subBusinessObjectDefinitionEntity =
            subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> subBusinessObjectFormatFileTypeEntity =
            subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> subBusinessObjectDataStatusEntity =
            subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.status);
        Join<StorageUnitEntity, StorageUnitStatusEntity> subStorageUnitStatusEntity = subStorageUnitEntity.join(StorageUnitEntity_.status);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate subQueryRestriction = builder.equal(subBusinessObjectDefinitionEntity, businessObjectDefinitionEntity);
        subQueryRestriction = builder.and(subQueryRestriction, builder
            .equal(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage), businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)));
        subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subBusinessObjectFormatFileTypeEntity, fileTypeEntity));

        // Create and add standard restrictions on primary and sub-partition values.
        subQueryRestriction =
            builder.and(subQueryRestriction, getQueryRestrictionOnPartitionValues(builder, subBusinessObjectDataEntity, businessObjectDataEntity));

        // Add restrictions on business object data version and business object data status.
        Predicate subQueryRestrictionOnBusinessObjectDataVersionAndStatus =
            getQueryRestrictionOnBusinessObjectDataVersionAndStatus(builder, subBusinessObjectDataEntity, subBusinessObjectDataStatusEntity,
                businessObjectDataVersion, businessObjectDataStatus);
        if (subQueryRestrictionOnBusinessObjectDataVersionAndStatus != null)
        {
            subQueryRestriction = builder.and(subQueryRestriction, subQueryRestrictionOnBusinessObjectDataVersionAndStatus);
        }

        // Create and add a standard restriction on storage.
        subQueryRestriction = builder.and(subQueryRestriction,
            getQueryRestrictionOnStorage(builder, subStorageEntity, subStoragePlatformEntity, storageNames, storagePlatformType, excludedStoragePlatformType));

        // If specified, add a restriction on storage unit status availability flag.
        if (selectOnlyAvailableStorageUnits)
        {
            subQueryRestriction = builder.and(subQueryRestriction, builder.isTrue(subStorageUnitStatusEntity.get(StorageUnitStatusEntity_.available)));
        }

        // Add all of the clauses to the subquery.
        subQuery.select(builder.max(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion))).where(subQueryRestriction);

        return subQuery;
    }

    /**
     * Retrieves a list of storage unit entities per specified parameters. This method processes a sublist of partition filters specified by
     * partitionFilterSubListFromIndex and partitionFilterSubListSize parameters.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status is returned
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified. When
     * business object data version and business object data status both are not specified, the latest data version for each set of partition values will be
     * used regardless of the status
     * @param storageNames the list of storage names where the business object data storage units should be looked for (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param partitionFilterSubListFromIndex the index of the first element in the partition filter sublist
     * @param partitionFilterSubListSize the size of the partition filter sublist
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     *
     * @return the list of storage unit entities sorted by partition values
     */
    private List<StorageUnitEntity> getStorageUnitsByPartitionFiltersAndStorages(BusinessObjectFormatKey businessObjectFormatKey,
        List<List<String>> partitionFilters, Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames,
        String storagePlatformType, String excludedStoragePlatformType, boolean selectOnlyAvailableStorageUnits, int partitionFilterSubListFromIndex,
        int partitionFilterSubListSize)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntity = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntity = storageUnitEntity.join(StorageUnitEntity_.businessObjectData);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntity = storageEntity.join(StorageEntity_.storagePlatform);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntity = storageUnitEntity.join(StorageUnitEntity_.status);

        // Create the standard restrictions (i.e. the standard where clauses).

        // Create a standard restriction based on the business object format key values.
        // Please note that we specify not to ignore the business object format version.
        Predicate mainQueryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity, businessObjectFormatKey, false);

        // If a format version was not specified, use the latest available for this set of partition values.
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() == null)
        {
            // Get the latest available format version for this set of partition values and per other restrictions.
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectFormatVersionSubQuery(builder, criteria, businessObjectDefinitionEntity, businessObjectFormatEntity, fileTypeEntity,
                    businessObjectDataEntity, businessObjectDataVersion, businessObjectDataStatus, storageNames, storagePlatformType,
                    excludedStoragePlatformType, selectOnlyAvailableStorageUnits);

            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.in(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)).value(subQuery));
        }

        // Add restriction as per specified primary and/or sub-partition values.
        mainQueryRestriction = builder.and(mainQueryRestriction, getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntity,
            partitionFilters.subList(partitionFilterSubListFromIndex, partitionFilterSubListFromIndex + partitionFilterSubListSize)));

        // If a data version was specified, use it. Otherwise, use the latest one as per specified business object data status.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        else
        {
            // Business object data version is not specified, so get the latest one as per specified business object data status, if any.
            // Meaning, when both business object data version and business object data status are not specified, we just return
            // the latest business object data version in the specified storage.
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectDataVersionSubQuery(builder, criteria, businessObjectDataEntity, businessObjectFormatEntity, businessObjectDataStatus,
                    storageNames, storagePlatformType, excludedStoragePlatformType, selectOnlyAvailableStorageUnits);

            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.in(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)).value(subQuery));
        }

        // If specified, add restriction on storage.
        mainQueryRestriction = builder.and(mainQueryRestriction,
            getQueryRestrictionOnStorage(builder, storageEntity, storagePlatformEntity, storageNames, storagePlatformType, excludedStoragePlatformType));

        // If specified, add a restriction on storage unit status availability flag.
        if (selectOnlyAvailableStorageUnits)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction, builder.isTrue(storageUnitStatusEntity.get(StorageUnitStatusEntity_.available)));
        }

        // Order by partitions and storage names.
        List<Order> orderBy = new ArrayList<>();
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartition : BUSINESS_OBJECT_DATA_PARTITIONS)
        {
            orderBy.add(builder.asc(businessObjectDataEntity.get(businessObjectDataPartition)));
        }
        orderBy.add(builder.asc(storageEntity.get(StorageEntity_.name)));

        // Add the clauses for the query.
        // Please note that we use multiselect here in order to eliminate the Hibernate N+1 SELECT's problem,
        // happening when we select storage unit entities and access their relative business object data entities.
        // This is an alternative approach, since adding @Fetch(FetchMode.JOIN) failed to address the issue.
        criteria
            .multiselect(storageUnitEntity, storageUnitStatusEntity, storageEntity, storagePlatformEntity, businessObjectDataEntity, businessObjectFormatEntity)
            .where(mainQueryRestriction).orderBy(orderBy);

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Build a list of storage unit entities to return.
        List<StorageUnitEntity> storageUnitEntities = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            storageUnitEntities.add(tuple.get(storageUnitEntity));
        }

        return storageUnitEntities;
    }
}
