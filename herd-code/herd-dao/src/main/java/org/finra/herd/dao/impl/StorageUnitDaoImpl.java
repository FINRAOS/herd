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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.SingularAttribute;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
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
        predicates.add(builder.equal(storageUnitEntityRoot.get(StorageUnitEntity_.storageName), storageEntity.getName()));

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
    public List<StorageUnitAvailabilityDto> getStorageUnitsByPartitionFilters(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectFormatUsage, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity, boolean selectOnlyAvailableStorageUnits,
        XMLGregorianCalendar asOfTime)
    {
        List<StorageUnitAvailabilityDto> results = new ArrayList<>();

        // Loop through each chunk of partition filters until we have reached the end of the list.
        for (int i = 0; i < partitionFilters.size(); i += MAX_PARTITION_FILTERS_PER_REQUEST)
        {
            // Get a sub-list for the current chunk of partition filters.
            List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtosSubset =
                getStorageUnitsByPartitionFilters(businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity, businessObjectFormatVersion,
                    partitionFilters, businessObjectDataVersion, businessObjectDataStatusEntity, storageEntities, storagePlatformEntity,
                    excludedStoragePlatformEntity, selectOnlyAvailableStorageUnits, i,
                    (i + MAX_PARTITION_FILTERS_PER_REQUEST) > partitionFilters.size() ? partitionFilters.size() - i : MAX_PARTITION_FILTERS_PER_REQUEST,
                    asOfTime);

            // Add the sub-list to the result.
            results.addAll(storageUnitAvailabilityDtosSubset);
        }

        return results;
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
     * Retrieves a list of storage unit availability DTOs per specified parameters. This method processes a sublist of partition filters specified by
     * partitionFilterSubListFromIndex and partitionFilterSubListSize parameters.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectFormatUsage the business object format usage (case-insensitive)
     * @param fileTypeEntity the file type entity
     * @param businessObjectFormatVersion the optional business object format version. If a business object format version isn't specified, the latest
     * available format version for each partition value will be used
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls
     * @param businessObjectDataVersion the optional business object data version. If a business object data version isn't specified, the latest data version
     * based on the specified business object data status is returned
     * @param businessObjectDataStatusEntity the optional business object data status entity. This parameter is ignored when the business object data version
     * is specified. When business object data version and business object data status both are not specified, the latest data version for each set of
     * partition values will be used regardless of the status
     * @param storageEntities the optional list of storage entities where business object data storage units should be looked for
     * @param storagePlatformEntity the optional storage platform entity, e.g. S3 for Hive DDL. It is ignored when the list of storage entities is not empty
     * @param excludedStoragePlatformEntity the optional storage platform entity to be excluded from search. It is ignored when the list of storage entities is
     * not empty or the storage platform entity is specified
     * @param partitionFilterSubListFromIndex the index of the first element in the partition filter sublist
     * @param partitionFilterSubListSize the size of the partition filter sublist
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     * @param asOfTime specifies the date and time when the business object status was effective at that time
     *
     * @return the list of storage unit availability DTOs sorted by partition values
     */
    private List<StorageUnitAvailabilityDto> getStorageUnitsByPartitionFilters(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectFormatUsage, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity, boolean selectOnlyAvailableStorageUnits,
        int partitionFilterSubListFromIndex, int partitionFilterSubListSize, XMLGregorianCalendar asOfTime)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage unit.
        Root<StorageUnitEntity> storageUnitEntityRoot = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.businessObjectData);
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.storage);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityRoot.join(StorageUnitEntity_.status);

        // Create standard restriction based on the business object format alternate key values including business object format version, if it is specified.
        Predicate mainQueryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntityJoin, businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity,
                businessObjectFormatVersion);

        // Add restriction as per specified primary and/or sub-partition values.
        mainQueryRestriction = builder.and(mainQueryRestriction, getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntityJoin,
            partitionFilters.subList(partitionFilterSubListFromIndex, partitionFilterSubListFromIndex + partitionFilterSubListSize)));

        // If specified, add restriction on business object data version.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.equal(businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        // Otherwise, add restriction on business object data status, if specified.
        else if (businessObjectDataStatusEntity != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction,
                builder.equal(businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.statusCode), businessObjectDataStatusEntity.getCode()));
        }

        // If specified valid as of time, add join and restriction on status history
        if (asOfTime != null)
        {
            Join<BusinessObjectDataEntity, BusinessObjectDataStatusHistoryEntity> businessObjectDataHistoryEntityJoin =
                businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.historicalStatuses);

            mainQueryRestriction = builder.and(mainQueryRestriction, builder
                .lessThan(businessObjectDataHistoryEntityJoin.get(BusinessObjectDataEntity_.updatedOn),
                    new Timestamp(asOfTime.toGregorianCalendar().getTimeInMillis())));
        }

        // If specified, add restriction on storage.
        mainQueryRestriction = builder.and(mainQueryRestriction,
            getQueryRestrictionOnStorage(builder, storageEntityJoin, storageEntities, storagePlatformEntity, excludedStoragePlatformEntity));

        // If specified, add a restriction on storage unit status availability flag.
        if (selectOnlyAvailableStorageUnits)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction, builder.isTrue(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.available)));
        }

        // Order by partitions and storage names.
        List<Order> orderBy = new ArrayList<>();
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartition : BUSINESS_OBJECT_DATA_PARTITIONS)
        {
            orderBy.add(builder.asc(businessObjectDataEntityJoin.get(businessObjectDataPartition)));
        }
        orderBy.add(builder.asc(storageEntityJoin.get(StorageEntity_.name)));

        // Get the columns.
        Path<Integer> storageUnitIdColumn = storageUnitEntityRoot.get(StorageUnitEntity_.id);
        Path<String> businessObjectFormatUsageColumn = businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage);
        Path<Integer> businessObjectFormatVersionColumn = businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion);
        Path<String> primaryPartitionValueColumn = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.partitionValue);
        Path<String> subPartitionValue1Column = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.partitionValue2);
        Path<String> subPartitionValue2Column = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.partitionValue3);
        Path<String> subPartitionValue3Column = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.partitionValue4);
        Path<String> subPartitionValue4Column = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.partitionValue5);
        Path<Integer> businessObjectDataVersionColumn = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.version);
        Path<String> storageNameColumn = storageEntityJoin.get(StorageEntity_.name);
        Path<String> storageUnitDirectoryPathColumn = storageUnitEntityRoot.get(StorageUnitEntity_.directoryPath);
        Path<String> businessObjectDataStatusColumn = businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.statusCode);
        Path<String> storageUnitStatusColumn = storageUnitEntityRoot.get(StorageUnitEntity_.statusCode);
        Path<Boolean> storageUnitAvailableColumn = storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.available);

        // Add the clauses for the query.
        criteria.multiselect(storageUnitIdColumn, businessObjectFormatUsageColumn, businessObjectFormatVersionColumn, primaryPartitionValueColumn,
            subPartitionValue1Column, subPartitionValue2Column, subPartitionValue3Column, subPartitionValue4Column, businessObjectDataVersionColumn,
            storageNameColumn, storageUnitDirectoryPathColumn, businessObjectDataStatusColumn, storageUnitStatusColumn, storageUnitAvailableColumn)
            .where(mainQueryRestriction).orderBy(orderBy);

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Build a list of storage unit availability DTOs.
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            storageUnitAvailabilityDtos.add(new StorageUnitAvailabilityDto(tuple.get(storageUnitIdColumn),
                new BusinessObjectDataKey(businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(),
                    tuple.get(businessObjectFormatUsageColumn), fileTypeEntity.getCode(), tuple.get(businessObjectFormatVersionColumn),
                    tuple.get(primaryPartitionValueColumn), getSubPartitionValuesFromRawSubPartitionValues(Arrays
                    .asList(tuple.get(subPartitionValue1Column), tuple.get(subPartitionValue2Column), tuple.get(subPartitionValue3Column),
                        tuple.get(subPartitionValue4Column))), tuple.get(businessObjectDataVersionColumn)), tuple.get(storageNameColumn),
                tuple.get(storageUnitDirectoryPathColumn), tuple.get(businessObjectDataStatusColumn), tuple.get(storageUnitStatusColumn),
                tuple.get(storageUnitAvailableColumn)));
        }

        // If at least one of the business object format and business object data versions was not specified,
        // filter in only latest version for each set of partition values.
        if (businessObjectFormatVersion == null || businessObjectDataVersion == null)
        {
            // Use linked hash map to filter entries and to preserve the original order of the entries returned by the database while doing it.
            // Please note that each set of partition values might have multiple identical latest versions registered in different storage entities.
            Map<List<String>, List<StorageUnitAvailabilityDto>> latestVersions = new LinkedHashMap<>();

            // Process the list of storage unit availability DTOs to select the latest versions.
            for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
            {
                // Retrieve business object data key and a list of partition values for this storage unit availability DTO.
                BusinessObjectDataKey businessObjectDataKey = storageUnitAvailabilityDto.getBusinessObjectDataKey();
                List<String> partitionValues = getPartitionValues(businessObjectDataKey);

                // If hash map already contains this set of partition values, check if this entry's business object format and business object data versions
                // against the entities currently stored in the hash map.  Since all stored entities for a particular set of partition values can only
                // have the same business object format and business object data versions, we always check only against the first stored entity.
                if (latestVersions.containsKey(partitionValues))
                {
                    // Get stored storage unit availability DTO currently stored for this set of partition values.
                    List<StorageUnitAvailabilityDto> partitionValuesLatestVersions = latestVersions.get(partitionValues);

                    // Get business object key for the stored storage unit availability DTO currently stored for this set of partition values.
                    // At least one DTO is expected to be present in the list, so we don't check for null here.
                    BusinessObjectDataKey latestVersionKey = partitionValuesLatestVersions.get(0).getBusinessObjectDataKey();

                    // If this entity has business object format version which is greater than the stored entity's version,
                    // overwrite all entities stored for this set of partition values with this one.
                    if (businessObjectDataKey.getBusinessObjectFormatVersion() > latestVersionKey.getBusinessObjectFormatVersion())
                    {
                        latestVersions.put(partitionValues, Lists.newArrayList(storageUnitAvailabilityDto));
                    }
                    // If this entity has business object format version which is equal to the stored entity's, then check business object data versions
                    else if (Objects.equals(businessObjectDataKey.getBusinessObjectFormatVersion(), latestVersionKey.getBusinessObjectFormatVersion()))
                    {
                        // If this entity has business object data version which is greater than the stored entity's version,
                        // overwrite all entities stored for this set of partition values with this one.
                        if (businessObjectDataKey.getBusinessObjectDataVersion() > latestVersionKey.getBusinessObjectDataVersion())
                        {
                            latestVersions.put(partitionValues, Lists.newArrayList(storageUnitAvailabilityDto));
                        }
                        // If this entity has business object data version which is equal to the stored entity's version,
                        // add this entity to the entities currently stored for this set of partition values.
                        else if (Objects.equals(businessObjectDataKey.getBusinessObjectDataVersion(), latestVersionKey.getBusinessObjectDataVersion()))
                        {
                            partitionValuesLatestVersions.add(storageUnitAvailabilityDto);
                        }
                    }
                }
                // Otherwise, just insert that new set of partition values into the hash map.
                else
                {
                    latestVersions.put(partitionValues, Lists.newArrayList(storageUnitAvailabilityDto));
                }
            }

            // Update the list of  storage unit availability DTOs to be returned per filtering results.
            storageUnitAvailabilityDtos.clear();
            for (List<StorageUnitAvailabilityDto> partitionValuesLatestVersions : latestVersions.values())
            {
                storageUnitAvailabilityDtos.addAll(partitionValuesLatestVersions);
            }
        }

        return storageUnitAvailabilityDtos;
    }

    /**
     * Returns all partition values from the specified business object data key as a list of strings.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the list of partition values
     */
    private List<String> getPartitionValues(BusinessObjectDataKey businessObjectDataKey)
    {
        List<String> partitionValues = new ArrayList<>();
        partitionValues.add(businessObjectDataKey.getPartitionValue());
        partitionValues.addAll(businessObjectDataKey.getSubPartitionValues());
        return partitionValues;
    }

    /**
     * Gets the sub-partition values for the specified list of raw sub-partition values.
     *
     * @param rawSubPartitionValues the list of raw sub-partition values
     *
     * @return the list of sub-partition values
     */
    private List<String> getSubPartitionValuesFromRawSubPartitionValues(List<String> rawSubPartitionValues)
    {
        List<String> subPartitionValues = new ArrayList<>();

        for (String rawSubPartitionValue : rawSubPartitionValues)
        {
            if (rawSubPartitionValue != null)
            {
                subPartitionValues.add(rawSubPartitionValue);
            }
            else
            {
                break;
            }
        }

        return subPartitionValues;
    }
}
