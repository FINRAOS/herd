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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Tuple;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.RegistrationDateRangeFilter;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity_;
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
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity_;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageEntity_;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity_;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity_;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity_;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitEntity_;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity_;

@Repository
public class BusinessObjectDataDaoImpl extends AbstractHerdDao implements BusinessObjectDataDao
{
    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Autowired
    private NamespaceDao namespaceDao;

    @Override
    public BusinessObjectDataEntity getBusinessObjectDataByAltKey(BusinessObjectDataKey businessObjectDataKey)
    {
        return getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, null);
    }

    @Override
    public BusinessObjectDataEntity getBusinessObjectDataByAltKeyAndStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to other tables that we need to filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate mainQueryRestriction =
            getQueryRestriction(builder, businessObjectDataEntity, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity,
                businessObjectDataKey);

        // If a format version was specified, use the latest available for this partition value.
        if (businessObjectDataKey.getBusinessObjectFormatVersion() == null)
        {
            // Business object format version is not specified, so just use the latest available for this set of partition values.
            Subquery<Integer> subQuery = criteria.subquery(Integer.class);

            // The criteria root is the business object data.
            Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

            // Join to the other tables we can filter on.
            Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
                subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
            Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> subBusinessObjectDefinitionEntity =
                subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
            Join<BusinessObjectFormatEntity, FileTypeEntity> subBusinessObjectFormatFileTypeEntity =
                subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
            Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> subBusinessObjectDataStatusEntity =
                subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.status);

            // Create the standard restrictions (i.e. the standard where clauses).
            Predicate subQueryRestriction = builder.equal(subBusinessObjectDefinitionEntity, businessObjectDefinitionEntity);
            subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage),
                businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)));
            subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subBusinessObjectFormatFileTypeEntity, fileTypeEntity));

            // Create and add standard restrictions on primary and sub-partition values.
            subQueryRestriction =
                builder.and(subQueryRestriction, getQueryRestrictionOnPartitionValues(builder, subBusinessObjectDataEntity, businessObjectDataEntity));

            // Add restrictions on business object data version and business object data status.
            Predicate subQueryRestrictionOnBusinessObjectDataVersionAndStatus =
                getQueryRestrictionOnBusinessObjectDataVersionAndStatus(builder, subBusinessObjectDataEntity, subBusinessObjectDataStatusEntity,
                    businessObjectDataKey.getBusinessObjectDataVersion(), businessObjectDataStatus);
            if (subQueryRestrictionOnBusinessObjectDataVersionAndStatus != null)
            {
                subQueryRestriction = builder.and(subQueryRestriction, subQueryRestrictionOnBusinessObjectDataVersionAndStatus);
            }

            subQuery.select(builder.max(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion))).where(subQueryRestriction);

            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.in(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)).value(subQuery));
        }

        // If a data version was not specified, use the latest one as per specified business object data status.
        if (businessObjectDataKey.getBusinessObjectDataVersion() == null)
        {
            // Since business object data version is not specified, just use the latest one as per specified business object data status.
            if (businessObjectDataStatus != null)
            {
                // Business object data version is not specified, so get the latest one as per specified business object data status.
                Subquery<Integer> subQuery = criteria.subquery(Integer.class);

                // The criteria root is the business object data.
                Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

                // Join to the other tables we can filter on.
                Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> subBusinessObjectDataStatusEntity =
                    subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.status);
                Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
                    subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);

                // Create the standard restrictions (i.e. the standard where clauses).
                Predicate subQueryRestriction = builder.equal(subBusinessObjectFormatEntity, businessObjectFormatEntity);

                // Create and add standard restrictions on primary and sub-partition values.
                subQueryRestriction =
                    builder.and(subQueryRestriction, getQueryRestrictionOnPartitionValues(builder, subBusinessObjectDataEntity, businessObjectDataEntity));

                // Create and add standard restrictions on business object data status.
                subQueryRestriction = builder.and(subQueryRestriction, builder
                    .equal(builder.upper(subBusinessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code)), businessObjectDataStatus.toUpperCase()));

                subQuery.select(builder.max(subBusinessObjectDataEntity.get(BusinessObjectDataEntity_.version))).where(subQueryRestriction);

                mainQueryRestriction =
                    builder.and(mainQueryRestriction, builder.in(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)).value(subQuery));
            }
            else
            {
                // Both business object data version and business object data status are not specified, so just use the latest business object data version.
                mainQueryRestriction =
                    builder.and(mainQueryRestriction, builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.latestVersion), true));
            }
        }

        criteria.select(businessObjectDataEntity).where(mainQueryRestriction);

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object data instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\"," +
                " businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", businessObjectDataVersion=\"%d\"," +
                " businessObjectDataStatus=\"%s\"}.", businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
            CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" : StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","),
            businessObjectDataKey.getBusinessObjectDataVersion(), businessObjectDataStatus));
    }

    @Override
    public Integer getBusinessObjectDataMaxVersion(BusinessObjectDataKey businessObjectDataKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Integer> criteria = builder.createQuery(Integer.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the path.
        Expression<Integer> maxBusinessObjectDataVersion = builder.max(businessObjectDataEntity.get(BusinessObjectDataEntity_.version));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectDataKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            businessObjectDataKey.getBusinessObjectFormatUsage().toUpperCase()));
        queryRestriction = builder.and(queryRestriction,
            builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectDataKey.getBusinessObjectFormatFileType().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            businessObjectDataKey.getBusinessObjectFormatVersion()));
        queryRestriction = builder.and(queryRestriction,
            builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), businessObjectDataKey.getPartitionValue()));

        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            queryRestriction = builder.and(queryRestriction, i < businessObjectDataKey.getSubPartitionValues().size() ?
                builder.equal(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i)), businessObjectDataKey.getSubPartitionValues().get(i)) :
                builder.isNull(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i))));
        }

        criteria.select(maxBusinessObjectDataVersion).where(queryRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    @Override
    public String getBusinessObjectDataMaxPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType, String upperBoundPartitionValue, String lowerBoundPartitionValue)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatus,
            storageNames, storagePlatformType, excludedStoragePlatformType, AggregateFunction.GREATEST, upperBoundPartitionValue, lowerBoundPartitionValue);
    }

    @Override
    public String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatus,
            storageNames, storagePlatformType, excludedStoragePlatformType, AggregateFunction.LEAST, null, null);
    }

    @Override
    public Long getBusinessObjectDataCount(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create path.
        Expression<Long> businessObjectDataCount = builder.count(businessObjectDataEntity.get(BusinessObjectDataEntity_.id));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectFormatKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectFormatKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            businessObjectFormatKey.getBusinessObjectFormatUsage().toUpperCase()));
        queryRestriction = builder.and(queryRestriction,
            builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectFormatKey.getBusinessObjectFormatFileType().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            businessObjectFormatKey.getBusinessObjectFormatVersion()));

        criteria.select(businessObjectDataCount).where(queryRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    @Override
    public List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectDataKey businessObjectDataKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            getQueryRestriction(builder, businessObjectDataEntity, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity,
                businessObjectDataKey);

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntity).where(queryRestriction);

        // Order by business object format and data versions.
        criteria.orderBy(builder.asc(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)),
            builder.asc(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)));

        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectFormatKey businessObjectFormatKey, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, String businessObjectDataStatus, String storageName)
    {
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = new ArrayList<>();

        // Loop through each chunk of partition filters until we have reached the end of the list.
        for (int i = 0; i < partitionFilters.size(); i += MAX_PARTITION_FILTERS_PER_REQUEST)
        {
            // Get a sub-list for the current chunk of partition filters.
            List<BusinessObjectDataEntity> chunkBusinessObjectDataEntities =
                getBusinessObjectDataEntities(businessObjectFormatKey, partitionFilters, businessObjectDataVersion, businessObjectDataStatus, storageName, i,
                    (i + MAX_PARTITION_FILTERS_PER_REQUEST) > partitionFilters.size() ? partitionFilters.size() - i : MAX_PARTITION_FILTERS_PER_REQUEST);

            // Add the sub-list to the result.
            resultBusinessObjectDataEntities.addAll(chunkBusinessObjectDataEntities);
        }

        return resultBusinessObjectDataEntities;
    }

    @Override
    public List<BusinessObjectDataEntity> getBusinessObjectDataFromStorageOlderThan(StorageEntity storageEntity, int thresholdMinutes,
        List<String> businessObjectDataStatuses)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntityJoin = businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.storageUnits);

        // Compute threshold timestamp based on the current database timestamp and threshold minutes.
        Timestamp thresholdTimestamp = HerdDateUtils.addMinutes(getCurrentTimestamp(), -thresholdMinutes);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(storageUnitEntityJoin.get(StorageUnitEntity_.storageName), storageEntity.getName()));
        predicates.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.statusCode).in(businessObjectDataStatuses));
        predicates.add(builder.lessThanOrEqualTo(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.createdOn), thresholdTimestamp));

        // Order results by "created on" timestamp.
        Order orderBy = builder.asc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.createdOn));

        // Add all clauses to the query.
        criteria.select(businessObjectDataEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public Map<BusinessObjectDataEntity, StoragePolicyEntity> getBusinessObjectDataEntitiesMatchingStoragePolicies(
        StoragePolicyPriorityLevel storagePolicyPriorityLevel, List<String> supportedBusinessObjectDataStatuses, int storagePolicyTransitionMaxAllowedAttempts,
        int startPosition, int maxResult)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);
        Root<StoragePolicyEntity> storagePolicyEntityRoot = criteria.from(StoragePolicyEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntityJoin = businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityJoin.join(StorageUnitEntity_.status);
        Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> businessObjectDataStatusEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.status);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<StoragePolicyEntity, StoragePolicyTransitionTypeEntity> storagePolicyTransitionTypeEntityJoin =
            storagePolicyEntityRoot.join(StoragePolicyEntity_.storagePolicyTransitionType);
        Join<StoragePolicyEntity, StoragePolicyStatusEntity> storagePolicyStatusEntityJoin = storagePolicyEntityRoot.join(StoragePolicyEntity_.status);

        // Create main query restrictions based on the specified parameters.
        List<Predicate> mainQueryPredicates = new ArrayList<>();

        // Add a restriction on business object definition.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isBusinessObjectDefinitionIsNull() ?
            builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.businessObjectDefinition)) :
            builder.equal(businessObjectDefinitionEntityJoin, storagePolicyEntityRoot.get(StoragePolicyEntity_.businessObjectDefinition)));

        // Add a restriction on business object format usage.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isUsageIsNull() ? builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.usage)) : builder
            .equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
                builder.upper(storagePolicyEntityRoot.get(StoragePolicyEntity_.usage))));

        // Add a restriction on business object format file type.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isFileTypeIsNull() ? builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.fileType)) :
            builder.equal(fileTypeEntityJoin, storagePolicyEntityRoot.get(StoragePolicyEntity_.fileType)));

        // Add a restriction on storage policy filter storage.
        mainQueryPredicates
            .add(builder.equal(storageUnitEntityJoin.get(StorageUnitEntity_.storage), storagePolicyEntityRoot.get(StoragePolicyEntity_.storage)));

        // Add a restriction on storage policy latest version flag.
        mainQueryPredicates.add(builder.isTrue(storagePolicyEntityRoot.get(StoragePolicyEntity_.latestVersion)));

        // Add a restriction on storage policy status.
        mainQueryPredicates.add(builder.equal(storagePolicyStatusEntityJoin.get(StoragePolicyStatusEntity_.code), StoragePolicyStatusEntity.ENABLED));

        // Add a restriction on supported business object data statuses.
        mainQueryPredicates.add(businessObjectDataStatusEntityJoin.get(BusinessObjectDataStatusEntity_.code).in(supportedBusinessObjectDataStatuses));

        // Add a restriction as per storage policy transition type.
        mainQueryPredicates.add(builder
            .and(builder.equal(storagePolicyTransitionTypeEntityJoin.get(StoragePolicyTransitionTypeEntity_.code), StoragePolicyTransitionTypeEntity.GLACIER),
                builder.or(builder.equal(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.code), StorageUnitStatusEntity.ENABLED),
                    builder.equal(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.code), StorageUnitStatusEntity.ARCHIVING))));

        // If specified, add restriction on maximum allowed attempts for a storage policy transition.
        if (storagePolicyTransitionMaxAllowedAttempts > 0)
        {
            mainQueryPredicates.add(builder.or(builder.isNull(storageUnitEntityJoin.get(StorageUnitEntity_.storagePolicyTransitionFailedAttempts)), builder
                .lessThan(storageUnitEntityJoin.get(StorageUnitEntity_.storagePolicyTransitionFailedAttempts), storagePolicyTransitionMaxAllowedAttempts)));
        }

        // Order the results by business object data "created on" value.
        Order orderByCreatedOn = builder.asc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.createdOn));

        // Add the select clause to the main query.
        criteria.multiselect(businessObjectDataEntityRoot, storagePolicyEntityRoot);

        // Add the where clause to the main query.
        criteria.where(mainQueryPredicates.toArray(new Predicate[] {}));

        // Add the order by clause to the main query.
        criteria.orderBy(orderByCreatedOn);

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).setFirstResult(startPosition).setMaxResults(maxResult).getResultList();

        // Populate the result map from the returned tuples (i.e. 1 tuple for each row).
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = new LinkedHashMap<>();
        for (Tuple tuple : tuples)
        {
            // Since multiple storage policies can contain identical filters, we add the below check to select each business object data instance only once.
            if (!result.containsKey(tuple.get(businessObjectDataEntityRoot)))
            {
                result.put(tuple.get(businessObjectDataEntityRoot), tuple.get(storagePolicyEntityRoot));
            }
        }

        return result;
    }

    /**
     * Retrieves partition value per specified parameters that includes the aggregate function.
     * <p>
     * Returns null if the business object format key does not exist.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status will be used for each partition value.
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     * @param storageNames the optional list of storage names (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param aggregateFunction the aggregate function to use against partition values
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the partition value
     */
    private String getBusinessObjectDataPartitionValue(int partitionColumnPosition, final BusinessObjectFormatKey businessObjectFormatKey,
        final Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType, final AggregateFunction aggregateFunction, String upperBoundPartitionValue, String lowerBoundPartitionValue)
    {
        // We cannot use businessObjectFormatKey passed in since it is case-insensitive. Case-insensitive values requires upper() function in the SQL query, and
        // it has caused performance problems. So we need to extract case-sensitive business object format key from database so we can eliminate the upper()
        // function.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        BusinessObjectFormatKey businessObjectFormatKeyCaseSensitive = (BusinessObjectFormatKey) businessObjectFormatKey.clone();
        if (businessObjectFormatEntity == null)
        {
            // Returns null if business object format key does not exist.
            return null;
        }
        else
        {
            // Sets the exact values for business object format key from the database. Note that usage type is still case-insensitive.
            businessObjectFormatKeyCaseSensitive.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectFormatKeyCaseSensitive.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
            businessObjectFormatKeyCaseSensitive.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        }

        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntityJoin = businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityJoin.join(StorageUnitEntity_.status);
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityJoin.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> storagePlatformEntityJoin = storageEntityJoin.join(StorageEntity_.storagePlatform);

        // Create the path.
        Expression<String> partitionValue;
        SingularAttribute<BusinessObjectDataEntity, String> singleValuedAttribute = BUSINESS_OBJECT_DATA_PARTITIONS.get(partitionColumnPosition - 1);
        switch (aggregateFunction)
        {
            case GREATEST:
                partitionValue = builder.greatest(businessObjectDataEntityRoot.get(singleValuedAttribute));
                break;
            case LEAST:
                partitionValue = builder.least(businessObjectDataEntityRoot.get(singleValuedAttribute));
                break;
            default:
                throw new IllegalArgumentException("Invalid aggregate function found: \"" + aggregateFunction + "\".");
        }

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate mainQueryRestriction = builder.equal(namespaceEntityJoin.get(NamespaceEntity_.code), businessObjectFormatKeyCaseSensitive.getNamespace());
        mainQueryRestriction = builder.and(mainQueryRestriction, builder.equal(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name),
            businessObjectFormatKeyCaseSensitive.getBusinessObjectDefinitionName()));
        mainQueryRestriction = builder.and(mainQueryRestriction, builder
            .equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
                businessObjectFormatKeyCaseSensitive.getBusinessObjectFormatUsage().toUpperCase()));
        mainQueryRestriction = builder.and(mainQueryRestriction,
            builder.equal(fileTypeEntityJoin.get(FileTypeEntity_.code), businessObjectFormatKeyCaseSensitive.getBusinessObjectFormatFileType()));

        // If a business object format version was specified, use it.
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction, builder
                .equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
                    businessObjectFormatKey.getBusinessObjectFormatVersion()));
        }

        // If a data version was specified, use it.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        // Business object data version is not specified, so get the latest one as per specified business object data status in the specified storage.
        else
        {
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectDataVersionSubQuery(builder, criteria, businessObjectDataEntityRoot, businessObjectFormatEntityJoin,
                    businessObjectDataStatus, storageNames, storagePlatformType, excludedStoragePlatformType, false);

            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.in(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version)).value(subQuery));
        }

        // Add an inclusive upper bound partition value restriction if specified.
        if (upperBoundPartitionValue != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.lessThanOrEqualTo(businessObjectDataEntityRoot.get(singleValuedAttribute), upperBoundPartitionValue));
        }

        // Add an inclusive lower bound partition value restriction if specified.
        if (lowerBoundPartitionValue != null)
        {
            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.greaterThanOrEqualTo(businessObjectDataEntityRoot.get(singleValuedAttribute), lowerBoundPartitionValue));
        }

        // If specified, add restriction on storage.
        mainQueryRestriction = builder.and(mainQueryRestriction,
            getQueryRestrictionOnStorage(builder, storageEntityJoin, storagePlatformEntityJoin, storageNames, storagePlatformType,
                excludedStoragePlatformType));

        // Search across only "available" storage units.
        mainQueryRestriction = builder.and(mainQueryRestriction, builder.isTrue(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.available)));

        criteria.select(partitionValue).where(mainQueryRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    /**
     * Retrieves a list of business object data entities per specified parameters. This method processes a sublist of partition filters specified by
     * partitionFilterSubListFromIndex and partitionFilterSubListSize parameters.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status is returned.
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified. When
     * business object data version and business object data status both are not specified, the latest data version for each set of partition values will be
     * used regardless of the status.
     * @param storageName the name of the storage where the business object data storage unit is located (case-insensitive)
     * @param partitionFilterSubListFromIndex the index of the first element in the partition filter sublist
     * @param partitionFilterSubListSize the size of the partition filter sublist
     *
     * @return the list of business object data entities sorted by partition values
     */
    private List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectFormatKey businessObjectFormatKey, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, String businessObjectDataStatus, String storageName, int partitionFilterSubListFromIndex,
        int partitionFilterSubListSize)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntity = businessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).

        // Create a standard restriction based on the business object format key values.
        // Please note that we specify not to ignore the business object format version.
        Predicate mainQueryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity, businessObjectFormatKey, false);

        // If a format version was not specified, use the latest available for this set of partition values.
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() == null)
        {
            // Business object format version is not specified, so just use the latest available for this set of partition values.
            Subquery<Integer> subQuery = criteria.subquery(Integer.class);

            // The criteria root is the business object data.
            Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

            // Join to the other tables we can filter on.
            Join<BusinessObjectDataEntity, StorageUnitEntity> subStorageUnitEntity = subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
            Join<StorageUnitEntity, StorageEntity> subStorageEntity = subStorageUnitEntity.join(StorageUnitEntity_.storage);
            Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
                subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
            Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> subBusinessObjectDefinitionEntity =
                subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
            Join<BusinessObjectFormatEntity, FileTypeEntity> subBusinessObjectFormatFileTypeEntity =
                subBusinessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
            Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> subBusinessObjectDataStatusEntity =
                subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.status);

            // Create the standard restrictions (i.e. the standard where clauses).
            Predicate subQueryRestriction = builder.equal(subBusinessObjectDefinitionEntity, businessObjectDefinitionEntity);
            subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage),
                businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)));
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
            subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subStorageEntity, storageEntity));

            subQuery.select(builder.max(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion))).where(subQueryRestriction);

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
                    Collections.singletonList(storageName), null, null, false);

            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.in(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)).value(subQuery));
        }

        // Add a storage name restriction to the main query where clause.
        mainQueryRestriction =
            builder.and(mainQueryRestriction, builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntity).where(mainQueryRestriction);

        // Order by partitions.
        List<Order> orderBy = new ArrayList<>();
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartition : BUSINESS_OBJECT_DATA_PARTITIONS)
        {
            orderBy.add(builder.asc(businessObjectDataEntity.get(businessObjectDataPartition)));
        }
        criteria.orderBy(orderBy);

        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<BusinessObjectDataEntity> getBusinessObjectDataEntitiesByPartitionValue(String partitionValue)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> query = criteriaBuilder.createQuery(BusinessObjectDataEntity.class);

        Root<BusinessObjectDataEntity> businessObjectDataEntity = query.from(BusinessObjectDataEntity.class);
        Predicate partitionValueEquals = criteriaBuilder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), partitionValue);
        query.select(businessObjectDataEntity).where(partitionValueEquals);

        return entityManager.createQuery(query).getResultList();
    }

    /**
     * Create search restrictions.
     *
     * @param builder the criteria builder
     * @param criteria the criteria
     * @param businessObjectDataEntity the root business object data entity
     * @param businessObjectDataSearchKey the business object data search key
     * @param namespaceEntity the namespace entity
     * @param fileTypeEntity the file type entity
     * @param isCountQuery specifies if this is a count query
     *
     * @return the search restrictions
     */
    private Predicate getPredict(CriteriaBuilder builder, CriteriaQuery<?> criteria, Root<BusinessObjectDataEntity> businessObjectDataEntity,
        BusinessObjectDataSearchKey businessObjectDataSearchKey, NamespaceEntity namespaceEntity, FileTypeEntity fileTypeEntity, boolean isCountQuery)
    {
        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        if (!isCountQuery)
        {
            List<Order> orderList = new ArrayList<>();
            orderList.add(builder.asc(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.namespace)));
            orderList.add(builder.asc(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)));
            orderList.add(builder.asc(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)));
            orderList.add(builder.asc(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.fileType)));
            orderList.add(builder.desc(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue2)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue3)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue4)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue5)));
            orderList.add(builder.desc(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)));
            criteria.orderBy(orderList);
        }

        // Create the standard restrictions based on the business object search key values (i.e. the standard where clauses).

        // Create a restriction on namespace code.
        Predicate predicate = builder.equal(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.namespace), namespaceEntity);

        // Create and append a restriction on business object definition name.
        predicate = builder.and(predicate, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataSearchKey.getBusinessObjectDefinitionName().toUpperCase()));

        // Create and append a restriction on business object format usage.
        if (!StringUtils.isEmpty(businessObjectDataSearchKey.getBusinessObjectFormatUsage()))
        {
            predicate = builder.and(predicate, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
                businessObjectDataSearchKey.getBusinessObjectFormatUsage().toUpperCase()));
        }

        // If specified, create and append a restriction on business object format file type.
        if (fileTypeEntity != null)
        {
            predicate = builder.and(predicate, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.fileType), fileTypeEntity));
        }

        // If specified, create and append a restriction on business object format version.
        if (businessObjectDataSearchKey.getBusinessObjectFormatVersion() != null)
        {
            predicate = builder.and(predicate, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
                businessObjectDataSearchKey.getBusinessObjectFormatVersion()));
        }

        predicate = createPartitionValueFilters(businessObjectDataSearchKey, businessObjectDataEntity, businessObjectFormatEntity, builder, predicate);

        List<AttributeValueFilter> attributeValueFilters = businessObjectDataSearchKey.getAttributeValueFilters();

        if (attributeValueFilters != null && !attributeValueFilters.isEmpty())
        {
            predicate = applyAttributeValueFilters(businessObjectDataSearchKey, businessObjectDataEntity, builder, predicate);
        }

        // Apply registration date range filter, if specified.
        if (businessObjectDataSearchKey.getRegistrationDateRangeFilter() != null)
        {
            predicate =
                applyRegistrationDateRangeFilter(businessObjectDataSearchKey.getRegistrationDateRangeFilter(), businessObjectDataEntity, builder, predicate);
        }

        if (BooleanUtils.isTrue(businessObjectDataSearchKey.isFilterOnLatestValidVersion()))
        {
            String validStatus = BusinessObjectDataStatusEntity.VALID;
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectDataVersionSubQuery(builder, criteria, businessObjectDataEntity, businessObjectFormatEntity, validStatus);
            predicate = builder.and(predicate, builder.in(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)).value(subQuery));
        }

        if (BooleanUtils.isTrue(businessObjectDataSearchKey.isFilterOnRetentionExpiration()))
        {
            predicate = applyRetentionExpirationFilter(businessObjectDataSearchKey, businessObjectDataEntity, businessObjectFormatEntity, builder, predicate);
        }

        return predicate;
    }

    @Override
    public Long getBusinessObjectDataCountBySearchKey(BusinessObjectDataSearchKey businessObjectDataSearchKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Create path.
        Expression<Long> businessObjectDataCount = builder.count(businessObjectDataEntityRoot);

        // Namespace is a required parameter, so fetch the relative entity to optimize the main query.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(businessObjectDataSearchKey.getNamespace());

        // If specified namespace does not exist, then return a zero record count.
        if (namespaceEntity == null)
        {
            return 0L;
        }

        // If file type is specified, fetch the relative entity to optimize the main query.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectDataSearchKey.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectDataSearchKey.getBusinessObjectFormatFileType());

            // If specified file type does not exist, then return a zero record count.
            if (fileTypeEntity == null)
            {
                return 0L;
            }
        }

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate;
        try
        {
            predicate = getPredict(builder, criteria, businessObjectDataEntityRoot, businessObjectDataSearchKey, namespaceEntity, fileTypeEntity, true);
        }
        catch (IllegalArgumentException ex)
        {
            // This exception means that there are no records found for the query, thus return 0 record count.
            return 0L;
        }

        // Add all clauses for the query.
        criteria.select(businessObjectDataCount).where(predicate).distinct(true);

        // Execute the query and return the result.
        return entityManager.createQuery(criteria).getSingleResult();
    }

    @Override
    public List<BusinessObjectData> searchBusinessObjectData(BusinessObjectDataSearchKey businessObjectDataSearchKey, Integer pageNum, Integer pageSize)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Namespace is a required parameter, so fetch the relative entity to optimize the main query.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(businessObjectDataSearchKey.getNamespace());

        // If specified namespace does not exist, then return an empty result list.
        if (namespaceEntity == null)
        {
            return Collections.emptyList();
        }

        // If file type is specified, fetch the relative entity to optimize the main query.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectDataSearchKey.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectDataSearchKey.getBusinessObjectFormatFileType());

            // If specified file type does not exist, then return an empty result list.
            if (fileTypeEntity == null)
            {
                return Collections.emptyList();
            }
        }

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate;
        try
        {
            predicate = getPredict(builder, criteria, businessObjectDataEntityRoot, businessObjectDataSearchKey, namespaceEntity, fileTypeEntity, false);
        }
        catch (IllegalArgumentException ex)
        {
            // This exception means that there are no records found for the query, thus return an empty result list.
            return Collections.emptyList();
        }

        // Add all clauses for the query.
        criteria.select(businessObjectDataEntityRoot).where(predicate);

        // Execute the query.
        List<BusinessObjectDataEntity> entityArray =
            entityManager.createQuery(criteria).setFirstResult(pageSize * (pageNum - 1)).setMaxResults(pageSize).getResultList();

        // Crete the result list of business object data.
        return getQueryResultListFromEntityList(entityArray, businessObjectDataSearchKey.getAttributeValueFilters());
    }

    /**
     * Creates a predicate for partition value filters.
     *
     * @param businessDataSearchKey businessDataSearchKey
     * @param businessObjectDataEntity businessObjectDataEntity
     * @param businessObjectFormatEntity businessObjectFormatEntity
     * @param builder builder
     * @param predicatePram predicate parameter
     *
     * @return the predicate
     */
    private Predicate createPartitionValueFilters(BusinessObjectDataSearchKey businessDataSearchKey, Root<BusinessObjectDataEntity> businessObjectDataEntity,
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity, CriteriaBuilder builder, Predicate predicatePram)
    {
        Predicate predicate = predicatePram;

        if (businessDataSearchKey.getPartitionValueFilters() != null && !businessDataSearchKey.getPartitionValueFilters().isEmpty())
        {
            for (PartitionValueFilter partitionFilter : businessDataSearchKey.getPartitionValueFilters())
            {
                Join<BusinessObjectFormatEntity, SchemaColumnEntity> schemaEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.schemaColumns);

                List<String> partitionValues = partitionFilter.getPartitionValues();

                predicate = builder
                    .and(predicate, builder.equal(builder.upper(schemaEntity.get(SchemaColumnEntity_.name)), partitionFilter.getPartitionKey().toUpperCase()));
                predicate = builder.and(predicate, builder.isNotNull(schemaEntity.get(SchemaColumnEntity_.partitionLevel)));

                if (partitionValues != null && !partitionValues.isEmpty())
                {
                    predicate = builder.and(predicate, builder.or(builder.and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 1),
                        businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue).in(partitionValues)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 2),
                            businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue2).in(partitionValues)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 3),
                            businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue3).in(partitionValues)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 4),
                            businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue4).in(partitionValues)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 5),
                            businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue5).in(partitionValues))));
                }
                else if (partitionFilter.getPartitionValueRange() != null)
                {
                    PartitionValueRange partitionRange = partitionFilter.getPartitionValueRange();
                    String startPartitionValue = partitionRange.getStartPartitionValue();
                    String endPartitionValue = partitionRange.getEndPartitionValue();

                    predicate = builder.and(predicate, builder.or(builder.and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 1),
                        builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), startPartitionValue),
                        builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), endPartitionValue)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 2),
                            builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue2), startPartitionValue),
                            builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue2), endPartitionValue)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 3),
                            builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue3), startPartitionValue),
                            builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue3), endPartitionValue)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 4),
                            builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue4), startPartitionValue),
                            builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue4), endPartitionValue)), builder
                        .and(builder.equal(schemaEntity.get(SchemaColumnEntity_.partitionLevel), 5),
                            builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue5), startPartitionValue),
                            builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue5), endPartitionValue))));
                }
            }
        }

        return predicate;
    }

    /**
     * Apply retention expiration filter to the main query predicate.
     *
     * @param businessObjectDataSearchKey the business object data search key
     * @param businessObjectDataEntityRoot the criteria root which is a business object data entity
     * @param businessObjectFormatEntityJoin the join with the business object format table
     * @param builder the criteria builder
     * @param mainQueryPredicate the main query predicate to be updated
     *
     * @return the updated main query predicate
     */
    private Predicate applyRetentionExpirationFilter(BusinessObjectDataSearchKey businessObjectDataSearchKey,
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot, Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin,
        CriteriaBuilder builder, Predicate mainQueryPredicate)
    {
        // Create a business object definition key per specified search key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(businessObjectDataSearchKey.getNamespace(), businessObjectDataSearchKey.getBusinessObjectDefinitionName());

        // Get latest versions of all business object formats that registered with the business object definition.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities =
            businessObjectFormatDao.getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(businessObjectDefinitionKey);

        // Create a result predicate to join all retention expiration predicates created per selected business object formats.
        Predicate businessObjectDefinitionRetentionExpirationPredicate = null;

        // Get the current database timestamp to be used to select expired business object data per BDATA_RETENTION_DATE retention type.
        Timestamp currentTimestamp = getCurrentTimestamp();

        // Create a predicate for each business object format with the retention information.
        for (BusinessObjectFormatEntity businessObjectFormatEntity : businessObjectFormatEntities)
        {
            if (businessObjectFormatEntity.getRetentionType() != null)
            {
                // Create a retention expiration predicate for this business object format.
                Predicate businessObjectFormatRetentionExpirationPredicate = null;

                if (StringUtils.equals(businessObjectFormatEntity.getRetentionType().getCode(), RetentionTypeEntity.BDATA_RETENTION_DATE))
                {
                    // Select business object data that has expired per its explicitly configured retention expiration date.
                    businessObjectFormatRetentionExpirationPredicate =
                        builder.lessThan(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.retentionExpiration), currentTimestamp);
                }
                else if (StringUtils.equals(businessObjectFormatEntity.getRetentionType().getCode(), RetentionTypeEntity.PARTITION_VALUE) &&
                    businessObjectFormatEntity.getRetentionPeriodInDays() != null)
                {
                    // Compute the retention expiration date and convert it to the date format to match against partition values.
                    String retentionExpirationDate = DateFormatUtils
                        .format(DateUtils.addDays(new Date(), -1 * businessObjectFormatEntity.getRetentionPeriodInDays()), DEFAULT_SINGLE_DAY_DATE_MASK);

                    // Create a predicate to compare business object data primary partition value against the retention expiration date.
                    businessObjectFormatRetentionExpirationPredicate =
                        builder.lessThan(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue), retentionExpirationDate);
                }

                // If it was initialize, complete processing of retention expiration predicate for this business object format.
                if (businessObjectFormatRetentionExpirationPredicate != null)
                {
                    // Update the predicate to match this business object format w/o version.
                    businessObjectFormatRetentionExpirationPredicate = builder.and(businessObjectFormatRetentionExpirationPredicate, builder
                        .equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
                            businessObjectFormatEntity.getUsage().toUpperCase()));
                    businessObjectFormatRetentionExpirationPredicate = builder.and(businessObjectFormatRetentionExpirationPredicate,
                        builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileType), businessObjectFormatEntity.getFileType()));

                    // Add this business object format specific retention expiration predicate to other
                    // retention expiration predicates created for the specified business object definition.
                    if (businessObjectDefinitionRetentionExpirationPredicate == null)
                    {
                        businessObjectDefinitionRetentionExpirationPredicate = businessObjectFormatRetentionExpirationPredicate;
                    }
                    else
                    {
                        businessObjectDefinitionRetentionExpirationPredicate =
                            builder.or(businessObjectDefinitionRetentionExpirationPredicate, businessObjectFormatRetentionExpirationPredicate);
                    }
                }
            }
        }

        // Fail if no retention expiration predicates got created per specified business objject definition.
        Assert.notNull(businessObjectDefinitionRetentionExpirationPredicate, String
            .format("Business object definition with name \"%s\" and namespace \"%s\" has no business object formats with supported retention type.",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));

        // Add created business object definition retention expiration predicate to the main query predicate passed to this method and return the result.
        return builder.and(mainQueryPredicate, businessObjectDefinitionRetentionExpirationPredicate);
    }

    /**
     * Apply attribute value filters to the main query predicate.
     *
     * @param businessDataSearchKey the business object data search key
     * @param businessObjectDataEntityRoot the criteria root which is a business object data entity
     * @param builder the criteria builder
     * @param mainQueryPredicate the main query predicate to be updated
     *
     * @return the updated main query predicate
     */
    private Predicate applyAttributeValueFilters(final BusinessObjectDataSearchKey businessDataSearchKey,
        final Root<BusinessObjectDataEntity> businessObjectDataEntityRoot, final CriteriaBuilder builder, Predicate mainQueryPredicate)
    {
        if (!CollectionUtils.isEmpty(businessDataSearchKey.getAttributeValueFilters()))
        {
            for (AttributeValueFilter attributeValueFilter : businessDataSearchKey.getAttributeValueFilters())
            {
                Join<BusinessObjectDataEntity, BusinessObjectDataAttributeEntity> dataAttributeEntity =
                    businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.attributes);

                if (!StringUtils.isEmpty(attributeValueFilter.getAttributeName()))
                {
                    mainQueryPredicate = builder.and(mainQueryPredicate,
                        builder.equal(dataAttributeEntity.get(BusinessObjectDataAttributeEntity_.name), attributeValueFilter.getAttributeName()));
                }

                if (!StringUtils.isEmpty(attributeValueFilter.getAttributeValue()))
                {
                    mainQueryPredicate = builder.and(mainQueryPredicate,
                        builder.equal(dataAttributeEntity.get(BusinessObjectDataAttributeEntity_.value), attributeValueFilter.getAttributeValue()));
                }
            }
        }

        return mainQueryPredicate;
    }

    /**
     * Apply a predicate for registration date range filter.
     *
     * @param registrationDateRangeFilter the registration date range filter, not null
     * @param businessObjectDataEntity the business object data entity
     * @param builder the query builder
     * @param predicate the predicate to be updated
     *
     * @return the predicate with added registration date range filter
     */
    private Predicate applyRegistrationDateRangeFilter(RegistrationDateRangeFilter registrationDateRangeFilter,
        Root<BusinessObjectDataEntity> businessObjectDataEntity, CriteriaBuilder builder, Predicate predicate)
    {
        // Apply predicate for registration start date and removed the time portion of the date.
        if (registrationDateRangeFilter.getStartRegistrationDate() != null)
        {
            predicate = builder.and(predicate, builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn),
                HerdDateUtils.resetTimeToMidnight(registrationDateRangeFilter.getStartRegistrationDate())));
        }

        // Apply predicate for registration end date. Removed time portion of the date and added one day to get the result till the end of the day
        if (registrationDateRangeFilter.getEndRegistrationDate() != null)
        {
            predicate = builder.and(predicate, builder.lessThan(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn),
                HerdDateUtils.addDays(HerdDateUtils.resetTimeToMidnight(registrationDateRangeFilter.getEndRegistrationDate()), 1)));
        }

        return predicate;
    }

    /**
     * Gets a query result list from an entity list.
     *
     * @param entityArray entity array from query
     * @param attributeValueList attribute value list
     *
     * @return the list of business object data
     */
    private List<BusinessObjectData> getQueryResultListFromEntityList(List<BusinessObjectDataEntity> entityArray, List<AttributeValueFilter> attributeValueList)
    {
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();
        Set<Integer> businessObjectIdSet = new HashSet<>();
        for (BusinessObjectDataEntity dataEntity : entityArray)
        {
            //need to skip the same data entity
            if (businessObjectIdSet.contains(dataEntity.getId()))
            {
                continue;
            }
            BusinessObjectData businessObjectData = new BusinessObjectData();
            businessObjectIdSet.add(dataEntity.getId());
            businessObjectData.setId(dataEntity.getId());
            businessObjectData.setPartitionValue(dataEntity.getPartitionValue());
            businessObjectData.setVersion(dataEntity.getVersion());
            businessObjectData.setLatestVersion(dataEntity.getLatestVersion());
            BusinessObjectFormatEntity formatEntity = dataEntity.getBusinessObjectFormat();
            businessObjectData.setNamespace(formatEntity.getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectData.setBusinessObjectDefinitionName(formatEntity.getBusinessObjectDefinition().getName());
            businessObjectData.setBusinessObjectFormatUsage(formatEntity.getUsage());
            businessObjectData.setBusinessObjectFormatFileType(formatEntity.getFileType().getCode());
            businessObjectData.setBusinessObjectFormatVersion(formatEntity.getBusinessObjectFormatVersion());
            businessObjectData.setPartitionKey(formatEntity.getPartitionKey());
            businessObjectData.setStatus(dataEntity.getStatus().getCode());

            List<String> subpartitions = new ArrayList<>();
            if (dataEntity.getPartitionValue2() != null)
            {
                subpartitions.add(dataEntity.getPartitionValue2());
            }
            if (dataEntity.getPartitionValue3() != null)
            {
                subpartitions.add(dataEntity.getPartitionValue3());
            }
            if (dataEntity.getPartitionValue4() != null)
            {
                subpartitions.add(dataEntity.getPartitionValue4());
            }
            if (dataEntity.getPartitionValue5() != null)
            {
                subpartitions.add(dataEntity.getPartitionValue5());
            }
            if (subpartitions.size() > 0)
            {
                businessObjectData.setSubPartitionValues(subpartitions);
            }

            //add attribute name and values in the request to the response
            if (attributeValueList != null && !attributeValueList.isEmpty())
            {
                Collection<BusinessObjectDataAttributeEntity> dataAttributeCollection = dataEntity.getAttributes();
                List<Attribute> attributeList = new ArrayList<>();
                for (BusinessObjectDataAttributeEntity attributeEntity : dataAttributeCollection)
                {
                    Attribute attribute = new Attribute(attributeEntity.getName(), attributeEntity.getValue());
                    if (shouldIncludeAttributeInResponse(attributeEntity, attributeValueList) && !attributeList.contains(attribute))
                    {
                        attributeList.add(attribute);
                    }
                }
                businessObjectData.setAttributes(attributeList);
            }
            businessObjectDataList.add(businessObjectData);
        }

        return businessObjectDataList;
    }

    /**
     * Checks if the attribute should be returned based on the attribute value query list if attribute name supplied, match attribute name case in sensitive if
     * attribute value supplied, match attribute value case sensitive with contain logic if both attribute name and value supplied, match both.
     *
     * @param attributeEntity the database attribute entity
     * @param attributeQueryList the attribute query list
     *
     * @return true for returning in response; false for not
     */
    private boolean shouldIncludeAttributeInResponse(BusinessObjectDataAttributeEntity attributeEntity, List<AttributeValueFilter> attributeQueryList)
    {
        String attributeName = attributeEntity.getName();
        String attributeValue = attributeEntity.getValue();

        for (AttributeValueFilter valueFiler : attributeQueryList)
        {
            String queryAttributeName = valueFiler.getAttributeName();
            String queryAttributeValue = valueFiler.getAttributeValue();
            Boolean matchAttributeName = false;
            Boolean matchAttributeValue = false;

            if (attributeName != null && attributeName.equalsIgnoreCase(queryAttributeName))
            {
                matchAttributeName = true;
            }

            if (attributeValue != null && queryAttributeValue != null && attributeValue.contains(queryAttributeValue))
            {
                matchAttributeValue = true;
            }

            if (!StringUtils.isEmpty(queryAttributeName) && !StringUtils.isEmpty(queryAttributeValue))
            {
                if (matchAttributeName && matchAttributeValue)
                {
                    return true;
                }
            }
            else if (!StringUtils.isEmpty(queryAttributeName) && matchAttributeName)
            {
                return true;
            }
            else if (!StringUtils.isEmpty(queryAttributeValue) && matchAttributeValue)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates a sub query for the maximum business object data version.
     *
     * @param builder criteria builder
     * @param criteria criteria query
     * @param businessObjectDataEntity business object data entity
     * @param businessObjectFormatEntity business object format entity
     * @param businessObjectDataStatus business object status, case insensitive
     *
     * @return the sub query to select the maximum business object data version
     */
    private Subquery<Integer> getMaximumBusinessObjectDataVersionSubQuery(CriteriaBuilder builder, CriteriaQuery<?> criteria,
        From<?, BusinessObjectDataEntity> businessObjectDataEntity, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        String businessObjectDataStatus)
    {
        // Create a sub query for the business object data version.
        Subquery<Integer> subQuery = criteria.subquery(Integer.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
            subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);

        // Add a standard restriction on business object format.
        Predicate subQueryRestriction = builder.equal(subBusinessObjectFormatEntity, businessObjectFormatEntity);

        // Create and add standard restrictions on primary and sub-partition values.
        subQueryRestriction =
            builder.and(subQueryRestriction, getQueryRestrictionOnPartitionValues(builder, subBusinessObjectDataEntity, businessObjectDataEntity));

        // If specified, create and add a standard restriction on business object data status.
        if (businessObjectDataStatus != null)
        {
            Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> subBusinessObjectDataStatusEntity =
                subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.status);

            subQueryRestriction = builder.and(subQueryRestriction, builder
                .equal(builder.upper(subBusinessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code)), businessObjectDataStatus.toUpperCase()));
        }

        // Add all clauses to the sub query.
        subQuery.select(builder.max(subBusinessObjectDataEntity.get(BusinessObjectDataEntity_.version))).where(subQueryRestriction);

        return subQuery;
    }

    @Override
    public List<BusinessObjectDataKey> getBusinessObjectDataByBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        Integer maxResults)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Join to other tables that we can order by on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate =
            builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinition), businessObjectDefinitionEntity);

        // Build the order by clause. The sort order is consistent with the search business object data implementation.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)));
        orderBy.add(builder.asc(fileTypeEntityJoin.get(FileTypeEntity_.code)));
        orderBy.add(builder.desc(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)));
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartition : BUSINESS_OBJECT_DATA_PARTITIONS)
        {
            orderBy.add(builder.desc(businessObjectDataEntityRoot.get(businessObjectDataPartition)));
        }
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version)));

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntityRoot).where(predicate).orderBy(orderBy);

        // Create a query.
        TypedQuery<BusinessObjectDataEntity> query = entityManager.createQuery(criteria);

        // If specified, set the maximum number of results for query to return.
        if (maxResults != null)
        {
            query.setMaxResults(maxResults);
        }

        // Run the query to get a list of entities back.
        List<BusinessObjectDataEntity> businessObjectDataEntities = query.getResultList();

        // Populate the "keys" objects from the returned entities.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
            businessObjectDataKeys.add(businessObjectDataKey);
            businessObjectDataKey.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
            businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
            businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
            businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
            businessObjectDataKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
            businessObjectDataKey.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
            businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        }

        return businessObjectDataKeys;
    }

    @Override
    public List<BusinessObjectDataKey> getBusinessObjectDataByBusinessObjectFormat(BusinessObjectFormatEntity businessObjectFormatEntity, Integer maxResults)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.businessObjectFormat), businessObjectFormatEntity);

        // Build the order by clause. The sort order is consistent with the search business object data implementation.
        List<Order> orderBy = new ArrayList<>();
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartition : BUSINESS_OBJECT_DATA_PARTITIONS)
        {
            orderBy.add(builder.desc(businessObjectDataEntityRoot.get(businessObjectDataPartition)));
        }
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version)));

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntityRoot).where(predicate).orderBy(orderBy);

        // Create a query.
        TypedQuery<BusinessObjectDataEntity> query = entityManager.createQuery(criteria);

        // If specified, set the maximum number of results for query to return.
        if (maxResults != null)
        {
            query.setMaxResults(maxResults);
        }

        // Run the query to get a list of entities back.
        List<BusinessObjectDataEntity> businessObjectDataEntities = query.getResultList();

        // Populate the "keys" objects from the returned entities.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
            businessObjectDataKeys.add(businessObjectDataKey);
            businessObjectDataKey.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
            businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
            businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
            businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
            businessObjectDataKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
            businessObjectDataKey.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
            businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        }

        return businessObjectDataKeys;
    }

    /**
     * Gets the sub-partition values for the specified business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the list of sub-partition values
     */
    private List<String> getSubPartitionValues(BusinessObjectDataEntity businessObjectDataEntity)
    {
        List<String> subPartitionValues = new ArrayList<>();

        List<String> rawSubPartitionValues = new ArrayList<>();
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue2());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue3());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue4());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue5());

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
