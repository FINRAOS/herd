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
import java.util.Objects;
import java.util.Set;

import javax.persistence.Tuple;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.SingularAttribute;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.FileTypeDao;
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
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity_;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitEntity_;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity_;

@Repository
public class BusinessObjectDataDaoImpl extends AbstractHerdDao implements BusinessObjectDataDao
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Override
    public BusinessObjectDataEntity getBusinessObjectDataByAltKey(BusinessObjectDataKey businessObjectDataKey)
    {
        return getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, null);
    }

    @Override
    public BusinessObjectDataEntity getBusinessObjectDataByAltKeyAndStatus(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        // Get business object format for the specified business object data. If business object format version is not specified, we should get the latest
        // business object format version. The business object format entity is needed in order to eliminate unnecessary table joins and upper() method calls
        // on case-insensitive elements of the business object data alternate key (except for business object format usage).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion()));

        // Return null if specified business object format does not exist.
        if (businessObjectFormatEntity == null)
        {
            return null;
        }

        // Get the relative entities from the business object format entity that are parts of the business object data alternate key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectFormatEntity.getBusinessObjectDefinition();
        FileTypeEntity fileTypeEntity = businessObjectFormatEntity.getFileType();

        // Get business object format usage (case-insensitive) along with other remaining parts of the business object data alternate key.
        String businessObjectFormatUsage = businessObjectDataKey.getBusinessObjectFormatUsage();
        Integer businessObjectFormatVersion = businessObjectDataKey.getBusinessObjectFormatVersion();
        String primaryPartitionValue = businessObjectDataKey.getPartitionValue();
        List<String> subPartitionValues = businessObjectDataKey.getSubPartitionValues();
        Integer businessObjectDataVersion = businessObjectDataKey.getBusinessObjectDataVersion();

        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);

        // Create restriction on business object definition.
        Predicate queryRestriction =
            builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinitionId), businessObjectDefinitionEntity.getId());

        // Create and append restriction on business object format usage. Use upper() method since business object format usage value is case-insensitive.
        queryRestriction = builder.and(queryRestriction,
            builder.equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)), businessObjectFormatUsage.toUpperCase()));

        // Create and append restriction on business object format file type.
        queryRestriction = builder
            .and(queryRestriction, builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileTypeCode), fileTypeEntity.getCode()));

        // If specified, create and append restriction on business object format version.
        if (businessObjectFormatVersion != null)
        {
            queryRestriction = builder.and(queryRestriction,
                builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion), businessObjectFormatVersion));
        }

        // Create and append restriction on partition values.
        queryRestriction = builder
            .and(queryRestriction, getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntityRoot, primaryPartitionValue, subPartitionValues));

        // If specified, add restriction on business object data version.
        if (businessObjectDataVersion != null)
        {
            queryRestriction =
                builder.and(queryRestriction, builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        // Otherwise, add restriction on business object data status, if specified.
        else if (businessObjectDataStatusEntity != null)
        {
            queryRestriction = builder.and(queryRestriction,
                builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.statusCode), businessObjectDataStatusEntity.getCode()));
        }

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntityRoot).where(queryRestriction);

        // Run the query to get a list of business object data entities.
        List<BusinessObjectDataEntity> businessObjectDataEntities = entityManager.createQuery(criteria).getResultList();

        // Initialize the result to null.
        BusinessObjectDataEntity resultBusinessObjectDataEntity = null;

        // If only one business object data entity is selected, we will return it as a result.
        // If we got multiple entries selected, return one with the latest business object format and business object data versions.
        // When both business object format and business object data versions are specified, there should be at most one entity selected,
        // so the logic below serves two purposes, selects entity with the latest versions and fails if it detects duplicate entities.
        if (CollectionUtils.isNotEmpty(businessObjectDataEntities))
        {
            // Process the list of selected business object data entities to select business object data entity with the latest versions.
            for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
            {
                // Initialize the result to the first selected business object data entity. This covers the scenario when only one entry is selected.
                if (resultBusinessObjectDataEntity == null)
                {
                    resultBusinessObjectDataEntity = businessObjectDataEntities.get(0);
                }
                // Compare this business object data entity against the entity currently selected as the result entity.
                else
                {
                    // If this business object data entity has business object format version which is greater
                    // than the current result entity's version, select this entity as the result entity.
                    if (businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion() >
                        resultBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion())
                    {
                        resultBusinessObjectDataEntity = businessObjectDataEntity;
                    }
                    // Otherwise, if this entity has business object format version which is equal
                    // to the current result entity's, then check business object data versions.
                    else if (Objects.equals(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion(),
                        resultBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion()))
                    {
                        // If this business object data entity has business object data version which is greater
                        // than the current result entity's version, select this entity as the result entity.
                        if (businessObjectDataEntity.getVersion() > resultBusinessObjectDataEntity.getVersion())
                        {
                            resultBusinessObjectDataEntity = businessObjectDataEntity;
                        }
                        // If this entity has business object data version which is equal to the current result entity's version,
                        // then we found duplicate entities, so throw an exception.
                        else if (Objects.equals(businessObjectDataEntity.getVersion(), resultBusinessObjectDataEntity.getVersion()))
                        {
                            throw new IllegalArgumentException(String.format(
                                "Found more than one business object data instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                                    " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\"," +
                                    " businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", businessObjectDataVersion=\"%d\"," +
                                    " businessObjectDataStatus=\"%s\"}.", businessObjectDataKey.getNamespace(),
                                businessObjectDataKey.getBusinessObjectDefinitionName(), businessObjectDataKey.getBusinessObjectFormatUsage(),
                                businessObjectDataKey.getBusinessObjectFormatFileType(), businessObjectDataKey.getBusinessObjectFormatVersion(),
                                businessObjectDataKey.getPartitionValue(), CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" :
                                    StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                                businessObjectDataStatusEntity != null ? businessObjectDataStatusEntity.getCode() : "null"));
                        }
                    }
                }
            }
        }

        // Return the result business object data entity.
        return resultBusinessObjectDataEntity;
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
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity, String upperBoundPartitionValue,
        String lowerBoundPartitionValue)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatusEntity,
            storageEntities, storagePlatformEntity, excludedStoragePlatformEntity, AggregateFunction.GREATEST, upperBoundPartitionValue,
            lowerBoundPartitionValue);
    }

    @Override
    public String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatusEntity,
            storageEntities, storagePlatformEntity, excludedStoragePlatformEntity, AggregateFunction.LEAST, null, null);
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
        StoragePolicyPriorityLevel storagePolicyPriorityLevel, Boolean doNotTransitionLatestValid, List<String> supportedBusinessObjectDataStatuses,
        int storagePolicyTransitionMaxAllowedAttempts, int startPosition, int maxResult)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data along with the storage policy.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);
        Root<StoragePolicyEntity> storagePolicyEntityRoot = criteria.from(StoragePolicyEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntityJoin = businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.storageUnits);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);

        // Create main query restrictions based on the specified parameters.
        List<Predicate> predicates = new ArrayList<>();

        // Add restriction on business object definition.
        predicates.add(storagePolicyPriorityLevel.isBusinessObjectDefinitionIsNull() ?
            builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.businessObjectDefinitionId)) : builder
            .equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinitionId),
                storagePolicyEntityRoot.get(StoragePolicyEntity_.businessObjectDefinitionId)));

        // Add restriction on business object format usage.
        predicates.add(storagePolicyPriorityLevel.isUsageIsNull() ? builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.usage)) : builder
            .equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
                builder.upper(storagePolicyEntityRoot.get(StoragePolicyEntity_.usage))));

        // Add restriction on business object format file type.
        predicates.add(storagePolicyPriorityLevel.isFileTypeIsNull() ? builder.isNull(storagePolicyEntityRoot.get(StoragePolicyEntity_.fileType)) : builder
            .equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileTypeCode),
                storagePolicyEntityRoot.get(StoragePolicyEntity_.fileTypeCode)));

        // Add restriction on storage policy filter storage.
        predicates.add(builder.equal(storageUnitEntityJoin.get(StorageUnitEntity_.storageName), storagePolicyEntityRoot.get(StoragePolicyEntity_.storageName)));

        // Add restriction on storage policy allowing or not to transition latest valid business object data versions.
        predicates.add(builder.equal(storagePolicyEntityRoot.get(StoragePolicyEntity_.doNotTransitionLatestValid), doNotTransitionLatestValid));

        // Add restriction on storage policy latest version flag.
        predicates.add(builder.isTrue(storagePolicyEntityRoot.get(StoragePolicyEntity_.latestVersion)));

        // Add restriction on storage policy status.
        predicates.add(builder.equal(storagePolicyEntityRoot.get(StoragePolicyEntity_.statusCode), StoragePolicyStatusEntity.ENABLED));

        // Add restriction on supported business object data statuses.
        predicates.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.statusCode).in(supportedBusinessObjectDataStatuses));

        // Add restrictions as per storage policy transition type.
        predicates
            .add(builder.equal(storagePolicyEntityRoot.get(StoragePolicyEntity_.storagePolicyTransitionTypeCode), StoragePolicyTransitionTypeEntity.GLACIER));
        predicates.add(storageUnitEntityJoin.get(StorageUnitEntity_.statusCode)
            .in(Lists.newArrayList(StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.ARCHIVING)));

        // If specified, add restriction on maximum allowed attempts for a storage policy transition.
        if (storagePolicyTransitionMaxAllowedAttempts > 0)
        {
            predicates.add(builder.or(builder.isNull(storageUnitEntityJoin.get(StorageUnitEntity_.storagePolicyTransitionFailedAttempts)), builder
                .lessThan(storageUnitEntityJoin.get(StorageUnitEntity_.storagePolicyTransitionFailedAttempts), storagePolicyTransitionMaxAllowedAttempts)));
        }

        // Order the results by business object data "created on" value.
        Order orderByCreatedOn = builder.asc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.createdOn));

        // Add the select clause to the main query.
        criteria.multiselect(businessObjectDataEntityRoot, storagePolicyEntityRoot);

        // Add the where clause to the main query.
        criteria.where(predicates.toArray(new Predicate[] {}));

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
     * Returns null if specified business object format does not exist.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If business object format version isn't specified, business object data
     * partition value is selected across all business object format versions
     * @param businessObjectDataVersion the optional business object data version. If business object data version isn't specified, business object data
     * partition value is selected from all available business object data as per specified business object data status entity
     * @param businessObjectDataStatusEntity the optional business object data status entity. This parameter is ignored when the business object data version is
     * specified
     * @param storageEntities the optional list of storage entities
     * @param storagePlatformEntity the optional storage platform entity, e.g. S3 for Hive DDL. It is ignored when the list of storage entities is not empty
     * @param excludedStoragePlatformEntity the optional storage platform entity to be excluded from search. It is ignored when the list of storage entities is
     * not empty or the storage platform entity is specified
     * @param aggregateFunction the aggregate function to use against partition values
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the partition value
     */
    private String getBusinessObjectDataPartitionValue(int partitionColumnPosition, final BusinessObjectFormatKey businessObjectFormatKey,
        final Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity, final AggregateFunction aggregateFunction,
        String upperBoundPartitionValue, String lowerBoundPartitionValue)
    {
        // Get business object format for the specified business object data. If business object format version is not specified, we should get the latest
        // business object format version. The business object format entity is needed in order to eliminate unnecessary table joins and upper() method calls
        // on case-insensitive elements of the business object data alternate key (except for business object format usage).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        // Return null if specified business object format does not exist.
        if (businessObjectFormatEntity == null)
        {
            return null;
        }

        // Get the relative entities from the business object format entity that are parts of the business object data alternate key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectFormatEntity.getBusinessObjectDefinition();
        FileTypeEntity fileTypeEntity = businessObjectFormatEntity.getFileType();

        // Get the business object format usage (case-insensitive) and optional business object format version from the business object format key.
        String businessObjectFormatUsage = businessObjectFormatKey.getBusinessObjectFormatUsage();
        Integer businessObjectFormatVersion = businessObjectFormatKey.getBusinessObjectFormatVersion();

        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntityJoin = businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageUnitStatusEntity> storageUnitStatusEntityJoin = storageUnitEntityJoin.join(StorageUnitEntity_.status);
        Join<StorageUnitEntity, StorageEntity> storageEntityJoin = storageUnitEntityJoin.join(StorageUnitEntity_.storage);

        // Create the path.
        Expression<String> partitionValueExpression;
        SingularAttribute<BusinessObjectDataEntity, String> partitionValueSingularAttribute = BUSINESS_OBJECT_DATA_PARTITIONS.get(partitionColumnPosition - 1);
        switch (aggregateFunction)
        {
            case GREATEST:
                partitionValueExpression = builder.greatest(businessObjectDataEntityRoot.get(partitionValueSingularAttribute));
                break;
            case LEAST:
                partitionValueExpression = builder.least(businessObjectDataEntityRoot.get(partitionValueSingularAttribute));
                break;
            default:
                throw new IllegalArgumentException("Invalid aggregate function found: \"" + aggregateFunction + "\".");
        }

        // Create standard restriction based on the business object format alternate key values including business object format version, if it is specified.
        Predicate mainQueryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntityJoin, businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity,
                businessObjectFormatVersion);

        // If specified, add restriction on business object data version.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction = builder
                .and(mainQueryRestriction, builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        // Otherwise, add restriction on business object data status, if specified.
        else if (businessObjectDataStatusEntity != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction,
                builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.statusCode), businessObjectDataStatusEntity.getCode()));
        }

        // If specified, add an inclusive upper bound partition value restriction.
        if (upperBoundPartitionValue != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction,
                builder.lessThanOrEqualTo(businessObjectDataEntityRoot.get(partitionValueSingularAttribute), upperBoundPartitionValue));
        }

        // If specified, add an inclusive lower bound partition value restriction.
        if (lowerBoundPartitionValue != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction,
                builder.greaterThanOrEqualTo(businessObjectDataEntityRoot.get(partitionValueSingularAttribute), lowerBoundPartitionValue));
        }

        // If specified, add restriction on storage.
        mainQueryRestriction = builder.and(mainQueryRestriction,
            getQueryRestrictionOnStorage(builder, storageEntityJoin, storageEntities, storagePlatformEntity, excludedStoragePlatformEntity));

        // Select across only "available" storage units.
        mainQueryRestriction = builder.and(mainQueryRestriction, builder.isTrue(storageUnitStatusEntityJoin.get(StorageUnitStatusEntity_.available)));

        // Add all clauses to the query.
        criteria.select(partitionValueExpression).where(mainQueryRestriction);

        // Run the query and return the selected partition value.
        return entityManager.createQuery(criteria).getSingleResult();
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
     * Create search restrictions per specified business object data search key.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntityRoot the root business object data entity
     * @param businessObjectFormatEntityJoin the join with the business object format table
     * @param businessObjectDataSearchKey the business object data search key
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param fileTypeEntity the file type entity
     *
     * @return the search restrictions
     */
    private Predicate getQueryPredicateBySearchKey(CriteriaBuilder builder, Root<BusinessObjectDataEntity> businessObjectDataEntityRoot,
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin, BusinessObjectDataSearchKey businessObjectDataSearchKey,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity)
    {
        // Create restriction on business object definition.
        Predicate predicate =
            builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinitionId), businessObjectDefinitionEntity.getId());

        // If specified, add restriction on business object format usage.
        if (!StringUtils.isEmpty(businessObjectDataSearchKey.getBusinessObjectFormatUsage()))
        {
            predicate = builder.and(predicate, builder.equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
                businessObjectDataSearchKey.getBusinessObjectFormatUsage().toUpperCase()));
        }

        // If specified, add restriction on business object format file type.
        if (fileTypeEntity != null)
        {
            predicate =
                builder.and(predicate, builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileTypeCode), fileTypeEntity.getCode()));
        }

        // If specified, add restriction on business object format version.
        if (businessObjectDataSearchKey.getBusinessObjectFormatVersion() != null)
        {
            predicate = builder.and(predicate, builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
                businessObjectDataSearchKey.getBusinessObjectFormatVersion()));
        }

        // If specified, add restrictions per partition value filters.
        if (CollectionUtils.isNotEmpty(businessObjectDataSearchKey.getPartitionValueFilters()))
        {
            predicate = addPartitionValueFiltersToPredicate(businessObjectDataSearchKey.getPartitionValueFilters(), businessObjectDataEntityRoot,
                businessObjectFormatEntityJoin, builder, predicate);
        }

        // If specified, add restrictions per attribute value filters.
        if (CollectionUtils.isNotEmpty(businessObjectDataSearchKey.getAttributeValueFilters()))
        {
            predicate =
                addAttributeValueFiltersToPredicate(businessObjectDataSearchKey.getAttributeValueFilters(), businessObjectDataEntityRoot, builder, predicate);
        }

        // If specified, add restrictions per registration date range filter.
        if (businessObjectDataSearchKey.getRegistrationDateRangeFilter() != null)
        {
            predicate =
                addRegistrationDateRangeFilterToPredicate(businessObjectDataSearchKey.getRegistrationDateRangeFilter(), businessObjectDataEntityRoot, builder,
                    predicate);
        }

        // If specified, add restrictions per latest valid filter.
        // We only apply restriction on business object data status here. For performance reasons,
        // selection of actual latest valid versions does not take place on the database end.
        if (BooleanUtils.isTrue(businessObjectDataSearchKey.isFilterOnLatestValidVersion()))
        {
            predicate = builder
                .and(predicate, builder.equal(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.statusCode), BusinessObjectDataStatusEntity.VALID));
        }

        // If specified, add restrictions per retention expiration filter.
        if (BooleanUtils.isTrue(businessObjectDataSearchKey.isFilterOnRetentionExpiration()))
        {
            predicate =
                addRetentionExpirationFilterToPredicate(builder, businessObjectDataEntityRoot, businessObjectFormatEntityJoin, businessObjectDefinitionEntity,
                    predicate);
        }

        return predicate;
    }

    @Override
    public Integer getBusinessObjectDataLimitedCountBySearchKey(BusinessObjectDataSearchKey businessObjectDataSearchKey, Integer recordCountLimit)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Namespace and business object definition are required parameters, so fetch the relative business object definition entity to optimize the main query.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(
            new BusinessObjectDefinitionKey(businessObjectDataSearchKey.getNamespace(), businessObjectDataSearchKey.getBusinessObjectDefinitionName()));

        // If specified business object definition does not exist, then return a zero record count.
        if (businessObjectDefinitionEntity == null)
        {
            return 0;
        }

        // If file type is specified, fetch the relative entity to optimize the main query.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectDataSearchKey.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectDataSearchKey.getBusinessObjectFormatFileType());

            // If specified file type does not exist, then return a zero record count.
            if (fileTypeEntity == null)
            {
                return 0;
            }
        }

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);

        // Add select clause to the query. We use business object data partition value column for select, since this is enough to check the record count.
        criteria.select(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue));

        // Add standard restrictions to the query. (i.e. the standard where clauses).
        try
        {
            criteria.where(getQueryPredicateBySearchKey(builder, businessObjectDataEntityRoot, businessObjectFormatEntityJoin, businessObjectDataSearchKey,
                businessObjectDefinitionEntity, fileTypeEntity));
        }
        catch (IllegalArgumentException ex)
        {
            // This exception means that there are no records found for the query, thus return 0 record count.
            return 0;
        }

        // If latest valid version filter is specified, ignore business object format and business object data versions when counting search results.
        // In order to do that, we group by all elements of business object data alternate key, except for the versions.  Please note that we need to apply
        // upper() call on business object format usage, since it is not a dictionary value (does not have the relative lookup table).
        if (BooleanUtils.isTrue(businessObjectDataSearchKey.isFilterOnLatestValidVersion()))
        {
            List<Expression<?>> groupBy = new ArrayList<>();
            groupBy.add(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectDefinitionId));
            groupBy.add(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)));
            groupBy.add(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileTypeCode));
            groupBy.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue));
            groupBy.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue2));
            groupBy.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue3));
            groupBy.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue4));
            groupBy.add(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue5));
            criteria.groupBy(groupBy);
        }

        // Execute the query.
        List<String> businessObjectDataPartitionValues = entityManager.createQuery(criteria).setMaxResults(recordCountLimit).getResultList();

        // Return the size of the result list.
        return CollectionUtils.size(businessObjectDataPartitionValues);
    }

    @Override
    public List<BusinessObjectData> searchBusinessObjectData(BusinessObjectDataSearchKey businessObjectDataSearchKey, Integer pageNum, Integer pageSize)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Namespace and business object definition are required parameters, so fetch the relative business object definition entity to optimize the main query.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(
            new BusinessObjectDefinitionKey(businessObjectDataSearchKey.getNamespace(), businessObjectDataSearchKey.getBusinessObjectDefinitionName()));

        // If specified business object definition does not exist, then return an empty result list.
        if (businessObjectDefinitionEntity == null)
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

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.businessObjectFormat);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate;
        try
        {
            predicate = getQueryPredicateBySearchKey(builder, businessObjectDataEntityRoot, businessObjectFormatEntityJoin, businessObjectDataSearchKey,
                businessObjectDefinitionEntity, fileTypeEntity);
        }
        catch (IllegalArgumentException ex)
        {
            // This exception means that there are no records found for the query, thus return an empty result list.
            return Collections.emptyList();
        }

        // Build an order by clause. Please note that descending order on business object format and business object data versions along with applying upper()
        // call on business object format are required  for the logic that processes (when the relative search filter is specified) the search query results
        // to filters in the latest valid versions.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage))));
        orderBy.add(builder.asc(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.fileTypeCode)));
        orderBy.add(builder.desc(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue2)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue3)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue4)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.partitionValue5)));
        orderBy.add(builder.desc(businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.version)));

        // Add all clauses for the query.
        criteria.select(businessObjectDataEntityRoot).where(predicate).orderBy(orderBy);

        // Run the query to get a list of business object data entities.
        List<BusinessObjectDataEntity> businessObjectDataEntities =
            entityManager.createQuery(criteria).setFirstResult(pageSize * (pageNum - 1)).setMaxResults(pageSize).getResultList();

        // If the registration date range filter is applied then include the created on date in the business object data included in the response message.
        boolean includeCreatedOnDate = businessObjectDataSearchKey.getRegistrationDateRangeFilter() != null;

        // Crete the result list of business object data.
        return getQueryResultListFromEntityList(businessObjectDataEntities, businessObjectDataSearchKey.getAttributeValueFilters(), includeCreatedOnDate);
    }

    /**
     * Adds partition value filters to the query predicate.
     *
     * @param partitionValueFilters the list of partition value filters, not empty
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectFormatEntity the business object format entity
     * @param builder the builder
     * @param predicate the query predicate to be updated, not null
     *
     * @return the updated query predicate
     */
    private Predicate addPartitionValueFiltersToPredicate(List<PartitionValueFilter> partitionValueFilters,
        Root<BusinessObjectDataEntity> businessObjectDataEntity, Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity,
        CriteriaBuilder builder, Predicate predicate)
    {
        for (PartitionValueFilter partitionValueFilter : partitionValueFilters)
        {
            Join<BusinessObjectFormatEntity, SchemaColumnEntity> schemaEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.schemaColumns);

            List<String> partitionValues = partitionValueFilter.getPartitionValues();

            predicate = builder
                .and(predicate, builder.equal(builder.upper(schemaEntity.get(SchemaColumnEntity_.name)), partitionValueFilter.getPartitionKey().toUpperCase()));
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
            else if (partitionValueFilter.getPartitionValueRange() != null)
            {
                PartitionValueRange partitionRange = partitionValueFilter.getPartitionValueRange();
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

        return predicate;
    }

    /**
     * Adds retention expiration filter to the query predicate.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntityRoot the root business object data entity
     * @param businessObjectFormatEntityJoin the join with the business object format table
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param predicate the query predicate to be updated, not null
     *
     * @return the updated query predicate
     */
    private Predicate addRetentionExpirationFilterToPredicate(CriteriaBuilder builder, Root<BusinessObjectDataEntity> businessObjectDataEntityRoot,
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, Predicate predicate)
    {
        // Get latest versions of all business object formats that registered with the business object definition.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities =
            businessObjectFormatDao.getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(businessObjectDefinitionEntity);

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
                businessObjectDefinitionEntity.getName(), businessObjectDefinitionEntity.getNamespace().getCode()));

        // Add created business object definition retention expiration predicate to the query predicate passed to this method and return the result.
        return builder.and(predicate, businessObjectDefinitionRetentionExpirationPredicate);
    }

    /**
     * Adds attribute value filters to the query predicate.
     *
     * @param attributeValueFilters the list of attribute value filters, not empty
     * @param businessObjectDataEntityRoot the criteria root which is a business object data entity
     * @param builder the criteria builder
     * @param predicate the query predicate to be updated, not null
     *
     * @return the updated query predicate
     */
    private Predicate addAttributeValueFiltersToPredicate(final List<AttributeValueFilter> attributeValueFilters,
        final Root<BusinessObjectDataEntity> businessObjectDataEntityRoot, final CriteriaBuilder builder, Predicate predicate)
    {
        for (AttributeValueFilter attributeValueFilter : attributeValueFilters)
        {
            Join<BusinessObjectDataEntity, BusinessObjectDataAttributeEntity> dataAttributeEntity =
                businessObjectDataEntityRoot.join(BusinessObjectDataEntity_.attributes);

            if (!StringUtils.isEmpty(attributeValueFilter.getAttributeName()))
            {
                predicate = builder
                    .and(predicate, builder.equal(dataAttributeEntity.get(BusinessObjectDataAttributeEntity_.name), attributeValueFilter.getAttributeName()));
            }

            if (!StringUtils.isEmpty(attributeValueFilter.getAttributeValue()))
            {
                predicate = builder
                    .and(predicate, builder.equal(dataAttributeEntity.get(BusinessObjectDataAttributeEntity_.value), attributeValueFilter.getAttributeValue()));
            }
        }

        return predicate;
    }

    /**
     * Adds registration date-time range filter to the query predicate.
     *
     * @param registrationDateRangeFilter the registration date-time range filter, not null
     * @param businessObjectDataEntity the business object data entity
     * @param builder the query builder
     * @param predicate the query predicate to be updated, not null
     *
     * @return the updated query predicate
     */
    private Predicate addRegistrationDateRangeFilterToPredicate(RegistrationDateRangeFilter registrationDateRangeFilter,
        Root<BusinessObjectDataEntity> businessObjectDataEntity, CriteriaBuilder builder, Predicate predicate)
    {
        // Apply predicate for registration start timestamp
        if (registrationDateRangeFilter.getStartRegistrationDate() != null)
        {
            predicate = builder.and(predicate, builder.greaterThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn),
                HerdDateUtils.convertToTimestamp(registrationDateRangeFilter.getStartRegistrationDate())));
        }

        // Apply predicate for registration end timestamp
        if (registrationDateRangeFilter.getEndRegistrationDate() != null)
        {
            Date endDate;

            // Determine if registration end date contains a time portion; if not, assume end of day.
            if (HerdDateUtils.containsTimePortion(registrationDateRangeFilter.getEndRegistrationDate()))
            {
                endDate = HerdDateUtils.convertToTimestamp(registrationDateRangeFilter.getEndRegistrationDate());
            }
            else
            {
                endDate = DateUtils.addDays(HerdDateUtils.convertToTimestamp(registrationDateRangeFilter.getEndRegistrationDate()), 1);
            }

            predicate = builder.and(predicate, builder.lessThan(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn), endDate));
        }

        return predicate;
    }

    /**
     * Gets a query result list from an entity list.
     *
     * @param entityArray entity array from query
     * @param attributeValueList attribute value list
     * @param includeCreatedOnDate boolean parameter to determine if the business object data should include the created on date
     *
     * @return the list of business object data
     */
    private List<BusinessObjectData> getQueryResultListFromEntityList(List<BusinessObjectDataEntity> entityArray, List<AttributeValueFilter> attributeValueList,
        boolean includeCreatedOnDate)
    {
        List<BusinessObjectData> businessObjectDataList = new ArrayList<>();
        Set<Long> businessObjectIdSet = new HashSet<>();
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

            if (includeCreatedOnDate)
            {
                businessObjectData.setCreatedOn(HerdDateUtils.getXMLGregorianCalendarValue(dataEntity.getCreatedOn()));
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

    @Override
    public List<BusinessObjectDataKey> getBusinessObjectDataByBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        boolean latestBusinessObjectFormatVersion, Integer maxResults)
    {
        // Get ids for business object formats registered with the specified business object definition.
        List<Long> businessObjectFormatIds =
            businessObjectFormatDao.getBusinessObjectFormatIdsByBusinessObjectDefinition(businessObjectDefinitionEntity, latestBusinessObjectFormatVersion);

        // Return no results if the business object definition has no business object formats registered with it.
        if (CollectionUtils.isEmpty(businessObjectFormatIds))
        {
            return new ArrayList<>();
        }

        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntityRoot = criteria.from(BusinessObjectDataEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate =
            getPredicateForInClause(builder, businessObjectDataEntityRoot.get(BusinessObjectDataEntity_.businessObjectFormatId), businessObjectFormatIds);

        // Build an order by clause.
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
