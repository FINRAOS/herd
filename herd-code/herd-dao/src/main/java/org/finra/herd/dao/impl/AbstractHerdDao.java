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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.OneToMany;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.AuditableEntity;
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

public abstract class AbstractHerdDao extends BaseJpaDaoImpl
{
    /**
     * A default date mask for a single day formatted as yyyy-MM-dd.
     */
    public static final String DEFAULT_SINGLE_DAY_DATE_MASK = "yyyy-MM-dd";

    public static final int MAX_PARTITION_FILTERS_PER_REQUEST = 100;

    protected static final List<SingularAttribute<BusinessObjectDataEntity, String>> BUSINESS_OBJECT_DATA_PARTITIONS = Collections.unmodifiableList(Arrays
        .asList(BusinessObjectDataEntity_.partitionValue, BusinessObjectDataEntity_.partitionValue2, BusinessObjectDataEntity_.partitionValue3,
            BusinessObjectDataEntity_.partitionValue4, BusinessObjectDataEntity_.partitionValue5));

    protected static final List<SingularAttribute<BusinessObjectDataEntity, String>> BUSINESS_OBJECT_DATA_SUBPARTITIONS =
        BUSINESS_OBJECT_DATA_PARTITIONS.subList(1, 1 + BusinessObjectDataEntity.MAX_SUBPARTITIONS);

    /**
     * Represents aggregate function.
     */
    protected enum AggregateFunction
    {
        GREATEST,
        LEAST
    }

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a query restriction predicate for the
     * specified entities as per business object data key values.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param businessObjectFormatEntity the business object format entity that appears in the from clause
     * @param fileTypeEntity the file type entity that appears in the from clause
     * @param businessObjectDefinitionEntity the business object definition entity that appears in the from clause
     * @param businessObjectDataKey the business object data key
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestriction(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
        From<?, BusinessObjectFormatEntity> businessObjectFormatEntity, From<?, FileTypeEntity> fileTypeEntity,
        From<?, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        // Create the standard restrictions based on the business object format key values that are part of the business object data key.
        // Please note that we specify not to ignore the business object format version.
        Predicate predicate = getQueryRestriction(builder, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity,
            getBusinessObjectFormatKey(businessObjectDataKey), false);

        // Create and append a restriction on partition values.
        predicate = builder.and(predicate, getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntity, businessObjectDataKey));

        // If it is specified, create and append a restriction on business object data version.
        if (businessObjectDataKey.getBusinessObjectDataVersion() != null)
        {
            predicate = builder.and(predicate,
                builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.version), businessObjectDataKey.getBusinessObjectDataVersion()));
        }

        return predicate;
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a query restriction predicate for the
     * specified business object data entity as per primary and sub-partition values in the business object data key.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param businessObjectDataKey the business object data key
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
        BusinessObjectDataKey businessObjectDataKey)
    {
        return getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntity, businessObjectDataKey.getPartitionValue(),
            businessObjectDataKey.getSubPartitionValues());
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a query restriction predicate for the
     * specified business object data entity as per primary and sub-partition values in the business object data key.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param primaryPartitionValue the primary partition value of the business object data
     * @param subPartitionValues the list of sub-partition values for the business object data
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
        String primaryPartitionValue, List<String> subPartitionValues)
    {
        // Create a standard restriction on primary partition value.
        Predicate predicate = builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), primaryPartitionValue);

        // Create and add standard restrictions on sub-partition values. Please note that the subpartition value columns are nullable.
        int subPartitionValuesCount = getSubPartitionValuesCount(subPartitionValues);
        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            predicate = builder.and(predicate, i < subPartitionValuesCount ?
                builder.equal(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i)), subPartitionValues.get(i)) :
                builder.isNull(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i))));
        }

        return predicate;
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Returns number of sub-partition values in
     * the specified business object data key.
     *
     * @param subPartitionValues the list of sub-partition values for business object data
     *
     * @return the number of sub-partition values
     */
    protected int getSubPartitionValuesCount(List<String> subPartitionValues)
    {
        return subPartitionValues == null ? 0 : subPartitionValues.size();
    }

    /**
     * TODO This method may be bformat specific. Consider creating new abstract class to group all bformat related DAO. Builds a query restriction predicate for
     * the specified business object format entity as per business object format key values.
     *
     * @param builder the criteria builder
     * @param businessObjectFormatEntity the business object format entity that appears in the from clause
     * @param fileTypeEntity the file type entity that appears in the from clause
     * @param businessObjectDefinitionEntity the business object definition entity that appears in the from clause
     * @param businessObjectFormatKey the business object format key
     * @param ignoreBusinessObjectFormatVersion specifies whether to ignore the business object format version when building the predicate
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestriction(CriteriaBuilder builder, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        From<?, FileTypeEntity> fileTypeEntity, From<?, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity,
        BusinessObjectFormatKey businessObjectFormatKey, boolean ignoreBusinessObjectFormatVersion)
    {
        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions based on the business object format key values (i.e. the standard where clauses).

        // Create a restriction on namespace code.
        Predicate predicate = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectFormatKey.getNamespace().toUpperCase());

        // Create and append a restriction on business object definition name.
        predicate = builder.and(predicate, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectFormatKey.getBusinessObjectDefinitionName().toUpperCase()));

        // Create and append a restriction on business object format usage.
        predicate = builder.and(predicate, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            businessObjectFormatKey.getBusinessObjectFormatUsage().toUpperCase()));

        // Create and append a restriction on business object format file type.
        predicate = builder.and(predicate,
            builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectFormatKey.getBusinessObjectFormatFileType().toUpperCase()));

        // If specified, create and append a restriction on business object format version.
        if (!ignoreBusinessObjectFormatVersion && businessObjectFormatKey.getBusinessObjectFormatVersion() != null)
        {
            predicate = builder.and(predicate, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
                businessObjectFormatKey.getBusinessObjectFormatVersion()));
        }

        return predicate;
    }

    /**
     * TODO This method may be bformat specific. Consider creating new abstract class to group all bformat related DAO. Gets a business object format key from
     * the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the newly created business object format key
     */
    private BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectDataKey businessObjectDataKey)
    {
        return new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion());
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a query restriction predicate for the
     * sub-query business object data entity as per partition values from the specified main query business object data entity.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls.
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
        List<List<String>> partitionFilters)
    {
        // Create a query restriction as per specified primary and/or sub-partition values.
        Predicate predicate = null;
        for (List<String> partitionFilter : partitionFilters)
        {
            // Add restriction for each partition level if the relative partition value is specified in the partition filter.
            Predicate partitionRestriction = null;
            for (int partitionLevel = 0; partitionLevel < BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; partitionLevel++)
            {
                String partitionValue = partitionFilter.get(partitionLevel);
                if (StringUtils.isNotBlank(partitionValue))
                {
                    Predicate partitionValueRestriction =
                        builder.equal(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_PARTITIONS.get(partitionLevel)), partitionValue);
                    partitionRestriction =
                        (partitionRestriction == null ? partitionValueRestriction : builder.and(partitionRestriction, partitionValueRestriction));
                }
            }
            predicate = (predicate == null ? partitionRestriction : builder.or(predicate, partitionRestriction));
        }

        return predicate;
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a query restriction predicate for the
     * sub-query business object data entity as per partition values from the specified main query business object data entity.
     *
     * @param builder the criteria builder
     * @param subBusinessObjectDataEntity the sub-query business object data entity that appears in the from clause
     * @param mainBusinessObjectDataEntity the main query business object data entity that appears in the from clause
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> subBusinessObjectDataEntity,
        From<?, BusinessObjectDataEntity> mainBusinessObjectDataEntity)
    {
        // Create a standard restriction on primary partition value.
        Predicate predicate = builder.equal(subBusinessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue),
            mainBusinessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue));

        // Create and add standard restrictions on sub-partition values. Please note that the subpartition value columns are nullable.
        for (SingularAttribute<BusinessObjectDataEntity, String> businessObjectDataPartitionValueSingularAttribute : BUSINESS_OBJECT_DATA_SUBPARTITIONS)
        {
            predicate = builder.and(predicate, builder.or(builder
                .and(builder.isNull(subBusinessObjectDataEntity.get(businessObjectDataPartitionValueSingularAttribute)),
                    builder.isNull(mainBusinessObjectDataEntity.get(businessObjectDataPartitionValueSingularAttribute))), builder
                .equal(subBusinessObjectDataEntity.get(businessObjectDataPartitionValueSingularAttribute),
                    mainBusinessObjectDataEntity.get(businessObjectDataPartitionValueSingularAttribute))));
        }

        return predicate;
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds query restriction predicates and adds
     * them to the query restriction as per specified business object data version and status. If a business object data version is specified, the business
     * object data status is ignored. When both business object data version and business object data status are not specified, the sub-query restriction is not
     * getting updated.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param businessObjectDataStatusEntity the business object data status entity that appears in the from clause
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the query restriction predicate or null if both business object data version and business object data status are not specified
     */
    protected Predicate getQueryRestrictionOnBusinessObjectDataVersionAndStatus(CriteriaBuilder builder,
        From<?, BusinessObjectDataEntity> businessObjectDataEntity, From<?, BusinessObjectDataStatusEntity> businessObjectDataStatusEntity,
        Integer businessObjectDataVersion, String businessObjectDataStatus)
    {
        Predicate predicate = null;

        // If specified, create a standard restriction on the business object data version.
        if (businessObjectDataVersion != null)
        {
            predicate = builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.version), businessObjectDataVersion);
        }
        // Only if a business object data version is not specified, check if we need to add a restriction on the business object data status.
        else if (businessObjectDataStatus != null)
        {
            predicate =
                builder.equal(builder.upper(businessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code)), businessObjectDataStatus.toUpperCase());
        }

        return predicate;
    }

    /**
     * Builds a query restriction predicate for the storage.
     *
     * @param builder the criteria builder
     * @param storageEntity the storage entity that appears in the from clause
     * @param storagePlatformEntity the storage platform entity that appears in the from clause
     * @param storageNames the list of storage names where the business object data storage units should be looked for (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     *
     * @return the query restriction predicate
     */
    protected Predicate getQueryRestrictionOnStorage(CriteriaBuilder builder, From<?, StorageEntity> storageEntity,
        From<?, StoragePlatformEntity> storagePlatformEntity, List<String> storageNames, String storagePlatformType, String excludedStoragePlatformType)
    {
        List<Predicate> predicates = new ArrayList<>();

        // If specified, add restriction on storage.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            // Add a storage name restriction to the main query where clause.
            List<String> uppercaseStorageNames = new ArrayList<>();
            for (String storageName : storageNames)
            {
                uppercaseStorageNames.add(storageName.toUpperCase());
            }
            predicates.add(builder.upper(storageEntity.get(StorageEntity_.name)).in(uppercaseStorageNames));
        }
        else if (StringUtils.isNotBlank(storagePlatformType))
        {
            // Select storage units only from the storages of the specified storage platform type.
            predicates.add(builder.equal(storagePlatformEntity.get(StoragePlatformEntity_.name), storagePlatformType));
        }
        else if (StringUtils.isNotBlank(excludedStoragePlatformType))
        {
            // Ignore any storages of the excluded storage platform type.
            predicates.add(builder.notEqual(storagePlatformEntity.get(StoragePlatformEntity_.name), excludedStoragePlatformType));
        }

        return builder.and(predicates.toArray(new Predicate[predicates.size()]));
    }

    /**
     * TODO This method may be bdata specific. Consider creating new abstract class to group all bdata related DAO. Builds a sub-query to select the maximum
     * business object data version.
     *
     * @param builder the criteria builder
     * @param criteria the criteria query
     * @param businessObjectDataEntity the business object data entity that appears in the from clause of the main query
     * @param businessObjectFormatEntity the business object format entity that appears in the from clause of the main query
     * @param businessObjectDataStatus the business object data status
     * @param storageNames the list of storage names where the business object data storage units should be looked for (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     *
     * @return the sub-query to select the maximum business object data version
     */
    protected Subquery<Integer> getMaximumBusinessObjectDataVersionSubQuery(CriteriaBuilder builder, CriteriaQuery<?> criteria,
        From<?, BusinessObjectDataEntity> businessObjectDataEntity, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        String businessObjectDataStatus, List<String> storageNames, String storagePlatformType, String excludedStoragePlatformType,
        boolean selectOnlyAvailableStorageUnits)
    {
        // Business object data version is not specified, so get the latest one in the specified storage.
        Subquery<Integer> subQuery = criteria.subquery(Integer.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> subStorageUnitEntity = subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> subStorageEntity = subStorageUnitEntity.join(StorageUnitEntity_.storage);
        Join<StorageEntity, StoragePlatformEntity> subStoragePlatformEntity = subStorageEntity.join(StorageEntity_.storagePlatform);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> subBusinessObjectFormatEntity =
            subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<StorageUnitEntity, StorageUnitStatusEntity> subStorageUnitStatusEntity = subStorageUnitEntity.join(StorageUnitEntity_.status);

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

        // Create and add a standard restriction on storage.
        subQueryRestriction = builder.and(subQueryRestriction,
            getQueryRestrictionOnStorage(builder, subStorageEntity, subStoragePlatformEntity, storageNames, storagePlatformType, excludedStoragePlatformType));

        // If specified, add a restriction on storage unit status availability flag.
        if (selectOnlyAvailableStorageUnits)
        {
            subQueryRestriction = builder.and(subQueryRestriction, builder.isTrue(subStorageUnitStatusEntity.get(StorageUnitStatusEntity_.available)));
        }

        subQuery.select(builder.max(subBusinessObjectDataEntity.get(BusinessObjectDataEntity_.version))).where(subQueryRestriction);

        return subQuery;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This method overwrites the base class method by first updating an entity's audit fields if the entity is of type AuditableEntity.
     */
    @Override
    public <T> T save(T entity)
    {
        updateAuditFields(entity);
        return super.save(entity);
    }

    /**
     * Updates the audit fields if the entity is of type AuditableEntity.
     *
     * @param entity the entity
     * @param <T> the type of entity
     */
    @SuppressWarnings("rawtypes")
    private <T> void updateAuditFields(T entity)
    {
        if (entity instanceof AuditableEntity)
        {
            AuditableEntity auditableEntity = (AuditableEntity) entity;

            // Get the currently logged in username.
            String username = herdDaoSecurityHelper.getCurrentUsername();

            // Always set the updated by field, but only set the created by field when it is null (i.e. this is a new record).
            if (auditableEntity.getCreatedBy() == null)
            {
                auditableEntity.setCreatedBy(username);
            }
            auditableEntity.setUpdatedBy(username);

            // Always set the updated on field to the current time, but only update the created on field when it is null (i.e. the first time).
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            auditableEntity.setUpdatedOn(currentTime);
            if (auditableEntity.getCreatedOn() == null)
            {
                auditableEntity.setCreatedOn(currentTime);
            }
        }

        // Try to update children one-to-many cascadable auditable entities.
        // Note that this assumes that OneToMany annotations are done on the field (as opposed to the method) and that all OneToMany fields are collections.
        // This approach also assumes that there are loops where children refer back to our entity (i.e. an infinite loop).
        // If there are other scenarios, we should modify this code to handle them.

        // Loop through all the fields of this entity.
        for (Field field : entity.getClass().getDeclaredFields())
        {
            // Get all the annotations for the field.
            for (Annotation annotation : field.getDeclaredAnnotations())
            {
                // Only look for OneToMany that cascade with "persist" or "merge".
                if (annotation instanceof OneToMany)
                {
                    OneToMany oneToManyAnnotation = (OneToMany) annotation;
                    List<CascadeType> cascadeTypes = new ArrayList<>(Arrays.asList(oneToManyAnnotation.cascade()));
                    if ((cascadeTypes.contains(CascadeType.ALL)) || (cascadeTypes.contains(CascadeType.PERSIST)) || cascadeTypes.contains(CascadeType.MERGE))
                    {
                        try
                        {
                            // Modify the accessibility to true so we can get the field (even if it's private) and get the value of the field for our entity.
                            field.setAccessible(true);
                            Object fieldValue = field.get(entity);

                            // If the field is a collection (which OneToMany annotated fields should be), then iterate through the collection and look for
                            // child auditable entities.
                            if (fieldValue instanceof Collection)
                            {
                                Collection collection = (Collection) fieldValue;
                                for (Object object : collection)
                                {
                                    if (object instanceof AuditableEntity)
                                    {
                                        // We found a child auditable entity so recurse to update it's audit fields as well.
                                        updateAuditFields(object);
                                    }
                                }
                            }
                        }
                        catch (IllegalAccessException ex)
                        {
                            // Because we're setting accessible to true above, we shouldn't get here.
                            throw new IllegalStateException("Unable to get field value for field \"" + field.getName() + "\" due to access restriction.", ex);
                        }
                    }
                }
            }
        }
    }
}
