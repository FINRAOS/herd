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
package org.finra.dm.dao.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.OneToMany;
import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.dm.core.DmDateUtils;
import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.dao.helper.DmDaoSecurityHelper;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.dto.DateRangeDto;
import org.finra.dm.model.dto.StorageAlternateKeyDto;
import org.finra.dm.model.jpa.AuditableEntity;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataEntity_;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity_;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity_;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity_;
import org.finra.dm.model.jpa.ConfigurationEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.CustomDdlEntity_;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.DataProviderEntity_;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity_;
import org.finra.dm.model.jpa.ExpectedPartitionValueEntity;
import org.finra.dm.model.jpa.ExpectedPartitionValueEntity_;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.FileTypeEntity_;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.jpa.JmsMessageEntity_;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.JobDefinitionEntity_;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.NamespaceEntity_;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity_;
import org.finra.dm.model.jpa.OnDemandPriceEntity;
import org.finra.dm.model.jpa.OnDemandPriceEntity_;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity_;
import org.finra.dm.model.jpa.SecurityFunctionEntity;
import org.finra.dm.model.jpa.SecurityFunctionEntity_;
import org.finra.dm.model.jpa.SecurityRoleEntity;
import org.finra.dm.model.jpa.SecurityRoleEntity_;
import org.finra.dm.model.jpa.SecurityRoleFunctionEntity;
import org.finra.dm.model.jpa.SecurityRoleFunctionEntity_;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageEntity_;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageFileEntity_;
import org.finra.dm.model.jpa.StorageFileViewEntity;
import org.finra.dm.model.jpa.StorageFileViewEntity_;
import org.finra.dm.model.jpa.StoragePlatformEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.jpa.StorageUnitEntity_;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.EmrClusterDefinitionKey;
import org.finra.dm.model.api.xml.ExpectedPartitionValueKey;
import org.finra.dm.model.api.xml.FileTypeKey;
import org.finra.dm.model.api.xml.NamespaceKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.api.xml.PartitionValueRange;
import org.finra.dm.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStat;
import org.finra.dm.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.dm.model.api.xml.StorageDailyUploadStat;
import org.finra.dm.model.api.xml.StorageDailyUploadStats;
import org.finra.dm.model.api.xml.StorageKey;

/**
 * The DM DAO implementation.
 */
// TODO: This class is too big and should be split up into smaller classes (e.g. NamespaceDao, StorageDao, etc.).
// TODO:     When this is fixed, we can remove the PMD suppress warning below.
@SuppressWarnings({"PMD.ExcessivePublicCount", "PMD.ExcessiveClassLength"})
@Repository
public class DmDaoImpl extends BaseJpaDaoImpl implements DmDao
{
    public static final int MAX_PARTITION_FILTERS_PER_REQUEST = 100;

    private static final List<SingularAttribute<BusinessObjectDataEntity, String>> BUSINESS_OBJECT_DATA_PARTITIONS = Arrays
        .asList(BusinessObjectDataEntity_.partitionValue, BusinessObjectDataEntity_.partitionValue2, BusinessObjectDataEntity_.partitionValue3,
            BusinessObjectDataEntity_.partitionValue4, BusinessObjectDataEntity_.partitionValue5);

    private static final List<SingularAttribute<BusinessObjectDataEntity, String>> BUSINESS_OBJECT_DATA_SUBPARTITIONS =
        BUSINESS_OBJECT_DATA_PARTITIONS.subList(1, 1 + BusinessObjectDataEntity.MAX_SUBPARTITIONS);

    /**
     * Represents aggregate function.
     */
    private enum AggregateFunction
    {
        GREATEST, LEAST
    }

    // TODO: Remove autowired environment once we migrate away from Oracle.  It is currently used only to check what database we are using.
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DmDaoSecurityHelper dmDaoSecurityHelper;

    // Configuration

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigurationEntity getConfigurationByKey(String key)
    {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigurationEntity.COLUMN_KEY, key);
        return queryUniqueByNamedQueryAndNamedParams(ConfigurationEntity.QUERY_GET_CONFIGURATION_BY_KEY, params);
    }

    // Namespace

    /**
     * {@inheritDoc}
     */
    @Override
    public NamespaceEntity getNamespaceByKey(NamespaceKey namespaceKey)
    {
        return getNamespaceByCd(namespaceKey.getNamespaceCode());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NamespaceEntity getNamespaceByCd(String namespaceCode)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceEntity> criteria = builder.createQuery(NamespaceEntity.class);

        // The criteria root is the namespace.
        Root<NamespaceEntity> namespaceEntity = criteria.from(NamespaceEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespaceCode.toUpperCase());

        criteria.select(namespaceEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one namespace with namespaceCode=\"%s\".", namespaceCode));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NamespaceKey> getNamespaces()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object definition.
        Root<NamespaceEntity> namespaceEntity = criteria.from(NamespaceEntity.class);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);

        // Add the select clause.
        criteria.select(namespaceCodeColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(namespaceCodeColumn));

        // Run the query to get a list of namespace codes back.
        List<String> namespaceCodes = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned namespace codes.
        List<NamespaceKey> namespaceKeys = new ArrayList<>();
        for (String namespaceCode : namespaceCodes)
        {
            namespaceKeys.add(new NamespaceKey(namespaceCode));
        }

        return namespaceKeys;
    }

    // DataProvider

    /**
     * {@inheritDoc}
     */
    @Override
    public DataProviderEntity getDataProviderByName(String name)
    {
        Map<String, Object> params = new HashMap<>();
        params.put(DataProviderEntity.COLUMN_NAME, name);
        return queryUniqueByNamedQueryAndNamedParams(DataProviderEntity.QUERY_GET_DATA_PROVIDER_BY_NAME, params);
    }

    // BusinessObjectDefinition

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectDefinitionKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName().toUpperCase()));

        criteria.select(businessObjectDefinitionEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one business object definition with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"}.",
                businessObjectDefinitionKey.getNamespace(), businessObjectDefinitionKey.getBusinessObjectDefinitionName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDefinitionEntity getLegacyBusinessObjectDefinitionByName(String businessObjectDefinitionName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionEntity> criteria = builder.createQuery(BusinessObjectDefinitionEntity.class);

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = criteria.from(BusinessObjectDefinitionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)), businessObjectDefinitionName.toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.isTrue(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.legacy)));

        criteria.select(businessObjectDefinitionEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one business object definition with parameters {businessObjectDefinitionName=\"%s\", legacy=\"Y\"}.",
                businessObjectDefinitionName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitions()
    {
        return getBusinessObjectDefinitions(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitions(String namespaceCode)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn);

        // If namespace code is specified, add the where clause.
        if (StringUtils.isNotBlank(namespaceCode))
        {
            criteria.where(builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespaceCode.toUpperCase()));
        }

        // Add the order by clause.
        if (StringUtils.isNotBlank(namespaceCode))
        {
            criteria.orderBy(builder.asc(namespaceCodeColumn), builder.asc(businessObjectDefinitionNameColumn));
        }
        else
        {
            criteria.orderBy(builder.asc(businessObjectDefinitionNameColumn));
        }

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey();
            businessObjectDefinitionKeys.add(businessObjectDefinitionKey);
            businessObjectDefinitionKey.setNamespace(tuple.get(namespaceCodeColumn));
            businessObjectDefinitionKey.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNameColumn));
        }

        return businessObjectDefinitionKeys;
    }

    // FileType

    /**
     * {@inheritDoc}
     */
    @Override
    public FileTypeEntity getFileTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<FileTypeEntity> criteria = builder.createQuery(FileTypeEntity.class);

        // The criteria root is the file types.
        Root<FileTypeEntity> fileType = criteria.from(FileTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate fileTypeCodeRestriction = builder.equal(builder.upper(fileType.get(FileTypeEntity_.code)), code.toUpperCase());

        criteria.select(fileType).where(fileTypeCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one file type with code \"%s\".", code));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FileTypeKey> getFileTypes()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the file type.
        Root<FileTypeEntity> fileTypeEntity = criteria.from(FileTypeEntity.class);

        // Get the columns.
        Path<String> fileTypeCodeColumn = fileTypeEntity.get(FileTypeEntity_.code);

        // Add the select clause.
        criteria.select(fileTypeCodeColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(fileTypeCodeColumn));

        // Run the query to get a list of file type codes back.
        List<String> fileTypeCodes = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned file type codes.
        List<FileTypeKey> fileTypeKeys = new ArrayList<>();
        for (String fileTypeCode : fileTypeCodes)
        {
            fileTypeKeys.add(new FileTypeKey(fileTypeCode));
        }

        return fileTypeKeys;
    }

    // BusinessObjectFormat

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectFormatEntity getBusinessObjectFormatByAltKey(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectFormatEntity> criteria = builder.createQuery(BusinessObjectFormatEntity.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntity = criteria.from(BusinessObjectFormatEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).
        // Please note that we specify not to ignore the business object format version.
        Predicate queryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity, businessObjectFormatKey, false);

        // If a business format version was not specified, use the latest one.
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() == null)
        {
            queryRestriction = builder.and(queryRestriction, builder.isTrue(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.latestVersion)));
        }

        criteria.select(businessObjectFormatEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one business object format instance with parameters " +
            "{namespace=\"%s\", businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
            "businessObjectFormatVersion=\"%d\"}.", businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
            businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
            businessObjectFormatKey.getBusinessObjectFormatVersion()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getBusinessObjectFormatMaxVersion(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Integer> criteria = builder.createQuery(Integer.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntity = criteria.from(BusinessObjectFormatEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create the standard restrictions (i.e. the standard where clauses).
        // Business object format version should be ignored when building the query restriction.
        Predicate queryRestriction =
            getQueryRestriction(builder, businessObjectFormatEntity, fileTypeEntity, businessObjectDefinitionEntity, businessObjectFormatKey, true);

        // Create the path.
        Expression<Integer> maxBusinessObjectFormatVersion =
            builder.max(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion));

        criteria.select(maxBusinessObjectFormatVersion).where(queryRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getBusinessObjectFormatCount(PartitionKeyGroupEntity partitionKeyGroupEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntity = criteria.from(BusinessObjectFormatEntity.class);

        // Create path.
        Expression<Long> businessObjectFormatCount = builder.count(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.id));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate partitionKeyGroupRestriction =
            builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.partitionKeyGroup), partitionKeyGroupEntity);

        criteria.select(businessObjectFormatCount).where(partitionKeyGroupRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectFormatKey> getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        boolean latestBusinessObjectFormatVersion)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntity = criteria.from(BusinessObjectFormatEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);
        Path<String> businessObjectFormatUsageColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage);
        Path<String> fileTypeCodeColumn = fileTypeEntity.get(FileTypeEntity_.code);
        Path<Integer> businessObjectFormatVersionColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion);
        Expression<Integer> maxBusinessObjectFormatVersionExpression =
            builder.max(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectDefinitionKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName().toUpperCase()));

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn, businessObjectFormatUsageColumn, fileTypeCodeColumn,
            latestBusinessObjectFormatVersion ? maxBusinessObjectFormatVersionExpression : businessObjectFormatVersionColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // If only the latest (maximum) business object format versions to be returned, create and apply the group by clause.
        if (latestBusinessObjectFormatVersion)
        {
            List<Expression<?>> grouping = new ArrayList<>();
            grouping.add(namespaceCodeColumn);
            grouping.add(businessObjectDefinitionNameColumn);
            grouping.add(businessObjectFormatUsageColumn);
            grouping.add(fileTypeCodeColumn);
            criteria.groupBy(grouping);
        }

        // Add the order by clause.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(businessObjectFormatUsageColumn));
        orderBy.add(builder.asc(fileTypeCodeColumn));
        if (!latestBusinessObjectFormatVersion)
        {
            orderBy.add(builder.asc(businessObjectFormatVersionColumn));
        }
        criteria.orderBy(orderBy);

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectFormatKey> businessObjectFormatKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
            businessObjectFormatKeys.add(businessObjectFormatKey);
            businessObjectFormatKey.setNamespace(tuple.get(namespaceCodeColumn));
            businessObjectFormatKey.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNameColumn));
            businessObjectFormatKey.setBusinessObjectFormatUsage(tuple.get(businessObjectFormatUsageColumn));
            businessObjectFormatKey.setBusinessObjectFormatFileType(tuple.get(fileTypeCodeColumn));
            businessObjectFormatKey.setBusinessObjectFormatVersion(
                tuple.get(latestBusinessObjectFormatVersion ? maxBusinessObjectFormatVersionExpression : businessObjectFormatVersionColumn));
        }

        return businessObjectFormatKeys;
    }

    /**
     * Builds a query restriction predicate for the specified business object format entity as per business object format key values.
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
    private Predicate getQueryRestriction(CriteriaBuilder builder, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
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

    // PartitionKeyGroup

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionKeyGroupEntity getPartitionKeyGroupByKey(PartitionKeyGroupKey partitionKeyGroupKey)
    {
        return getPartitionKeyGroupByName(partitionKeyGroupKey.getPartitionKeyGroupName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionKeyGroupEntity getPartitionKeyGroupByName(String partitionKeyGroupName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PartitionKeyGroupEntity> criteria = builder.createQuery(PartitionKeyGroupEntity.class);

        // The criteria root is the partition key group.
        Root<PartitionKeyGroupEntity> partitionKeyGroupEntity = criteria.from(PartitionKeyGroupEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate partitionKeyGroupRestriction =
            builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)), partitionKeyGroupName.toUpperCase());

        criteria.select(partitionKeyGroupEntity).where(partitionKeyGroupRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one \"%s\" partition key group.", partitionKeyGroupName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PartitionKeyGroupKey> getPartitionKeyGroups()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PartitionKeyGroupEntity> criteria = builder.createQuery(PartitionKeyGroupEntity.class);

        // The criteria root is the partition key group.
        Root<PartitionKeyGroupEntity> partitionKeyGroupEntity = criteria.from(PartitionKeyGroupEntity.class);

        criteria.select(partitionKeyGroupEntity);

        List<PartitionKeyGroupKey> partitionKeyGroupKeys = new ArrayList<>();
        for (PartitionKeyGroupEntity entity : entityManager.createQuery(criteria).getResultList())
        {
            PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
            partitionKeyGroupKey.setPartitionKeyGroupName(entity.getPartitionKeyGroupName());
            partitionKeyGroupKeys.add(partitionKeyGroupKey);
        }

        return partitionKeyGroupKeys;
    }

    // ExpectedPartitionValue

    /**
     * {@inheritDoc}
     */
    @Override
    public ExpectedPartitionValueEntity getExpectedPartitionValue(ExpectedPartitionValueKey expectedPartitionValueKey, int offset)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExpectedPartitionValueEntity> criteria = builder.createQuery(ExpectedPartitionValueEntity.class);

        // The criteria root is the expected partition value.
        Root<ExpectedPartitionValueEntity> expectedPartitionValueEntity = criteria.from(ExpectedPartitionValueEntity.class);

        // Join to the other tables we can filter on.
        Join<ExpectedPartitionValueEntity, PartitionKeyGroupEntity> partitionKeyGroupEntity =
            expectedPartitionValueEntity.join(ExpectedPartitionValueEntity_.partitionKeyGroup);

        // Add a restriction to filter case insensitive groups that match the user specified group.
        Predicate whereRestriction = builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)),
            expectedPartitionValueKey.getPartitionKeyGroupName().toUpperCase());

        // Depending on the offset, we might need to order the records in the query.
        Order orderByExpectedPartitionValue = null;

        // Add additional restrictions to handle expected partition value and an optional offset.
        if (offset == 0)
        {
            // Since there is no offset, we need to match the expected partition value exactly.
            whereRestriction = builder.and(whereRestriction, builder
                .equal(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue), expectedPartitionValueKey.getExpectedPartitionValue()));
        }
        else if (offset > 0)
        {
            // For a positive offset value, add a restriction to filter expected partition values that are >= the user specified expected partition value.
            whereRestriction = builder.and(whereRestriction, builder
                .greaterThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                    expectedPartitionValueKey.getExpectedPartitionValue()));

            // Order by expected partition value in ascending order.
            orderByExpectedPartitionValue = builder.asc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));
        }
        else
        {
            // For a negative offset value, add a restriction to filter expected partition values that are <= the user specified expected partition value.
            whereRestriction = builder.and(whereRestriction, builder
                .lessThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                    expectedPartitionValueKey.getExpectedPartitionValue()));

            // Order by expected partition value in descending order.
            orderByExpectedPartitionValue = builder.desc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));
        }

        // Add the clauses for the query and execute the query.
        if (offset == 0)
        {
            criteria.select(expectedPartitionValueEntity).where(whereRestriction);

            return executeSingleResultQuery(criteria, String
                .format("Found more than one expected partition value with parameters {partitionKeyGroupName=\"%s\", expectedPartitionValue=\"%s\"}.",
                    expectedPartitionValueKey.getPartitionKeyGroupName(), expectedPartitionValueKey.getExpectedPartitionValue()));
        }
        else
        {
            criteria.select(expectedPartitionValueEntity).where(whereRestriction).orderBy(orderByExpectedPartitionValue);

            List<ExpectedPartitionValueEntity> resultList =
                entityManager.createQuery(criteria).setFirstResult(Math.abs(offset)).setMaxResults(1).getResultList();

            return resultList.size() > 0 ? resultList.get(0) : null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ExpectedPartitionValueEntity> getExpectedPartitionValuesByGroupAndRange(String partitionKeyGroupName, PartitionValueRange partitionValueRange)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExpectedPartitionValueEntity> criteria = builder.createQuery(ExpectedPartitionValueEntity.class);

        // The criteria root is the expected partition value.
        Root<ExpectedPartitionValueEntity> expectedPartitionValueEntity = criteria.from(ExpectedPartitionValueEntity.class);

        // Join to the other tables we can filter on.
        Join<ExpectedPartitionValueEntity, PartitionKeyGroupEntity> partitionKeyGroupEntity =
            expectedPartitionValueEntity.join(ExpectedPartitionValueEntity_.partitionKeyGroup);

        // Add a restriction to filter case insensitive groups that match the user specified group.
        Predicate whereRestriction =
            builder.equal(builder.upper(partitionKeyGroupEntity.get(PartitionKeyGroupEntity_.partitionKeyGroupName)), partitionKeyGroupName.toUpperCase());

        // If we have a possible partition value range, we need to add additional restrictions.
        if (partitionValueRange != null)
        {
            // Add a restriction to filter values that are >= the user specified range start value.
            if (StringUtils.isNotBlank(partitionValueRange.getStartPartitionValue()))
            {
                whereRestriction = builder.and(whereRestriction, builder
                    .greaterThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                        partitionValueRange.getStartPartitionValue()));
            }

            // Add a restriction to filter values that are <= the user specified range end value.
            if (StringUtils.isNotBlank(partitionValueRange.getEndPartitionValue()))
            {
                whereRestriction = builder.and(whereRestriction, builder
                    .lessThanOrEqualTo(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue),
                        partitionValueRange.getEndPartitionValue()));
            }
        }

        // Order the results by partition value.
        Order orderByValue = builder.asc(expectedPartitionValueEntity.get(ExpectedPartitionValueEntity_.partitionValue));

        // Add the clauses for the query.
        criteria.select(expectedPartitionValueEntity).where(whereRestriction).orderBy(orderByValue);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    // CustomDdl

    /**
     * {@inheritDoc}
     */
    @Override
    public CustomDdlEntity getCustomDdlByKey(CustomDdlKey customDdlKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CustomDdlEntity> criteria = builder.createQuery(CustomDdlEntity.class);

        // The criteria root is the custom DDL.
        Root<CustomDdlEntity> customDdlEntity = criteria.from(CustomDdlEntity.class);

        // Join to the other tables we can filter on.
        Join<CustomDdlEntity, BusinessObjectFormatEntity> businessObjectFormatEntity = customDdlEntity.join(CustomDdlEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), customDdlKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            customDdlKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            customDdlKey.getBusinessObjectFormatUsage().toUpperCase()));
        queryRestriction = builder.and(queryRestriction,
            builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), customDdlKey.getBusinessObjectFormatFileType().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder
            .equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion), customDdlKey.getBusinessObjectFormatVersion()));
        queryRestriction = builder.and(queryRestriction,
            builder.equal(builder.upper(customDdlEntity.get(CustomDdlEntity_.customDdlName)), customDdlKey.getCustomDdlName().toUpperCase()));

        // Add select and where clauses.
        criteria.select(customDdlEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one custom DDL instance with parameters {businessObjectDefinitionName=\"%s\"," +
                " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\", customDdlName=\"%s\"}.",
            customDdlKey.getBusinessObjectDefinitionName(), customDdlKey.getBusinessObjectFormatUsage(), customDdlKey.getBusinessObjectFormatFileType(),
            customDdlKey.getBusinessObjectFormatVersion(), customDdlKey.getCustomDdlName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<CustomDdlKey> getCustomDdls(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object format.
        Root<CustomDdlEntity> customDdlEntity = criteria.from(CustomDdlEntity.class);

        // Join to the other tables we can filter on.
        Join<CustomDdlEntity, BusinessObjectFormatEntity> businessObjectFormatEntity = customDdlEntity.join(CustomDdlEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);
        Path<String> businessObjectFormatUsageColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage);
        Path<String> fileTypeCodeColumn = fileTypeEntity.get(FileTypeEntity_.code);
        Path<Integer> businessObjectFormatVersionColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion);
        Path<String> customDdlNameColumn = customDdlEntity.get(CustomDdlEntity_.customDdlName);

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

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn, businessObjectFormatUsageColumn, fileTypeCodeColumn,
            businessObjectFormatVersionColumn, customDdlNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(customDdlNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<CustomDdlKey> customDdlKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            CustomDdlKey customDdlKey = new CustomDdlKey();
            customDdlKeys.add(customDdlKey);
            customDdlKey.setNamespace(tuple.get(namespaceCodeColumn));
            customDdlKey.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNameColumn));
            customDdlKey.setBusinessObjectFormatUsage(tuple.get(businessObjectFormatUsageColumn));
            customDdlKey.setBusinessObjectFormatFileType(tuple.get(fileTypeCodeColumn));
            customDdlKey.setBusinessObjectFormatVersion(tuple.get(businessObjectFormatVersionColumn));
            customDdlKey.setCustomDdlName(tuple.get(customDdlNameColumn));
        }

        return customDdlKeys;
    }

    // BusinessObjectDataStatus

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDataStatusEntity getBusinessObjectDataStatusByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataStatusEntity> criteria = builder.createQuery(BusinessObjectDataStatusEntity.class);

        // The criteria root is the business object data statuses.
        Root<BusinessObjectDataStatusEntity> businessObjectDataStatus = criteria.from(BusinessObjectDataStatusEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate businessObjectDataStatusCodeRestriction =
            builder.equal(builder.upper(businessObjectDataStatus.get(BusinessObjectDataStatusEntity_.code)), code.toUpperCase());

        criteria.select(businessObjectDataStatus).where(businessObjectDataStatusCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one business object data status with code \"%s\".", code));
    }

    // BusinessObjectData

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDataEntity getBusinessObjectDataByAltKey(BusinessObjectDataKey businessObjectDataKey)
    {
        return getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, null);
    }

    /**
     * {@inheritDoc}
     */
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

        return executeSingleResultQuery(criteria,
            String.format("Found more than one business object data instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\"," +
                " businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", businessObjectDataVersion=\"%d\"," +
                " businessObjectDataStatus=\"%s\"}.", businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" :
                    StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                businessObjectDataStatus));
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBusinessObjectDataMaxPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String storageName, String upperBoundPartitionValue, String lowerBoundPartitionValue)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, storageName,
            AggregateFunction.GREATEST, upperBoundPartitionValue, lowerBoundPartitionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String storageName)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, storageName,
            AggregateFunction.LEAST, null, null);
    }

    /**
     * Retrieves partition value per specified parameters that includes the aggregate function.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version for each
     * partition value will be used.
     * @param storageName the name of the storage where the business object data storage unit is located (case-insensitive)
     * @param aggregateFunction the aggregate function to use against partition values
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the partition value
     */
    private String getBusinessObjectDataPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String storageName, AggregateFunction aggregateFunction, String upperBoundPartitionValue,
        String lowerBoundPartitionValue)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntity = businessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create the path.
        Expression<String> partitionValue;
        SingularAttribute<BusinessObjectDataEntity, String> singleValuedAttribute = BUSINESS_OBJECT_DATA_PARTITIONS.get(partitionColumnPosition - 1);
        switch (aggregateFunction)
        {
            case GREATEST:
                partitionValue = builder.greatest(businessObjectDataEntity.get(singleValuedAttribute));
                break;
            case LEAST:
                partitionValue = builder.least(businessObjectDataEntity.get(singleValuedAttribute));
                break;
            default:
                throw new IllegalArgumentException("Invalid aggregate function found: \"" + aggregateFunction + "\".");
        }

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate mainQueryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectFormatKey.getNamespace().toUpperCase());
        mainQueryRestriction = builder.and(mainQueryRestriction, builder
            .equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
                businessObjectFormatKey.getBusinessObjectDefinitionName().toUpperCase()));
        mainQueryRestriction = builder.and(mainQueryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            businessObjectFormatKey.getBusinessObjectFormatUsage().toUpperCase()));
        mainQueryRestriction = builder.and(mainQueryRestriction,
            builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectFormatKey.getBusinessObjectFormatFileType().toUpperCase()));

        // If a business object format version was specified, use it.
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() != null)
        {
            mainQueryRestriction = builder.and(mainQueryRestriction, builder
                .equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
                    businessObjectFormatKey.getBusinessObjectFormatVersion()));
        }

        // If a data version was specified, use it. Otherwise, use the latest one.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        else
        {
            // Business object data version is not specified, so get the latest one regardless of the business object data status in the specified storage.
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectDataVersionSubQuery(builder, criteria, businessObjectDataEntity, businessObjectFormatEntity, null, storageEntity);

            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.in(businessObjectDataEntity.get(BusinessObjectDataEntity_.version)).value(subQuery));
        }

        // Add an inclusive upper bound partition value restriction if specified.
        if (upperBoundPartitionValue != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.lessThanOrEqualTo(businessObjectDataEntity.get(singleValuedAttribute), upperBoundPartitionValue));
        }

        // Add an inclusive lower bound partition value restriction if specified.
        if (lowerBoundPartitionValue != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.greaterThanOrEqualTo(businessObjectDataEntity.get(singleValuedAttribute), lowerBoundPartitionValue));
        }

        // Add a storage name restriction to the query where clause.
        mainQueryRestriction =
            builder.and(mainQueryRestriction, builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase()));

        criteria.select(partitionValue).where(mainQueryRestriction);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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
                    storageEntity);

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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDataEntity> getBusinessObjectDataFromStorageOlderThan(String storageName, int thresholdMinutes,
        List<String> businessObjectDataStatusesToIgnore)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataEntity> criteria = builder.createQuery(BusinessObjectDataEntity.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntity = businessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);
        Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> businessObjectDataStatusEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.status);

        // Compute threshold timestamp based on the current database timestamp and threshold minutes.
        Timestamp thresholdTimestamp = subtractMinutes(getCurrentTimestamp(), thresholdMinutes);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());
        queryRestriction = builder.and(queryRestriction,
            builder.not(businessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code).in(businessObjectDataStatusesToIgnore)));
        queryRestriction =
            builder.and(queryRestriction, builder.lessThanOrEqualTo(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn), thresholdTimestamp));

        // Order the results by file path.
        Order orderByCreatedOn = builder.asc(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn));

        // Add the clauses for the query.
        criteria.select(businessObjectDataEntity).where(queryRestriction).orderBy(orderByCreatedOn);

        return entityManager.createQuery(criteria).getResultList();
    }

    /**
     * Builds a sub-query to select the maximum business object data version.
     *
     * @param builder the criteria builder
     * @param criteria the criteria query
     * @param businessObjectDataEntity the business object data entity that appears in the from clause of the main query
     * @param businessObjectFormatEntity the business object format entity that appears in the from clause of the main query
     * @param businessObjectDataStatus the business object data status
     *
     * @return the sub-query to select the maximum business object data version
     */
    private Subquery<Integer> getMaximumBusinessObjectDataVersionSubQuery(CriteriaBuilder builder, CriteriaQuery<?> criteria,
        From<?, BusinessObjectDataEntity> businessObjectDataEntity, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        String businessObjectDataStatus, From<?, StorageEntity> storageEntity)
    {
        // Business object data version is not specified, so get the latest one in the specified storage.
        Subquery<Integer> subQuery = criteria.subquery(Integer.class);

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subQuery.from(BusinessObjectDataEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> subStorageUnitEntity = subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<StorageUnitEntity, StorageEntity> subStorageEntity = subStorageUnitEntity.join(StorageUnitEntity_.storage);
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

        // Create and add a standard restriction on storage.
        subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subStorageEntity, storageEntity));

        subQuery.select(builder.max(subBusinessObjectDataEntity.get(BusinessObjectDataEntity_.version))).where(subQueryRestriction);

        return subQuery;
    }

    /**
     * Builds a query restriction predicate for the specified entities as per business object data key values.
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
    private Predicate getQueryRestriction(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
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
     * Builds a query restriction predicate for the specified business object data entity as per primary and sub-partition values in the business object data
     * key.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param businessObjectDataKey the business object data key
     *
     * @return the query restriction predicate
     */
    private Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Create a standard restriction on primary partition value.
        Predicate predicate = builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.partitionValue), businessObjectDataKey.getPartitionValue());

        // Create and add standard restrictions on sub-partition values. Please note that the subpartition value columns are nullable.
        int subPartitionValuesCount = getSubPartitionValuesCount(businessObjectDataKey);
        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            predicate = builder.and(predicate, i < subPartitionValuesCount ?
                builder.equal(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i)), businessObjectDataKey.getSubPartitionValues().get(i)) :
                builder.isNull(businessObjectDataEntity.get(BUSINESS_OBJECT_DATA_SUBPARTITIONS.get(i))));
        }

        return predicate;
    }

    /**
     * Builds a query restriction predicate for the sub-query business object data entity as per partition values from the specified main query business object
     * data entity.
     *
     * @param builder the criteria builder
     * @param subBusinessObjectDataEntity the sub-query business object data entity that appears in the from clause
     * @param mainBusinessObjectDataEntity the main query business object data entity that appears in the from clause
     *
     * @return the query restriction predicate
     */
    private Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> subBusinessObjectDataEntity,
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
     * Builds a query restriction predicate for the sub-query business object data entity as per partition values from the specified main query business object
     * data entity.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls.
     *
     * @return the query restriction predicate
     */
    private Predicate getQueryRestrictionOnPartitionValues(CriteriaBuilder builder, From<?, BusinessObjectDataEntity> businessObjectDataEntity,
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
     * Builds query restriction predicates and adds them to the query restriction as per specified business object data version and status. If a business object
     * data version is specified, the business object data status is ignored.  When both business object data version and business object data status are not
     * specified, the sub-query restriction is not getting updated.
     *
     * @param builder the criteria builder
     * @param businessObjectDataEntity the business object data entity that appears in the from clause
     * @param businessObjectDataStatusEntity the business object data status entity that appears in the from clause
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the query restriction predicate or null if both business object data version and business object data status are not specified
     */
    private Predicate getQueryRestrictionOnBusinessObjectDataVersionAndStatus(CriteriaBuilder builder,
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

    private Timestamp getCurrentTimestamp()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Timestamp> criteria = builder.createQuery(Timestamp.class);

        // Add the clauses for the query.
        criteria.select(builder.currentTimestamp()).from(ConfigurationEntity.class);

        return entityManager.createQuery(criteria).getSingleResult();
    }

    private Timestamp subtractMinutes(Timestamp timestamp, int minutes)
    {
        long current = timestamp.getTime();
        long result = current - minutes * 60L * 1000L;
        return new Timestamp(result);
    }

    // StoragePlatform

    /**
     * {@inheritDoc}
     */
    @Override
    public StoragePlatformEntity getStoragePlatformByName(String name)
    {
        Map<String, Object> params = new HashMap<>();
        params.put(StoragePlatformEntity.COLUMN_NAME, name);
        return queryUniqueByNamedQueryAndNamedParams(StoragePlatformEntity.QUERY_GET_STORAGE_PLATFORM_BY_NAME, params);
    }

    // Storage

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageKey> getStorages()
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

    // StorageUnit

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageUnitEntity getStorageUnitByStorageNameAndDirectoryPath(String storageName, String directoryPath)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitEntity> criteria = builder.createQuery(StorageUnitEntity.class);

        // The criteria root is the storage units.
        Root<StorageUnitEntity> storageUnitEntity = criteria.from(StorageUnitEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate storageNameRestriction = builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageName.toUpperCase());
        Predicate directoryPathRestriction = builder.equal(storageUnitEntity.get(StorageUnitEntity_.directoryPath), directoryPath);

        criteria.select(storageUnitEntity).where(builder.and(storageNameRestriction, directoryPathRestriction));

        List<StorageUnitEntity> resultList = entityManager.createQuery(criteria).getResultList();

        // Return the first found storage unit or null if none were found.
        return resultList.size() >= 1 ? resultList.get(0) : null;
    }

    /**
     * {@inheritDoc}
     */
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

    // StorageFile

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageFileEntity> getStorageFileEntities(String storageName, String filePathPrefix)
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

        return entityManager.createQuery(criteria).getResultList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageFileEntity> getStorageFilesByStorageAndBusinessObjectData(StorageEntity storageEntity,
        List<BusinessObjectDataEntity> businessObjectDataEntities)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageFileEntity> criteria = builder.createQuery(StorageFileEntity.class);

        // The criteria root is the storage files.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(storageUnitEntity.get(StorageUnitEntity_.storage), storageEntity);
        queryRestriction = builder
            .and(queryRestriction, getPredicateForInClause(builder, storageUnitEntity.get(StorageUnitEntity_.businessObjectData), businessObjectDataEntities));

        // Order the results by storage file path.
        Order orderBy = builder.asc(storageFileEntity.get(StorageFileEntity_.path));

        // Add the clauses for the query.
        criteria.select(storageFileEntity).where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    // StorageUploadStatistics

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange)
    {
        // TODO: Remove Oracle specific code once we migrate away from Oracle.
        switch (Database.valueOf(configurationHelper.getProperty(ConfigurationValue.DATABASE_TYPE)))
        {
            case ORACLE:
                return getStorageUploadStatsOracle(storageAlternateKey, dateRange);
            default:
                return getStorageUploadStatsDatabaseAgnostic(storageAlternateKey, dateRange);
        }
    }

    /**
     * TODO: Make this method the main body of getStorageUploadStats once we migrate away from Oracle getStorageUploadStats TODO:    that leverages the storage
     * file view to query. This method is meant to be database agnostic.
     *
     * @param storageAlternateKey the storage alternate key
     * @param dateRange the date range
     *
     * @return the upload statistics
     */
    private StorageDailyUploadStats getStorageUploadStatsDatabaseAgnostic(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        Root<StorageFileViewEntity> storageFileViewEntity = criteria.from(StorageFileViewEntity.class);

        Path<Date> createdDate = storageFileViewEntity.get(StorageFileViewEntity_.createdDate);

        Expression<Long> totalFilesExpression = builder.count(storageFileViewEntity.get(StorageFileViewEntity_.storageFileId));
        Expression<Long> totalBytesExpression = builder.sum(storageFileViewEntity.get(StorageFileViewEntity_.fileSizeInBytes));

        Predicate storageNameRestriction =
            builder.equal(builder.upper(storageFileViewEntity.get(StorageFileViewEntity_.storageCode)), storageAlternateKey.getStorageName().toUpperCase());
        Predicate createDateRestriction =
            builder.and(builder.greaterThanOrEqualTo(createdDate, dateRange.getLowerDate()), builder.lessThanOrEqualTo(createdDate, dateRange.getUpperDate()));

        criteria.multiselect(createdDate, totalFilesExpression, totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        List<Expression<?>> grouping = new ArrayList<>();
        grouping.add(createdDate);

        criteria.groupBy(grouping);

        criteria.orderBy(builder.asc(createdDate));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageDailyUploadStats uploadStats = new StorageDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageDailyUploadStat uploadStat = new StorageDailyUploadStat();
            uploadStats.getStorageDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(DmDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }

    /**
     * TODO: Remove this method once we migrate away from Oracle getStorageUploadStats that uses Oracle specific 'trunc' function.
     *
     * @param storageAlternateKey the storage alternate key
     * @param dateRange the date range
     *
     * @return the upload statistics
     */
    private StorageDailyUploadStats getStorageUploadStatsOracle(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage file.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);

        // Create paths.
        Expression<Date> truncCreatedOnDateExpression = builder.function("trunc", Date.class, storageFileEntity.get(StorageFileEntity_.createdOn));
        Expression<Long> totalFilesExpression = builder.count(storageFileEntity.get(StorageFileEntity_.id));
        Expression<Long> totalBytesExpression = builder.sum(storageFileEntity.get(StorageFileEntity_.fileSizeBytes));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate storageNameRestriction =
            builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageAlternateKey.getStorageName().toUpperCase());
        Predicate createDateRestriction = builder.and(builder.greaterThanOrEqualTo(truncCreatedOnDateExpression, dateRange.getLowerDate()),
            builder.lessThanOrEqualTo(truncCreatedOnDateExpression, dateRange.getUpperDate()));

        criteria.multiselect(truncCreatedOnDateExpression, totalFilesExpression, totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        // Create the group by clause.
        List<Expression<?>> grouping = new ArrayList<>();

        grouping.add(truncCreatedOnDateExpression);
        criteria.groupBy(grouping);

        // Create the order by clause.
        criteria.orderBy(builder.asc(truncCreatedOnDateExpression));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageDailyUploadStats uploadStats = new StorageDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageDailyUploadStat uploadStat = new StorageDailyUploadStat();
            uploadStats.getStorageDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(DmDateUtils.getXMLGregorianCalendarValue(tuple.get(truncCreatedOnDateExpression)));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        DateRangeDto dateRange)
    {
        // TODO: Remove Oracle specific code once we migrate away from Oracle.
        switch (Database.valueOf(configurationHelper.getProperty(ConfigurationValue.DATABASE_TYPE)))
        {
            case ORACLE:
                return getStorageUploadStatsByBusinessObjectDefinitionOracle(storageAlternateKey, dateRange);
            default:
                return getStorageUploadStatsByBusinessObjectDefinitionDatabaseAgnostic(storageAlternateKey, dateRange);
        }
    }

    /**
     * TODO: Make this method the main body of getStorageUploadStatsByBusinessObjectDefinition once we migrate away from Oracle TODO:
     * getStorageUploadStatsByBusinessObjectDefinition that uses storage file view. This method is meant to be database agnostic.
     *
     * @param storageAlternateKey the storage alternate key
     * @param dateRange the date range
     *
     * @return the upload statistics
     */
    private StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinitionDatabaseAgnostic(
        StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        Root<StorageFileViewEntity> storageFileViewEntity = criteria.from(StorageFileViewEntity.class);

        Path<String> namespaceCode = storageFileViewEntity.get(StorageFileViewEntity_.namespaceCode);
        Path<String> dataProviderCode = storageFileViewEntity.get(StorageFileViewEntity_.dataProviderCode);
        Path<String> businessObjectDefinitionName = storageFileViewEntity.get(StorageFileViewEntity_.businessObjectDefinitionName);
        Path<Date> createdDate = storageFileViewEntity.get(StorageFileViewEntity_.createdDate);
        Expression<Long> totalFilesExpression = builder.count(storageFileViewEntity.get(StorageFileViewEntity_.storageFileId));
        Expression<Long> totalBytesExpression = builder.sum(storageFileViewEntity.get(StorageFileViewEntity_.fileSizeInBytes));

        Predicate storageNameRestriction =
            builder.equal(builder.upper(storageFileViewEntity.get(StorageFileViewEntity_.storageCode)), storageAlternateKey.getStorageName().toUpperCase());
        Predicate createDateRestriction =
            builder.and(builder.greaterThanOrEqualTo(createdDate, dateRange.getLowerDate()), builder.lessThanOrEqualTo(createdDate, dateRange.getUpperDate()));

        criteria.multiselect(createdDate, namespaceCode, dataProviderCode, businessObjectDefinitionName, totalFilesExpression, totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        // Create the group by clause.
        List<Expression<?>> grouping = new ArrayList<>();
        grouping.add(createdDate);
        grouping.add(namespaceCode);
        grouping.add(dataProviderCode);
        grouping.add(businessObjectDefinitionName);
        criteria.groupBy(grouping);

        // Create the order by clause.
        criteria.orderBy(builder.asc(createdDate), builder.asc(namespaceCode), builder.asc(dataProviderCode), builder.asc(businessObjectDefinitionName));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageBusinessObjectDefinitionDailyUploadStats uploadStats = new StorageBusinessObjectDefinitionDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageBusinessObjectDefinitionDailyUploadStat uploadStat = new StorageBusinessObjectDefinitionDailyUploadStat();
            uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(DmDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
            uploadStat.setNamespace(tuple.get(namespaceCode));
            uploadStat.setDataProviderName(tuple.get(dataProviderCode));
            uploadStat.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionName));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }

    /**
     * TODO: Remove this method once we migrate away from Oracle getStorageUploadStatsByBusinessObjectDefinition that uses Oracle specific 'trunc' function.
     *
     * @param storageAlternateKey the storage alternate key
     * @param dateRange the date range
     *
     * @return the upload statistics
     */
    private StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinitionOracle(StorageAlternateKeyDto storageAlternateKey,
        DateRangeDto dateRange)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage file.
        Root<StorageFileEntity> storageFileEntity = criteria.from(StorageFileEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);
        Join<StorageUnitEntity, StorageEntity> storageEntity = storageUnitEntity.join(StorageUnitEntity_.storage);
        Join<StorageUnitEntity, BusinessObjectDataEntity> businessObjectDataEntity = storageUnitEntity.join(StorageUnitEntity_.businessObjectData);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, DataProviderEntity> dataProviderEntity =
            businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.dataProvider);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create paths and expressions.
        Path<String> namespacePath = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> dataProviderNamePath = dataProviderEntity.get(DataProviderEntity_.name);
        Path<String> businessObjectDefinitionNamePath = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);
        Expression<Date> truncCreatedOnDateExpression = builder.function("trunc", Date.class, storageFileEntity.get(StorageFileEntity_.createdOn));
        Expression<Long> totalFilesExpression = builder.count(storageFileEntity.get(StorageFileEntity_.id));
        Expression<Long> totalBytesExpression = builder.sum(storageFileEntity.get(StorageFileEntity_.fileSizeBytes));

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate storageNameRestriction =
            builder.equal(builder.upper(storageEntity.get(StorageEntity_.name)), storageAlternateKey.getStorageName().toUpperCase());
        Predicate createDateRestriction = builder.and(builder.greaterThanOrEqualTo(truncCreatedOnDateExpression, dateRange.getLowerDate()),
            builder.lessThanOrEqualTo(truncCreatedOnDateExpression, dateRange.getUpperDate()));

        criteria.multiselect(truncCreatedOnDateExpression, namespacePath, dataProviderNamePath, businessObjectDefinitionNamePath, totalFilesExpression,
            totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        // Create the group by clause.
        List<Expression<?>> grouping = new ArrayList<>();

        grouping.add(truncCreatedOnDateExpression);
        grouping.add(namespacePath);
        grouping.add(dataProviderNamePath);
        grouping.add(businessObjectDefinitionNamePath);
        criteria.groupBy(grouping);

        // Create the order by clause.
        criteria.orderBy(builder.asc(truncCreatedOnDateExpression), builder.asc(namespacePath), builder.asc(dataProviderNamePath),
            builder.asc(businessObjectDefinitionNamePath));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageBusinessObjectDefinitionDailyUploadStats uploadStats = new StorageBusinessObjectDefinitionDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageBusinessObjectDefinitionDailyUploadStat uploadStat = new StorageBusinessObjectDefinitionDailyUploadStat();
            uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(DmDateUtils.getXMLGregorianCalendarValue(tuple.get(truncCreatedOnDateExpression)));
            uploadStat.setNamespace(tuple.get(namespacePath));
            uploadStat.setDataProviderName(tuple.get(dataProviderNamePath));
            uploadStat.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNamePath));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }

    // JobDefinition

    /**
     * {@inheritDoc}
     */
    @Override
    public JobDefinitionEntity getJobDefinitionByAltKey(String namespace, String jobName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinition = criteria.from(JobDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<JobDefinitionEntity, NamespaceEntity> namespaceJoin = jobDefinition.join(JobDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate namespaceRestriction = builder.equal(builder.upper(namespaceJoin.get(NamespaceEntity_.code)), namespace.toUpperCase());
        Predicate jobNameRestriction = builder.equal(builder.upper(jobDefinition.get(JobDefinitionEntity_.name)), jobName.toUpperCase());

        criteria.select(jobDefinition).where(builder.and(namespaceRestriction, jobNameRestriction));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one Activiti job definition with parameters {namespace=\"%s\", jobName=\"%s\"}.", namespace, jobName));
    }

    // EmrClusterDefinition

    /**
     * {@inheritDoc}
     */
    @Override
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(EmrClusterDefinitionKey emrClusterDefinitionKey)
    {
        return getEmrClusterDefinitionByAltKey(emrClusterDefinitionKey.getNamespace(), emrClusterDefinitionKey.getEmrClusterDefinitionName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(String namespaceCd, String definitionName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<EmrClusterDefinitionEntity> criteria = builder.createQuery(EmrClusterDefinitionEntity.class);

        // The criteria root is the EMR cluster definition.
        Root<EmrClusterDefinitionEntity> emrClusterDefinition = criteria.from(EmrClusterDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<EmrClusterDefinitionEntity, NamespaceEntity> namespace = emrClusterDefinition.join(EmrClusterDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate namespaceRestriction = builder.equal(builder.upper(namespace.get(NamespaceEntity_.code)), namespaceCd.toUpperCase());
        Predicate definitionNameRestriction =
            builder.equal(builder.upper(emrClusterDefinition.get(EmrClusterDefinitionEntity_.name)), definitionName.toUpperCase());

        criteria.select(emrClusterDefinition).where(builder.and(namespaceRestriction, definitionNameRestriction));

        return executeSingleResultQuery(criteria, String
            .format("Found more than one EMR cluster definition with parameters {namespace=\"%s\", clusterDefinitionName=\"%s\"}.", namespace, definitionName));
    }

    // NotificationEvent

    /**
     * {@inheritDoc}
     */
    @Override
    public NotificationEventTypeEntity getNotificationEventTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NotificationEventTypeEntity> criteria = builder.createQuery(NotificationEventTypeEntity.class);

        // The criteria root is the notification event type.
        Root<NotificationEventTypeEntity> notificationEventTypeEntity = criteria.from(NotificationEventTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(notificationEventTypeEntity.get(NotificationEventTypeEntity_.code)), code.toUpperCase());

        criteria.select(notificationEventTypeEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one notification event type with code \"%s\".", code));
    }

    // BusinessObjectDataNotification

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationByAltKey(BusinessObjectDataNotificationRegistrationKey key)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataNotificationRegistrationEntity> criteria = builder.createQuery(BusinessObjectDataNotificationRegistrationEntity.class);

        // The criteria root is the business object data notification registration entity.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), key.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder
            .equal(builder.upper(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.name)),
                key.getNotificationName().toUpperCase()));

        criteria.select(businessObjectDataNotificationEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one business object data notification registration with with parameters {namespace=\"%s\", notificationName=\"%s\"}.",
                key.getNamespace(), key.getNotificationName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDataNotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeys(String namespaceCode)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data notification.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> notificationNameColumn = businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespaceCode.toUpperCase());

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, notificationNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(notificationNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectDataNotificationRegistrationKey> businessObjectDataNotificationKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey = new BusinessObjectDataNotificationRegistrationKey();
            businessObjectDataNotificationKeys.add(businessObjectDataNotificationKey);
            businessObjectDataNotificationKey.setNamespace(tuple.get(namespaceCodeColumn));
            businessObjectDataNotificationKey.setNotificationName(tuple.get(notificationNameColumn));
        }

        return businessObjectDataNotificationKeys;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataNotificationRegistrationEntity> criteria = builder.createQuery(BusinessObjectDataNotificationRegistrationEntity.class);

        // The criteria root is the business object data notification registration entity.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, NotificationEventTypeEntity> notificationEventTypeEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.notificationEventType);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> businessObjectDefinitionNamespaceEntity =
            businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, FileTypeEntity> fileTypeEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.fileType, JoinType.LEFT);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(notificationEventTypeEntity.get(NotificationEventTypeEntity_.code)), notificationEventTypeCode.toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder
            .equal(builder.upper(businessObjectDefinitionNamespaceEntity.get(NamespaceEntity_.code)), businessObjectDataKey.getNamespace().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder
            .or(builder.isNull(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.usage)), builder
                .equal(builder.upper(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.usage)),
                    businessObjectDataKey.getBusinessObjectFormatUsage().toUpperCase())));
        queryRestriction = builder.and(queryRestriction, builder
            .or(builder.isNull(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.fileType)),
                builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectDataKey.getBusinessObjectFormatFileType().toUpperCase())));
        queryRestriction = builder.and(queryRestriction, builder
            .or(builder.isNull(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion)), builder
                .equal(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion),
                    businessObjectDataKey.getBusinessObjectFormatVersion())));

        // Order the results by namespace and notification name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntity.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.name)));

        // Add the clauses for the query.
        criteria.select(businessObjectDataNotificationEntity).where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    // SecurityFunction

    /**
     * {@inheritDoc}
     */
    @Override
    @Cacheable(DaoSpringModuleConfig.DM_CACHE_NAME)
    public List<String> getSecurityFunctionsForRole(String roleCd)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        // CriteriaQuery<Tuple> criteria = builder.createTupleQuery();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object definition.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntity = criteria.from(SecurityRoleFunctionEntity.class);

        // Join to the other tables we can filter on.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntity = securityRoleFunctionEntity.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntity =
            securityRoleFunctionEntity.join(SecurityRoleFunctionEntity_.securityFunction);

        // Get the columns.
        Path<String> functionCodeColumn = securityFunctionEntity.get(SecurityFunctionEntity_.code);

        // Add the select clause.
        criteria.select(functionCodeColumn);

        criteria.where(builder.equal(builder.upper(securityRoleEntity.get(SecurityRoleEntity_.code)), roleCd.toUpperCase()));

        // Add the order by clause.
        criteria.orderBy(builder.asc(functionCodeColumn));

        // Run the query to get a list of tuples back.
        return entityManager.createQuery(criteria).getResultList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Cacheable(DaoSpringModuleConfig.DM_CACHE_NAME)
    public List<String> getSecurityFunctions()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object definition.
        Root<SecurityFunctionEntity> securityFunctionEntity = criteria.from(SecurityFunctionEntity.class);

        // Get the columns.
        Path<String> functionCodeColumn = securityFunctionEntity.get(SecurityFunctionEntity_.code);

        // Add the select clause.
        criteria.select(functionCodeColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(functionCodeColumn));

        // Run the query to get a list of tuples back.
        return entityManager.createQuery(criteria).getResultList();
    }

    // JmsMessage

    /**
     * {@inheritDoc}
     */
    @Override
    public JmsMessageEntity getOldestJmsMessage()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JmsMessageEntity> criteria = builder.createQuery(JmsMessageEntity.class);

        // The criteria root is the jms message.
        Root<JmsMessageEntity> jmsMessageEntity = criteria.from(JmsMessageEntity.class);

        // Add the select clause.
        criteria.select(jmsMessageEntity);

        // Add the order by clause, since we want to return only the oldest JMS message (a message with the smallest sequence generated id).
        criteria.orderBy(builder.asc(jmsMessageEntity.get(JmsMessageEntity_.id)));

        // Execute the query and ask it to return only the first row.
        List<JmsMessageEntity> resultList = entityManager.createQuery(criteria).setMaxResults(1).getResultList();

        // Return the result.
        return resultList.size() > 0 ? resultList.get(0) : null;
    }

    // OnDemandPricing

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation uses JPA criteria, and throws exception when more than 1 on-demand price is found for the specified parameters.
     */
    @Override
    public OnDemandPriceEntity getOnDemandPrice(String region, String instanceType)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<OnDemandPriceEntity> criteriaQuery = criteriaBuilder.createQuery(OnDemandPriceEntity.class);

        Root<OnDemandPriceEntity> onDemandPrice = criteriaQuery.from(OnDemandPriceEntity.class);
        Path<String> regionPath = onDemandPrice.get(OnDemandPriceEntity_.region);
        Path<String> instanceTypePath = onDemandPrice.get(OnDemandPriceEntity_.instanceType);

        Predicate regionEquals = criteriaBuilder.equal(regionPath, region);
        Predicate instanceTypeEquals = criteriaBuilder.equal(instanceTypePath, instanceType);

        criteriaQuery.select(onDemandPrice).where(regionEquals, instanceTypeEquals);

        return executeSingleResultQuery(criteriaQuery,
            "More than 1 on-demand price found with given region '" + region + "' and instance type '" + instanceType + "'.");
    }

    // Other methods

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
            String username = dmDaoSecurityHelper.getCurrentUsername();

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

    /**
     * Executes query, validates if result list contains no more than record and returns the query result.
     *
     * @param criteria the criteria select query to be executed
     * @param message the exception message to use if the query returns fails
     *
     * @return the query result or null if 0 records were selected
     */
    private <T> T executeSingleResultQuery(CriteriaQuery<T> criteria, String message)
    {
        List<T> resultList = entityManager.createQuery(criteria).getResultList();

        // Validate that the query returned no more than one record.
        Validate.isTrue(resultList.size() < 2, message);

        return resultList.size() == 1 ? resultList.get(0) : null;
    }

    /**
     * Gets a business object format key from the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the newly created business object format key
     */
    private static BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectDataKey businessObjectDataKey)
    {
        return new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion());
    }

    /**
     * Returns number of sub-partition values in the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the number of sub-partition values in the business object data key
     */
    private static int getSubPartitionValuesCount(BusinessObjectDataKey businessObjectDataKey)
    {
        return businessObjectDataKey.getSubPartitionValues() == null ? 0 : businessObjectDataKey.getSubPartitionValues().size();
    }
}
