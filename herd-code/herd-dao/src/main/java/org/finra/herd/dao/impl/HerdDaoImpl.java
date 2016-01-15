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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStat;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageDailyUploadStat;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.DateRangeDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.AuditableEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.CustomDdlEntity_;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.DataProviderEntity_;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity_;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.model.jpa.JmsMessageEntity_;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity_;
import org.finra.herd.model.jpa.OnDemandPriceEntity;
import org.finra.herd.model.jpa.OnDemandPriceEntity_;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity_;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityFunctionEntity_;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity_;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity_;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageEntity_;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageFileEntity_;
import org.finra.herd.model.jpa.StorageFileViewEntity;
import org.finra.herd.model.jpa.StorageFileViewEntity_;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity_;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity_;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity_;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitEntity_;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity_;

/**
 * The herd DAO implementation.
 */
// TODO: This class is too big and should be split up into smaller classes (e.g. NamespaceDao, StorageDao, etc.).
// TODO:     When this is fixed, we can remove the PMD suppress warning below.
@SuppressWarnings({"PMD.ExcessivePublicCount", "PMD.ExcessiveClassLength"})
@Repository
public class HerdDaoImpl extends BaseJpaDaoImpl implements HerdDao
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
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    // System

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getCurrentTimestamp()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Timestamp> criteria = builder.createQuery(Timestamp.class);

        // Add the clauses for the query.
        criteria.select(builder.currentTimestamp()).from(ConfigurationEntity.class);

        return entityManager.createQuery(criteria).getSingleResult();
    }

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
    public DataProviderEntity getDataProviderByKey(DataProviderKey dataProviderKey)
    {
        return getDataProviderByName(dataProviderKey.getDataProviderName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataProviderEntity getDataProviderByName(String dataProviderName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<DataProviderEntity> criteria = builder.createQuery(DataProviderEntity.class);

        // The criteria root is the data provider.
        Root<DataProviderEntity> dataProviderEntity = criteria.from(DataProviderEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(dataProviderEntity.get(DataProviderEntity_.name)), dataProviderName.toUpperCase());

        criteria.select(dataProviderEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one data provider with name=\"%s\".", dataProviderName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DataProviderKey> getDataProviders()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the data provider.
        Root<DataProviderEntity> dataProviderEntity = criteria.from(DataProviderEntity.class);

        // Get the columns.
        Path<String> dataProviderNameColumn = dataProviderEntity.get(DataProviderEntity_.name);

        // Add the select clause.
        criteria.select(dataProviderNameColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(dataProviderNameColumn));

        // Run the query to get a list of data provider names back.
        List<String> dataProviderNames = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned data provider names.
        List<DataProviderKey> dataProviderKeys = new ArrayList<>();
        for (String dataProviderName : dataProviderNames)
        {
            dataProviderKeys.add(new DataProviderKey(dataProviderName));
        }

        return dataProviderKeys;
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

    @Override
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(String namespace, String name)
    {
        return getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(namespace, name));
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
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, String upperBoundPartitionValue,
        String lowerBoundPartitionValue)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatus,
            storageNames, AggregateFunction.GREATEST, upperBoundPartitionValue, lowerBoundPartitionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames)
    {
        return getBusinessObjectDataPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion, businessObjectDataStatus,
            storageNames, AggregateFunction.LEAST, null, null);
    }

    /**
     * Retrieves partition value per specified parameters that includes the aggregate function.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status will be used for each partition value.
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     * @param storageNames the list of storages (case-insensitive)
     * @param aggregateFunction the aggregate function to use against partition values
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the partition value
     */
    private String getBusinessObjectDataPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, AggregateFunction aggregateFunction,
        String upperBoundPartitionValue, String lowerBoundPartitionValue)
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

        // If a data version was specified, use it.
        if (businessObjectDataVersion != null)
        {
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.equal(businessObjectDataEntity.get(BusinessObjectDataEntity_.version), businessObjectDataVersion));
        }
        // Business object data version is not specified, so get the latest one as per specified business object data status in the specified storage.
        else
        {
            Subquery<Integer> subQuery =
                getMaximumBusinessObjectDataVersionSubQuery(builder, criteria, businessObjectDataEntity, businessObjectFormatEntity, businessObjectDataStatus,
                    storageEntity);

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

        // Add a storage name restriction to the main query where clause.
        List<String> uppercaseStorageNames = new ArrayList<>();
        for (String storageName : storageNames)
        {
            uppercaseStorageNames.add(storageName.toUpperCase());
        }
        mainQueryRestriction = builder.and(mainQueryRestriction, builder.upper(storageEntity.get(StorageEntity_.name)).in(uppercaseStorageNames));

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
        Timestamp thresholdTimestamp = HerdDateUtils.addMinutes(getCurrentTimestamp(), -thresholdMinutes);

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
     * {@inheritDoc}
     */
    @Override
    public Map<BusinessObjectDataEntity, StoragePolicyEntity> getBusinessObjectDataEntitiesMatchingStoragePolicies(
        StoragePolicyPriorityLevel storagePolicyPriorityLevel, List<String> supportedBusinessObjectDataStatuses, int startPosition, int maxResult)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data.
        Root<BusinessObjectDataEntity> businessObjectDataEntity = criteria.from(BusinessObjectDataEntity.class);
        Root<StoragePolicyEntity> storagePolicyEntity = criteria.from(StoragePolicyEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataEntity, StorageUnitEntity> storageUnitEntity = businessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);
        Join<BusinessObjectDataEntity, BusinessObjectDataStatusEntity> businessObjectDataStatusEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.status);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntity =
            businessObjectDataEntity.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create main query restrictions based on the specified parameters.
        List<Predicate> mainQueryPredicates = new ArrayList<>();

        // Add a restriction on business object definition.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isBusinessObjectDefinitionIsNull() ?
            builder.isNull(storagePolicyEntity.get(StoragePolicyEntity_.businessObjectDefinition)) :
            builder.equal(businessObjectDefinitionEntity, storagePolicyEntity.get(StoragePolicyEntity_.businessObjectDefinition)));

        // Add a restriction on business object format usage.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isUsageIsNull() ? builder.isNull(storagePolicyEntity.get(StoragePolicyEntity_.usage)) : builder
            .equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
                builder.upper(storagePolicyEntity.get(StoragePolicyEntity_.usage))));

        // Add a restriction on business object format file type.
        mainQueryPredicates.add(storagePolicyPriorityLevel.isFileTypeIsNull() ? builder.isNull(storagePolicyEntity.get(StoragePolicyEntity_.fileType)) :
            builder.equal(fileTypeEntity, storagePolicyEntity.get(StoragePolicyEntity_.fileType)));

        // Add a restriction on storage policy filter storage.
        mainQueryPredicates.add(builder.equal(storageUnitEntity.get(StorageUnitEntity_.storage), storagePolicyEntity.get(StoragePolicyEntity_.storage)));

        // Add a restriction on supported business object data statuses.
        mainQueryPredicates.add(businessObjectDataStatusEntity.get(BusinessObjectDataStatusEntity_.code).in(supportedBusinessObjectDataStatuses));

        // Build a subquery to eliminate business object data instances that already have storage unit in the storage policy destination storage.
        Subquery<BusinessObjectDataEntity> subquery = criteria.subquery(BusinessObjectDataEntity.class);
        Root<BusinessObjectDataEntity> subBusinessObjectDataEntity = subquery.from(BusinessObjectDataEntity.class);
        subquery.select(subBusinessObjectDataEntity);

        // Join to the other tables we can filter on for the subquery.
        Join<BusinessObjectDataEntity, StorageUnitEntity> subStorageUnitEntity = subBusinessObjectDataEntity.join(BusinessObjectDataEntity_.storageUnits);

        // Create the subquery restrictions based on the main query and storage policy destination storage.
        List<Predicate> subQueryPredicates = new ArrayList<>();
        subQueryPredicates.add(builder.equal(subBusinessObjectDataEntity, businessObjectDataEntity));
        subQueryPredicates
            .add(builder.equal(subStorageUnitEntity.get(StorageUnitEntity_.storage), storagePolicyEntity.get(StoragePolicyEntity_.destinationStorage)));

        // Add all clauses to the subquery.
        subquery.select(subBusinessObjectDataEntity).where(subQueryPredicates.toArray(new Predicate[] {}));

        // Add a main query restriction based on the subquery to eliminate business object data
        // instances that already have storage unit in the storage policy destination storage.
        mainQueryPredicates.add(builder.not(builder.exists(subquery)));

        // Order the results by business object data "created on" value.
        Order orderByCreatedOn = builder.asc(businessObjectDataEntity.get(BusinessObjectDataEntity_.createdOn));

        // Add the select clause to the main query.
        criteria.multiselect(businessObjectDataEntity, storagePolicyEntity);

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
            if (!result.containsKey(tuple.get(businessObjectDataEntity)))
            {
                result.put(tuple.get(businessObjectDataEntity), tuple.get(storagePolicyEntity));
            }
        }

        return result;
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

    // StorageUnitStatus

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageUnitStatusEntity getStorageUnitStatusByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitStatusEntity> criteria = builder.createQuery(StorageUnitStatusEntity.class);

        // The criteria root is the storage unit status.
        Root<StorageUnitStatusEntity> storageUnitStatusEntity = criteria.from(StorageUnitStatusEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate storageUnitStatusCodeRestriction =
            builder.equal(builder.upper(storageUnitStatusEntity.get(StorageUnitStatusEntity_.code)), code.toUpperCase());

        criteria.select(storageUnitStatusEntity).where(storageUnitStatusCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one storage unit status with code \"%s\".", code));
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

    /**
     * {@inheritDoc}
     */
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
                    businessObjectDataEntity, businessObjectDataVersion, businessObjectDataStatus, storageEntity, selectOnlyAvailableStorageUnits);

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

        // If specified, add restriction on storage.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            // Add a storage name restriction to the main query where clause.
            List<String> uppercaseStorageNames = new ArrayList<>();
            for (String storageName : storageNames)
            {
                uppercaseStorageNames.add(storageName.toUpperCase());
            }
            mainQueryRestriction = builder.and(mainQueryRestriction, builder.upper(storageEntity.get(StorageEntity_.name)).in(uppercaseStorageNames));
        }
        else if (StringUtils.isNotBlank(storagePlatformType))
        {
            // Select storage units only from the storages of the specified storage platform type.
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.equal(storagePlatformEntity.get(StoragePlatformEntity_.name), storagePlatformType));
        }
        else if (StringUtils.isNotBlank(excludedStoragePlatformType))
        {
            // Ignore any storages of the excluded storage platform type.
            mainQueryRestriction =
                builder.and(mainQueryRestriction, builder.notEqual(storagePlatformEntity.get(StoragePlatformEntity_.name), excludedStoragePlatformType));
        }

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
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     *
     * @return the sub-query to select the maximum business object data version
     */
    private Subquery<Integer> getMaximumBusinessObjectFormatVersionSubQuery(CriteriaBuilder builder, CriteriaQuery<?> criteria,
        From<?, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity, From<?, BusinessObjectFormatEntity> businessObjectFormatEntity,
        From<?, FileTypeEntity> fileTypeEntity, From<?, BusinessObjectDataEntity> businessObjectDataEntity, Integer businessObjectDataVersion,
        String businessObjectDataStatus, From<?, StorageEntity> storageEntity, boolean selectOnlyAvailableStorageUnits)
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
        subQueryRestriction = builder.and(subQueryRestriction, builder.equal(subStorageEntity, storageEntity));

        // If specified, add a restriction on storage unit status availability flag.
        if (selectOnlyAvailableStorageUnits)
        {
            subQueryRestriction = builder.and(subQueryRestriction, builder.isTrue(subStorageUnitStatusEntity.get(StorageUnitStatusEntity_.available)));
        }

        // Add all of the clauses to the subquery.
        subQuery.select(builder.max(subBusinessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion))).where(subQueryRestriction);

        return subQuery;
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
    public List<StorageFileEntity> getStorageFilesByStorageAndFilePathPrefix(String storageName, String filePathPrefix)
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
    public MultiValuedMap<Integer, String> getStorageFilePathsByStorageUnits(List<StorageUnitEntity> storageUnitEntities)
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

        // Join to the other tables we can filter on.
        Join<StorageFileEntity, StorageUnitEntity> storageUnitEntity = storageFileEntity.join(StorageFileEntity_.storageUnit);

        // Get the columns.
        Path<Integer> storageUnitIdColumn = storageUnitEntity.get(StorageUnitEntity_.id);
        Path<String> storageFilePathColumn = storageFileEntity.get(StorageFileEntity_.path);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = getPredicateForInClause(builder, storageUnitEntity, storageUnitEntities);

        // Add the select clause.
        criteria.multiselect(storageUnitIdColumn, storageFilePathColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

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

    // StoragePolicyRuleType

    /**
     * {@inheritDoc}
     */
    @Override
    public StoragePolicyRuleTypeEntity getStoragePolicyRuleTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StoragePolicyRuleTypeEntity> criteria = builder.createQuery(StoragePolicyRuleTypeEntity.class);

        // The criteria root is the storage policy rule type.
        Root<StoragePolicyRuleTypeEntity> storagePolicyRuleTypeEntity = criteria.from(StoragePolicyRuleTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(storagePolicyRuleTypeEntity.get(StoragePolicyRuleTypeEntity_.code)), code.toUpperCase());

        criteria.select(storagePolicyRuleTypeEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one storage policy rule type with code \"%s\".", code));
    }

    // StoragePolicy

    /**
     * {@inheritDoc}
     */
    @Override
    public StoragePolicyEntity getStoragePolicyByAltKey(StoragePolicyKey key)
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
        queryRestriction = builder
            .and(queryRestriction, builder.equal(builder.upper(storagePolicyEntity.get(StoragePolicyEntity_.name)), key.getStoragePolicyName().toUpperCase()));

        criteria.select(storagePolicyEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one storage policy with with parameters {namespace=\"%s\", storagePolicyName=\"%s\"}.", key.getNamespace(),
                key.getStoragePolicyName()));
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
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
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
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(truncCreatedOnDateExpression)));
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
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
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
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(truncCreatedOnDateExpression)));
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

    @Override
    public List<JobDefinitionEntity> getJobDefinitionsByFilter(String namespace, String jobName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinition = criteria.from(JobDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<JobDefinitionEntity, NamespaceEntity> namespaceJoin = jobDefinition.join(JobDefinitionEntity_.namespace);

        List<Predicate> predicates = new ArrayList<>();

        // Create the restrictions (i.e. the where clauses).
        if (StringUtils.isNotBlank(namespace))
        {
            predicates.add(builder.equal(builder.upper(namespaceJoin.get(NamespaceEntity_.code)), namespace.toUpperCase()));
        }
        if (StringUtils.isNotBlank(jobName))
        {
            predicates.add(builder.equal(builder.upper(jobDefinition.get(JobDefinitionEntity_.name)), jobName.toUpperCase()));
        }

        criteria.select(jobDefinition);
        if (predicates.size() > 0)
        {
            criteria.where(builder.and(predicates.toArray(new Predicate[predicates.size()])));
        }

        return entityManager.createQuery(criteria).getResultList();
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

    // SecurityFunction

    /**
     * {@inheritDoc}
     */
    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
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
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
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
