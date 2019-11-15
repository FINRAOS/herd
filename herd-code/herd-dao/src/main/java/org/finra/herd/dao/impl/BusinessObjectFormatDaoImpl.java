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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity_;

@Repository
public class BusinessObjectFormatDaoImpl extends AbstractHerdDao implements BusinessObjectFormatDao
{
    @Autowired
    private FileTypeDao fileTypeDao;

    @Autowired
    private NamespaceDao namespaceDao;

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

    @Override
    public Long getBusinessObjectFormatCountByPartitionKeyGroup(PartitionKeyGroupEntity partitionKeyGroupEntity)
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

    @Override
    public Long getBusinessObjectFormatCountByPartitionKeys(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, List<String> partitionKeys)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteria = builder.createQuery(Long.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntityRoot = criteria.from(BusinessObjectFormatEntity.class);

        // Create path.
        Expression<Long> businessObjectFormatRecordCount = builder.count(businessObjectFormatEntityRoot);

        // Namespace is a required parameter, so fetch the relative entity to optimize the main query.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);

        // If specified namespace does not exist, then return a zero record count.
        if (namespaceEntity == null)
        {
            return 0L;
        }

        // If file type is specified, fetch the relative entity to optimize the main query.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectFormatFileType);

            // If specified file type does not exist, then return a zero record count.
            if (fileTypeEntity == null)
            {
                return 0L;
            }
        }

        // Join to the other tables we can filter on.
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntityRoot.join(BusinessObjectFormatEntity_.businessObjectDefinition);

        // Create main query restrictions based on the specified parameters.
        List<Predicate> predicates = new ArrayList<>();

        // Create restriction on namespace code and business object definition name.
        predicates.add(builder.equal(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.namespace), namespaceEntity));
        predicates.add(
            builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)), businessObjectDefinitionName.toUpperCase()));

        // If specified, create restriction on business object format usage.
        if (!StringUtils.isEmpty(businessObjectFormatUsage))
        {
            predicates.add(
                builder.equal(builder.upper(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.usage)), businessObjectFormatUsage.toUpperCase()));
        }

        // If specified, create restriction on business object format file type.
        if (fileTypeEntity != null)
        {
            predicates.add(builder.equal(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.fileTypeCode), fileTypeEntity.getCode()));
        }

        // If specified, create restriction on business object format version.
        if (businessObjectFormatVersion != null)
        {
            predicates
                .add(builder.equal(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.businessObjectFormatVersion), businessObjectFormatVersion));
        }

        // If specified, create restriction on partition keys.
        if (CollectionUtils.isNotEmpty(partitionKeys))
        {
            for (String partitionKey : partitionKeys)
            {
                // Add restriction on partition key (partition column).
                // Partition key must identify a partition column that is at partition level that could be explicitly registered.
                // Partition level uses one-based numbering.
                Join<BusinessObjectFormatEntity, SchemaColumnEntity> schemaColumnEntityJoin =
                    businessObjectFormatEntityRoot.join(BusinessObjectFormatEntity_.schemaColumns);
                predicates.add(builder.equal(builder.upper(schemaColumnEntityJoin.get(SchemaColumnEntity_.name)), partitionKey.toUpperCase()));
                predicates.add(builder.isNotNull(schemaColumnEntityJoin.get(SchemaColumnEntity_.partitionLevel)));
                predicates
                    .add(builder.lessThan(schemaColumnEntityJoin.get(SchemaColumnEntity_.partitionLevel), BusinessObjectDataEntity.MAX_SUBPARTITIONS + 2));
            }
        }

        // Add all clauses for the query.
        criteria.select(businessObjectFormatRecordCount).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).distinct(true);

        // Execute the query and return the result.
        return entityManager.createQuery(criteria).getSingleResult();
    }

    @Override
    public List<Integer> getBusinessObjectFormatIdsByBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Integer> criteria = builder.createQuery(Integer.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntityRoot = criteria.from(BusinessObjectFormatEntity.class);

        // Create path.
        Expression<Integer> businessObjectFormatIdColumn = businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.id);

        // Create standard restrictions.
        Predicate predicate =
            builder.equal(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.businessObjectDefinitionId), businessObjectDefinitionEntity.getId());

        // Build an order by clause.
        Order orderBy = builder.asc(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.id));

        // Add all clauses to the query.
        criteria.select(businessObjectFormatIdColumn).where(predicate).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }

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

    @Override
    public List<BusinessObjectFormatKey> getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        boolean latestBusinessObjectFormatVersion)
    {
        return getBusinessObjectFormatsWithFilters(businessObjectDefinitionKey, null, latestBusinessObjectFormatVersion);
    }

    @Override
    public List<BusinessObjectFormatKey> getBusinessObjectFormatsWithFilters(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        String businessObjectFormatUsage, boolean latestBusinessObjectFormatVersion)
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

        // Add the business object format usage where parameter is not empty
        if (StringUtils.isNotEmpty(businessObjectFormatUsage))
        {
            queryRestriction = builder.and(queryRestriction,
                builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)), businessObjectFormatUsage.toUpperCase()));
        }

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

    @Override
    public List<BusinessObjectFormatEntity> getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectFormatEntity> criteria = builder.createQuery(BusinessObjectFormatEntity.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntityRoot = criteria.from(BusinessObjectFormatEntity.class);

        // Create restriction on business object definition.
        Predicate queryRestriction =
            builder.equal(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.businessObjectDefinitionId), businessObjectDefinitionEntity.getId());

        // Add restriction on the latest business object format version flag.
        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.latestVersion), true));

        // Add order by clause.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.usage)));
        orderBy.add(builder.asc(businessObjectFormatEntityRoot.get(BusinessObjectFormatEntity_.fileTypeCode)));

        // Add all clauses to the query.
        criteria.where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
