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

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

@Repository
public class BusinessObjectFormatDaoImpl extends AbstractHerdDao implements BusinessObjectFormatDao
{
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
    public List<BusinessObjectFormatKey> getBusinessObjectFormatsWithFilters(
        BusinessObjectDefinitionKey businessObjectDefinitionKey, String businessObjectFormatUsage, boolean latestBusinessObjectFormatVersion)
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
        BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectFormatEntity> criteria = builder.createQuery(BusinessObjectFormatEntity.class);

        // The criteria root is the business object format.
        Root<BusinessObjectFormatEntity> businessObjectFormatEntity = criteria.from(BusinessObjectFormatEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectFormatEntity.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectDefinitionKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName().toUpperCase()));

        // Add the order by clause.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)));
        orderBy.add(builder.asc(fileTypeEntity.get(FileTypeEntity_.code)));

        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.latestVersion), true));

        criteria.orderBy(orderBy);
        // Add the where clause.
        criteria.where(queryRestriction);
        return entityManager.createQuery(criteria).getResultList();
    }
}
