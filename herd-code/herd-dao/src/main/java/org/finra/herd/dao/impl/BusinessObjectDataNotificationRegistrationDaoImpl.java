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
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity_;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity_;

@Repository
public class BusinessObjectDataNotificationRegistrationDaoImpl extends AbstractNotificationRegistrationDao
    implements BusinessObjectDataNotificationRegistrationDao
{
    @Override
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistrationByAltKey(NotificationRegistrationKey key)
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

    @Override
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeysByNamespace(String namespace)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data notification registration.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);

        // Get the columns.
        Path<String> notificationRegistrationNamespaceColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> notificationRegistrationNameColumn = businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespace.toUpperCase());

        // Add the select clause.
        criteria.multiselect(notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(notificationRegistrationNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the list of keys from the returned tuples.
        return getNotificationRegistrationKeys(tuples, notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn);
    }

    @Override
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data notification registration.
        Root<BusinessObjectDataNotificationRegistrationEntity> notificationRegistrationEntityRoot =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> notificationRegistrationNamespaceEntityJoin =
            notificationRegistrationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            notificationRegistrationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> businessObjectDefinitionNamespaceEntity =
            businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, FileTypeEntity> fileTypeEntity =
            notificationRegistrationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.fileType, JoinType.LEFT);

        // Get the columns.
        Path<String> notificationRegistrationNamespaceColumn = notificationRegistrationNamespaceEntityJoin.get(NamespaceEntity_.code);
        Path<String> notificationRegistrationNameColumn = notificationRegistrationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionNamespaceEntity.get(NamespaceEntity_.code)),
            businessObjectDataNotificationFilter.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataNotificationFilter.getBusinessObjectDefinitionName().toUpperCase()));
        if (StringUtils.isNotBlank(businessObjectDataNotificationFilter.getBusinessObjectFormatUsage()))
        {
            predicates.add(builder.or(builder.isNull(notificationRegistrationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.usage)), builder
                .equal(builder.upper(notificationRegistrationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.usage)),
                    businessObjectDataNotificationFilter.getBusinessObjectFormatUsage().toUpperCase())));
        }
        if (StringUtils.isNotBlank(businessObjectDataNotificationFilter.getBusinessObjectFormatFileType()))
        {
            predicates.add(builder.or(builder.isNull(notificationRegistrationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.fileType)),
                builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)),
                    businessObjectDataNotificationFilter.getBusinessObjectFormatFileType().toUpperCase())));
        }

        // Add the select and where clauses to the query.
        criteria.multiselect(notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn)
            .where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Add the order by clause to the query.
        criteria.orderBy(builder.asc(notificationRegistrationNamespaceColumn), builder.asc(notificationRegistrationNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the list of keys from the returned tuples.
        return getNotificationRegistrationKeys(tuples, notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn);
    }

    @Override
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus,
        String notificationRegistrationStatus)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataNotificationRegistrationEntity> criteria = builder.createQuery(BusinessObjectDataNotificationRegistrationEntity.class);

        // The criteria root is the business object data notification registration entity.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntityRoot =
            criteria.from(BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, NotificationEventTypeEntity> notificationEventTypeEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.notificationEventType);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> businessObjectDefinitionNamespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, FileTypeEntity> fileTypeEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.fileType, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDataStatusEntity> newBusinessObjectDataStatusEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.newBusinessObjectDataStatus, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDataStatusEntity> oldBusinessObjectDataStatusEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.oldBusinessObjectDataStatus, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, NotificationRegistrationStatusEntity> notificationRegistrationStatusEntityJoin =
            businessObjectDataNotificationEntityRoot.join(BusinessObjectDataNotificationRegistrationEntity_.notificationRegistrationStatus);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates
            .add(builder.equal(builder.upper(notificationEventTypeEntityJoin.get(NotificationEventTypeEntity_.code)), notificationEventTypeCode.toUpperCase()));
        predicates.add(builder
            .equal(builder.upper(businessObjectDefinitionNamespaceEntityJoin.get(NamespaceEntity_.code)), businessObjectDataKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.or(builder.isNull(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.usage)), builder
            .equal(builder.upper(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.usage)),
                businessObjectDataKey.getBusinessObjectFormatUsage().toUpperCase())));
        predicates.add(builder.or(builder.isNull(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.fileType)),
            builder.equal(builder.upper(fileTypeEntityJoin.get(FileTypeEntity_.code)), businessObjectDataKey.getBusinessObjectFormatFileType().toUpperCase())));
        predicates.add(builder
            .or(builder.isNull(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion)),
                builder.equal(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion),
                    businessObjectDataKey.getBusinessObjectFormatVersion())));
        predicates.add(builder
            .or(builder.isNull(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.newBusinessObjectDataStatus)),
                builder.equal(builder.upper(newBusinessObjectDataStatusEntityJoin.get(BusinessObjectDataStatusEntity_.code)),
                    newBusinessObjectDataStatus.toUpperCase())));
        // Please note that old business object data status parameter value is null for a business object data registration event.
        predicates.add(builder
            .or(builder.isNull(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.oldBusinessObjectDataStatus)),
                builder.equal(builder.upper(oldBusinessObjectDataStatusEntityJoin.get(BusinessObjectDataStatusEntity_.code)),
                    oldBusinessObjectDataStatus == null ? null : oldBusinessObjectDataStatus.toUpperCase())));
        predicates.add(builder.equal(builder.upper(notificationRegistrationStatusEntityJoin.get(NotificationRegistrationStatusEntity_.code)),
            notificationRegistrationStatus.toUpperCase()));

        // Order the results by namespace and notification name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntityJoin.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(businessObjectDataNotificationEntityRoot.get(BusinessObjectDataNotificationRegistrationEntity_.name)));

        // Add the clauses for the query.
        criteria.select(businessObjectDataNotificationEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
