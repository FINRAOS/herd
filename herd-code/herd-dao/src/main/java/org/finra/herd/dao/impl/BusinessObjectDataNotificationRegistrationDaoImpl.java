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

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
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
public class BusinessObjectDataNotificationRegistrationDaoImpl extends BaseJpaDaoImpl implements BusinessObjectDataNotificationRegistrationDao
{
    @Override
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistrationByAltKey(
        NotificationRegistrationKey key)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataNotificationRegistrationEntity> criteria = builder.createQuery(BusinessObjectDataNotificationRegistrationEntity.class);

        // The criteria root is the business object data notification registration entity.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity = criteria.from(
            BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity = businessObjectDataNotificationEntity.join(
            BusinessObjectDataNotificationRegistrationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), key.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.name)), key.getNotificationName().toUpperCase()));

        criteria.select(businessObjectDataNotificationEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object data notification registration with with parameters {namespace=\"%s\", notificationName=\"%s\"}.", key
                .getNamespace(), key.getNotificationName()));
    }

    @Override
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeys(String namespaceCode)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object data notification.
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity = criteria.from(
            BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity = businessObjectDataNotificationEntity.join(
            BusinessObjectDataNotificationRegistrationEntity_.namespace);

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
        List<NotificationRegistrationKey> businessObjectDataNotificationKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            NotificationRegistrationKey businessObjectDataNotificationKey = new NotificationRegistrationKey();
            businessObjectDataNotificationKeys.add(businessObjectDataNotificationKey);
            businessObjectDataNotificationKey.setNamespace(tuple.get(namespaceCodeColumn));
            businessObjectDataNotificationKey.setNotificationName(tuple.get(notificationNameColumn));
        }

        return businessObjectDataNotificationKeys;
    }

    @Override
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        return getBusinessObjectDataNotificationRegistrations(notificationEventTypeCode, businessObjectDataKey, newBusinessObjectDataStatus,
            oldBusinessObjectDataStatus, "ENABLED");
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
        Root<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationEntity = criteria.from(
            BusinessObjectDataNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataNotificationRegistrationEntity, NamespaceEntity> namespaceEntity = businessObjectDataNotificationEntity.join(
            BusinessObjectDataNotificationRegistrationEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, NotificationEventTypeEntity> notificationEventTypeEntity = businessObjectDataNotificationEntity
            .join(BusinessObjectDataNotificationRegistrationEntity_.notificationEventType);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> businessObjectDefinitionNamespaceEntity = businessObjectDefinitionEntity.join(
            BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDataNotificationRegistrationEntity, FileTypeEntity> fileTypeEntity = businessObjectDataNotificationEntity.join(
            BusinessObjectDataNotificationRegistrationEntity_.fileType, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDataStatusEntity> newBusinessObjectDataStatusEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.newBusinessObjectDataStatus, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, BusinessObjectDataStatusEntity> oldBusinessObjectDataStatusEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.oldBusinessObjectDataStatus, JoinType.LEFT);
        Join<BusinessObjectDataNotificationRegistrationEntity, NotificationRegistrationStatusEntity> notificationRegistrationStatusEntity =
            businessObjectDataNotificationEntity.join(BusinessObjectDataNotificationRegistrationEntity_.notificationRegistrationStatus);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(notificationEventTypeEntity.get(NotificationEventTypeEntity_.code)), notificationEventTypeCode
            .toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionNamespaceEntity.get(NamespaceEntity_.code)),
            businessObjectDataKey.getNamespace().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.or(builder.isNull(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.usage)), builder.equal(builder.upper(businessObjectDataNotificationEntity.get(
                BusinessObjectDataNotificationRegistrationEntity_.usage)), businessObjectDataKey.getBusinessObjectFormatUsage().toUpperCase())));
        queryRestriction = builder.and(queryRestriction, builder.or(builder.isNull(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.fileType)), builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)),
                businessObjectDataKey.getBusinessObjectFormatFileType().toUpperCase())));
        queryRestriction = builder.and(queryRestriction, builder.or(builder.isNull(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion)), builder.equal(businessObjectDataNotificationEntity.get(
                BusinessObjectDataNotificationRegistrationEntity_.businessObjectFormatVersion), businessObjectDataKey.getBusinessObjectFormatVersion())));
        queryRestriction = builder.and(queryRestriction, builder.or(builder.isNull(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.newBusinessObjectDataStatus)), builder.equal(builder.upper(newBusinessObjectDataStatusEntity.get(
                BusinessObjectDataStatusEntity_.code)), newBusinessObjectDataStatus.toUpperCase())));
        // Please note that old business object data status parameter value is null for a business object data registration event.
        queryRestriction = builder.and(queryRestriction, builder.or(builder.isNull(businessObjectDataNotificationEntity.get(
            BusinessObjectDataNotificationRegistrationEntity_.oldBusinessObjectDataStatus)), builder.equal(builder.upper(oldBusinessObjectDataStatusEntity.get(
                BusinessObjectDataStatusEntity_.code)), oldBusinessObjectDataStatus == null ? null : oldBusinessObjectDataStatus.toUpperCase())));

        if (notificationRegistrationStatus != null)
        {
            queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(notificationRegistrationStatusEntity.get(
                NotificationRegistrationStatusEntity_.code)), notificationRegistrationStatus.toUpperCase()));
        }

        // Order the results by namespace and notification name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntity.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(businessObjectDataNotificationEntity.get(BusinessObjectDataNotificationRegistrationEntity_.name)));

        // Add the clauses for the query.
        criteria.select(businessObjectDataNotificationEntity).where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
