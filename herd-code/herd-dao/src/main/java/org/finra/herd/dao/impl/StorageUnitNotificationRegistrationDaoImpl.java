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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.StorageUnitNotificationRegistrationDao;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity_;

@Repository
public class StorageUnitNotificationRegistrationDaoImpl extends AbstractHerdDao implements StorageUnitNotificationRegistrationDao
{
    @Override
    public StorageUnitNotificationRegistrationEntity getStorageUnitNotificationRegistrationByAltKey(NotificationRegistrationKey key)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StorageUnitNotificationRegistrationEntity> criteria = builder.createQuery(StorageUnitNotificationRegistrationEntity.class);

        // The criteria root is the storage unit notification registration entity.
        Root<StorageUnitNotificationRegistrationEntity> businessObjectDataNotificationEntity = criteria.from(StorageUnitNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitNotificationRegistrationEntity, NamespaceEntity> namespaceEntity =
            businessObjectDataNotificationEntity.join(StorageUnitNotificationRegistrationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), key.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder
            .equal(builder.upper(businessObjectDataNotificationEntity.get(StorageUnitNotificationRegistrationEntity_.name)),
                key.getNotificationName().toUpperCase()));

        criteria.select(businessObjectDataNotificationEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String
            .format("Found more than one storage unit notification registration with with parameters {namespace=\"%s\", notificationName=\"%s\"}.",
                key.getNamespace(), key.getNotificationName()));
    }

    @Override
    public List<NotificationRegistrationKey> getStorageUnitNotificationRegistrationKeysByNamespace(String namespace)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage unit notification registration.
        Root<StorageUnitNotificationRegistrationEntity> businessObjectDataNotificationEntityRoot =
            criteria.from(StorageUnitNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitNotificationRegistrationEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDataNotificationEntityRoot.join(StorageUnitNotificationRegistrationEntity_.namespace);

        // Get the columns.
        Path<String> notificationRegistrationNamespaceColumn = namespaceEntityJoin.get(NamespaceEntity_.code);
        Path<String> notificationRegistrationNameColumn = businessObjectDataNotificationEntityRoot.get(StorageUnitNotificationRegistrationEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), namespace.toUpperCase());

        // Add the select clause.
        criteria.multiselect(notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(notificationRegistrationNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<NotificationRegistrationKey> notificationRegistrationKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey();
            notificationRegistrationKeys.add(notificationRegistrationKey);
            notificationRegistrationKey.setNamespace(tuple.get(notificationRegistrationNamespaceColumn));
            notificationRegistrationKey.setNotificationName(tuple.get(notificationRegistrationNameColumn));
        }

        return notificationRegistrationKeys;
    }

    @Override
    public List<NotificationRegistrationKey> getStorageUnitNotificationRegistrationKeysByNotificationFilter(
        StorageUnitNotificationFilter businessObjectDataNotificationFilter)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the storage unit notification registration.
        Root<StorageUnitNotificationRegistrationEntity> notificationRegistrationEntityRoot = criteria.from(StorageUnitNotificationRegistrationEntity.class);

        // Join to the other tables we can filter on.
        Join<StorageUnitNotificationRegistrationEntity, NamespaceEntity> notificationRegistrationNamespaceEntityJoin =
            notificationRegistrationEntityRoot.join(StorageUnitNotificationRegistrationEntity_.namespace);
        Join<StorageUnitNotificationRegistrationEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity =
            notificationRegistrationEntityRoot.join(StorageUnitNotificationRegistrationEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> businessObjectDefinitionNamespaceEntity =
            businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);
        Join<StorageUnitNotificationRegistrationEntity, FileTypeEntity> fileTypeEntity =
            notificationRegistrationEntityRoot.join(StorageUnitNotificationRegistrationEntity_.fileType, JoinType.LEFT);

        // Get the columns.
        Path<String> notificationRegistrationNamespaceColumn = notificationRegistrationNamespaceEntityJoin.get(NamespaceEntity_.code);
        Path<String> notificationRegistrationNameColumn = notificationRegistrationEntityRoot.get(StorageUnitNotificationRegistrationEntity_.name);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionNamespaceEntity.get(NamespaceEntity_.code)),
            businessObjectDataNotificationFilter.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataNotificationFilter.getBusinessObjectDefinitionName().toUpperCase()));
        if (StringUtils.isNotBlank(businessObjectDataNotificationFilter.getBusinessObjectFormatUsage()))
        {
            predicates.add(builder.or(builder.isNull(notificationRegistrationEntityRoot.get(StorageUnitNotificationRegistrationEntity_.usage)), builder
                .equal(builder.upper(notificationRegistrationEntityRoot.get(StorageUnitNotificationRegistrationEntity_.usage)),
                    businessObjectDataNotificationFilter.getBusinessObjectFormatUsage().toUpperCase())));
        }
        if (StringUtils.isNotBlank(businessObjectDataNotificationFilter.getBusinessObjectFormatFileType()))
        {
            predicates.add(builder.or(builder.isNull(notificationRegistrationEntityRoot.get(StorageUnitNotificationRegistrationEntity_.fileType)), builder
                .equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)),
                    businessObjectDataNotificationFilter.getBusinessObjectFormatFileType().toUpperCase())));
        }

        // Add the select and where clauses to the query.
        criteria.multiselect(notificationRegistrationNamespaceColumn, notificationRegistrationNameColumn)
            .where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Add the order by clause to the query.
        criteria.orderBy(builder.asc(notificationRegistrationNamespaceColumn), builder.asc(notificationRegistrationNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<NotificationRegistrationKey> notificationRegistrationKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey();
            notificationRegistrationKeys.add(notificationRegistrationKey);
            notificationRegistrationKey.setNamespace(tuple.get(notificationRegistrationNamespaceColumn));
            notificationRegistrationKey.setNotificationName(tuple.get(notificationRegistrationNameColumn));
        }

        return notificationRegistrationKeys;
    }
}
