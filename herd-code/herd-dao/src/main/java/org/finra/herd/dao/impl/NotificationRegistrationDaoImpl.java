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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.NotificationRegistrationDao;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationRegistrationEntity_;

@Repository
public class NotificationRegistrationDaoImpl extends BaseJpaDaoImpl implements NotificationRegistrationDao
{
    @Override
    public NotificationRegistrationEntity getNotificationRegistration(String namespace, String name)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NotificationRegistrationEntity> criteriaQuery = criteriaBuilder.createQuery(NotificationRegistrationEntity.class);
        Root<NotificationRegistrationEntity> notificationRegistration = criteriaQuery.from(NotificationRegistrationEntity.class);
        Path<NamespaceEntity> namespacePath = notificationRegistration.get(NotificationRegistrationEntity_.namespace);
        return executeSingleResultQuery(criteriaQuery.select(notificationRegistration).where(criteriaBuilder.equal(criteriaBuilder.upper(namespacePath.get(
            NamespaceEntity_.code)), namespace.toUpperCase()), criteriaBuilder.equal(criteriaBuilder.upper(notificationRegistration.get(
                NotificationRegistrationEntity_.name)), name.toUpperCase())), "More than 1 notification registration was found with namespace \"" + namespace
                    + "\" and name \"" + name + "\"");
    }
}
