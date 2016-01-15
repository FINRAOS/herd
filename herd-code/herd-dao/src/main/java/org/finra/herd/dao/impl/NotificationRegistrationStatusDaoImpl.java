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
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.NotificationRegistrationStatusDao;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity_;

@Repository
public class NotificationRegistrationStatusDaoImpl extends BaseJpaDaoImpl implements NotificationRegistrationStatusDao
{
    @Override
    public NotificationRegistrationStatusEntity getNotificationRegistrationStatus(String code)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NotificationRegistrationStatusEntity> criteriaQuery = criteriaBuilder.createQuery(NotificationRegistrationStatusEntity.class);
        Root<NotificationRegistrationStatusEntity> notificationRegistrationStatus = criteriaQuery.from(NotificationRegistrationStatusEntity.class);
        return executeSingleResultQuery(criteriaQuery.select(notificationRegistrationStatus).where(criteriaBuilder.equal(criteriaBuilder.upper(
            notificationRegistrationStatus.get(NotificationRegistrationStatusEntity_.code)), code.toUpperCase())),
            "More than 1 notification registration status was found with code \"" + code + "\"");
    }
}
