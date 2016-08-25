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
package org.finra.herd.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.service.NotificationRegistrationStatusService;
import org.finra.herd.service.helper.NotificationRegistrationDaoHelper;
import org.finra.herd.service.helper.NotificationRegistrationStatusDaoHelper;

@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class NotificationRegistrationStatusServiceImpl implements NotificationRegistrationStatusService
{
    @Autowired
    private NotificationRegistrationDaoHelper notificationRegistrationDaoHelper;

    @Autowired
    private NotificationRegistrationStatusDaoHelper notificationRegistrationStatusDaoHelper;

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public NotificationRegistrationStatusUpdateResponse updateNotificationRegistrationStatus(String namespace, String notificationName,
        NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest)
    {
        Assert.hasText(namespace, "The namespace must be specified");
        Assert.hasText(notificationName, "The notification name must be specified");
        String notificationRegistrationStatus = notificationRegistrationStatusUpdateRequest.getNotificationRegistrationStatus();
        Assert.hasText(notificationRegistrationStatus, "The notification registration status must be specified");
        NotificationRegistrationEntity notificationRegistration =
            notificationRegistrationDaoHelper.getNotificationRegistration(namespace.trim(), notificationName.trim());
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity =
            notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatusEntity(notificationRegistrationStatus.trim());
        notificationRegistration.setNotificationRegistrationStatus(notificationRegistrationStatusEntity);

        NotificationRegistrationStatusUpdateResponse notificationRegistrationStatusUpdateResponse = new NotificationRegistrationStatusUpdateResponse();
        notificationRegistrationStatusUpdateResponse.setNotificationRegistrationKey(
            new NotificationRegistrationKey(notificationRegistration.getNamespace().getCode(), notificationRegistration.getName()));
        notificationRegistrationStatusUpdateResponse.setNotificationRegistrationStatus(notificationRegistrationStatusEntity.getCode());
        return notificationRegistrationStatusUpdateResponse;
    }
}
