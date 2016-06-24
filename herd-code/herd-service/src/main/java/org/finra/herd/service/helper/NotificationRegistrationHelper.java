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
package org.finra.herd.service.helper;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;

/**
 * A helper class for NotificationRegistration related code.
 */
@Component
public class NotificationRegistrationHelper
{
    /**
     * Gets a notification registration key from the notification registration entity.
     *
     * @param notificationRegistrationEntity the notification registration entity
     *
     * @return the notification registration key
     */
    public NotificationRegistrationKey getNotificationRegistrationKey(NotificationRegistrationEntity notificationRegistrationEntity)
    {
        return new NotificationRegistrationKey(notificationRegistrationEntity.getNamespace().getCode(), notificationRegistrationEntity.getName());
    }
}
