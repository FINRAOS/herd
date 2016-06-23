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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.NotificationRegistrationDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.NotificationRegistrationEntity;

/**
 * DAO Helper for notification registration status entity.
 */
@Component
public class NotificationRegistrationDaoHelper
{
    @Autowired
    private NotificationRegistrationDao notificationRegistrationDao;

    /**
     * Gets a notification registration entity by its namespace and name, or throws an ObjectNotFoundException if not found.
     *
     * @param namespace The namespace of the notification registration
     * @param name The name of the notification registration
     *
     * @return The notification registration entity
     */
    public NotificationRegistrationEntity getNotificationRegistration(String namespace, String name)
    {
        NotificationRegistrationEntity notificationRegistration = notificationRegistrationDao.getNotificationRegistration(namespace, name);
        if (notificationRegistration == null)
        {
            throw new ObjectNotFoundException("The notification registration with namespace \"" + namespace + "\" and name \"" + name + "\" was not found.");
        }
        return notificationRegistration;
    }
}
