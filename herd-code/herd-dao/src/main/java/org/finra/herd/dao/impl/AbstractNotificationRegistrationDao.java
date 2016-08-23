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
import javax.persistence.criteria.Path;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;

public abstract class AbstractNotificationRegistrationDao extends AbstractHerdDao
{
    /**
     * Gets a list of notification registration keys from the list of tuples that contain notification registration name and namespace columns.
     *
     * @param tuples the list tof tuples that contain notification registration name and namespace columns
     * @param notificationRegistrationNamespaceColumn the column that contains the namespace of the notification registration
     * @param notificationRegistrationNameColumn the column that contains the name of the notification registration
     *
     * @return the list of notification registration keys
     */
    protected List<NotificationRegistrationKey> getNotificationRegistrationKeys(List<Tuple> tuples, Path<String> notificationRegistrationNamespaceColumn,
        Path<String> notificationRegistrationNameColumn)
    {
        List<NotificationRegistrationKey> notificationRegistrationKeys = new ArrayList<>();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
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
