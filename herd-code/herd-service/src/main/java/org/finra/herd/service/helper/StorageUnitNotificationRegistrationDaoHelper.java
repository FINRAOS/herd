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

import org.finra.herd.dao.StorageUnitNotificationRegistrationDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;

@Component
public class StorageUnitNotificationRegistrationDaoHelper
{
    @Autowired
    private StorageUnitNotificationRegistrationDao storageUnitNotificationRegistrationDao;

    /**
     * Gets a storage unit notification registration entity based on the key and makes sure that it exists.
     *
     * @param key the storage unit notification registration key
     *
     * @return the storage unit notification registration entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the entity doesn't exist
     */
    public StorageUnitNotificationRegistrationEntity getStorageUnitNotificationRegistrationEntity(NotificationRegistrationKey key)
        throws ObjectNotFoundException
    {
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity =
            storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(key);

        if (storageUnitNotificationRegistrationEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage unit notification registration with name \"%s\" does not exist for \"%s\" namespace.", key.getNotificationName(),
                    key.getNamespace()));
        }

        return storageUnitNotificationRegistrationEntity;
    }
}
