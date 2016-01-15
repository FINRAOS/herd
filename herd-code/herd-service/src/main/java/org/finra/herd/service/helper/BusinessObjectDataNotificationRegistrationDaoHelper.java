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

import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

@Component
public class BusinessObjectDataNotificationRegistrationDaoHelper
{
    @Autowired
    private BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    /**
     * Gets a business object data notification registration entity based on the key and makes sure that it exists.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification registration entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistrationEntity(
        NotificationRegistrationKey key) throws ObjectNotFoundException
    {
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity = businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationByAltKey(key);

        if (businessObjectDataNotificationRegistrationEntity == null)
        {
            throw new ObjectNotFoundException(String.format(
                "Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.", key.getNotificationName(), key
                    .getNamespace()));
        }

        return businessObjectDataNotificationRegistrationEntity;
    }
}
