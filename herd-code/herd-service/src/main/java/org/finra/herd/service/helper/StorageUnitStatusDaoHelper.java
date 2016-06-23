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

import org.finra.herd.dao.StorageUnitStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * Helper for storage unit status related operations which require DAO.
 */
@Component
public class StorageUnitStatusDaoHelper
{
    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    /**
     * Gets a storage unit status entity by the code and ensure it exists.
     *
     * @param code the storage unit status code (case insensitive)
     *
     * @return the storage unit status entity
     * @throws ObjectNotFoundException if the status entity doesn't exist
     */
    public StorageUnitStatusEntity getStorageUnitStatusEntity(String code) throws ObjectNotFoundException
    {
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(code);

        if (storageUnitStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage unit status \"%s\" doesn't exist.", code));
        }

        return storageUnitStatusEntity;
    }
}
