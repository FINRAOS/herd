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

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.StorageDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * Helper for storage related operations which require DAO.
 */
@Component
public class StorageDaoHelper
{
    @Autowired
    private StorageDao storageDao;

    /**
     * Gets a storage entity by storage key and makes sure that it exists.
     *
     * @param storageKey the storage key
     *
     * @return the storage entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public StorageEntity getStorageEntity(StorageKey storageKey) throws ObjectNotFoundException
    {
        return getStorageEntity(storageKey.getStorageName());
    }

    /**
     * Gets a storage entity based on the storage name and makes sure that it exists.
     *
     * @param storageName the storage name (case insensitive)
     *
     * @return the storage entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public StorageEntity getStorageEntity(String storageName) throws ObjectNotFoundException
    {
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);

        if (storageEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage with name \"%s\" doesn't exist.", storageName));
        }

        return storageEntity;
    }

    /**
     * Validates that specified storage names exist.
     *
     * @param storageNames the list of storage names
     *
     * @throws ObjectNotFoundException if one of the storage entities doesn't exist
     */
    public void validateStorageExistence(List<String> storageNames) throws ObjectNotFoundException
    {
        for (String storageName : storageNames)
        {
            getStorageEntity(storageName);
        }
    }
}
