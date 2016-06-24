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

import org.finra.herd.dao.StoragePolicyDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.StoragePolicyEntity;

/**
 * Helper for storage policy related operations which require DAO.
 */
@Component
public class StoragePolicyDaoHelper
{
    @Autowired
    private StoragePolicyDao storagePolicyDao;

    /**
     * Gets a storage policy entity based on the key and makes sure that it exists.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the storage policy entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyEntity getStoragePolicyEntityByKey(StoragePolicyKey storagePolicyKey) throws ObjectNotFoundException
    {
        StoragePolicyEntity storagePolicyEntity = storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey);

        if (storagePolicyEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                    storagePolicyKey.getNamespace()));
        }

        return storagePolicyEntity;
    }

    /**
     * Gets a storage policy entity based on the key and version and makes sure that it exists.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     *
     * @return the storage policy entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyEntity getStoragePolicyEntityByKeyAndVersion(StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
        throws ObjectNotFoundException
    {
        StoragePolicyEntity storagePolicyEntity = storagePolicyDao.getStoragePolicyByAltKeyAndVersion(storagePolicyKey, storagePolicyVersion);

        if (storagePolicyEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage policy with name \"%s\" and version \"%d\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                    storagePolicyVersion, storagePolicyKey.getNamespace()));
        }

        return storagePolicyEntity;
    }
}
