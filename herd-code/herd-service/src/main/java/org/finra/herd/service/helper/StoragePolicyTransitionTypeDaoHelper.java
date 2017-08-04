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

import org.finra.herd.dao.StoragePolicyTransitionTypeDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;

/**
 * Helper for storage policy transition type related operations which require DAO.
 */
@Component
public class StoragePolicyTransitionTypeDaoHelper
{
    @Autowired
    private StoragePolicyTransitionTypeDao storagePolicyTransitionTypeDao;

    /**
     * Gets the storage policy transition type entity and ensure it exists.
     *
     * @param code the storage policy transition type code (case insensitive)
     *
     * @return the storage policy transition type entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyTransitionTypeEntity getStoragePolicyTransitionTypeEntity(String code) throws ObjectNotFoundException
    {
        StoragePolicyTransitionTypeEntity storagePolicyTransitionTypeEntity = storagePolicyTransitionTypeDao.getStoragePolicyTransitionTypeByCode(code);

        if (storagePolicyTransitionTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage policy transition type with code \"%s\" doesn't exist.", code));
        }

        return storagePolicyTransitionTypeEntity;
    }
}
