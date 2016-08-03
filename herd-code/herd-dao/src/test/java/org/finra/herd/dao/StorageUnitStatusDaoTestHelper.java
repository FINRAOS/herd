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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class StorageUnitStatusDaoTestHelper
{
    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    /**
     * Creates and persists a new storage unit status entity.
     *
     * @param statusCode the code of the storage unit status
     *
     * @return the newly created storage unit status entity
     */
    public StorageUnitStatusEntity createStorageUnitStatusEntity(String statusCode)
    {
        return createStorageUnitStatusEntity(statusCode, AbstractDaoTest.DESCRIPTION, AbstractDaoTest.STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
    }

    /**
     * Creates and persists a new storage unit status entity.
     *
     * @param statusCode the code of the storage unit status
     * @param description the description of the status code
     * @param available specifies if the business object data stored in the relative storage unit is available or not for consumption
     *
     * @return the newly created storage unit status entity
     */
    public StorageUnitStatusEntity createStorageUnitStatusEntity(String statusCode, String description, Boolean available)
    {
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(statusCode);
        storageUnitStatusEntity.setDescription(description);
        storageUnitStatusEntity.setAvailable(available);
        return storageUnitStatusDao.saveAndRefresh(storageUnitStatusEntity);
    }
}
