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
package org.finra.herd.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.api.xml.StoragePlatform;
import org.finra.herd.model.api.xml.StoragePlatforms;
import org.finra.herd.service.StoragePlatformService;
import org.finra.herd.service.helper.StoragePlatformHelper;

/**
 * The storage service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePlatformServiceImpl implements StoragePlatformService
{
    @Autowired
    private StoragePlatformHelper storagePlatformHelper;

    @Autowired
    private HerdDao herdDao;

    @Override
    public StoragePlatform getStoragePlatform(String storagePlatformName)
    {
        String storagePlatformNameLocal = storagePlatformName;

        // Perform validation and trim.
        Assert.hasText(storagePlatformNameLocal, "A storage platform name must be specified.");
        storagePlatformNameLocal = storagePlatformNameLocal.trim();

        return createStoragePlatformFromEntity(storagePlatformHelper.getStoragePlatformEntity(storagePlatformNameLocal));
    }

    /**
     * Gets a list of storage platforms.
     *
     * @return the list of storage platforms.
     */
    @Override
    public StoragePlatforms getStoragePlatforms()
    {
        List<StoragePlatformEntity> storagePlatformEntities = herdDao.findAll(StoragePlatformEntity.class);
        StoragePlatforms storagePlatforms = new StoragePlatforms();
        for (StoragePlatformEntity storagePlatformEntity : storagePlatformEntities)
        {
            storagePlatforms.getStoragePlatforms().add(createStoragePlatformFromEntity(storagePlatformEntity));
        }
        return storagePlatforms;
    }

    /**
     * Creates a storage platform object from it's entity object.
     *
     * @param storagePlatformEntity the storage platform entity.
     *
     * @return the storage platform.
     */
    private StoragePlatform createStoragePlatformFromEntity(StoragePlatformEntity storagePlatformEntity)
    {
        StoragePlatform storagePlatform = new StoragePlatform();
        storagePlatform.setName(storagePlatformEntity.getName());
        return storagePlatform;
    }
}
