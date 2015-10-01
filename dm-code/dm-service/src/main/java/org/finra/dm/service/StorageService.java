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
package org.finra.dm.service;

import java.util.Date;

import org.finra.dm.model.dto.StorageAlternateKeyDto;
import org.finra.dm.model.api.xml.Storage;
import org.finra.dm.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.dm.model.api.xml.StorageCreateRequest;
import org.finra.dm.model.api.xml.StorageDailyUploadStats;
import org.finra.dm.model.api.xml.StorageKeys;
import org.finra.dm.model.api.xml.StorageUpdateRequest;

/**
 * The storage service.
 */
public interface StorageService
{
    public Storage createStorage(StorageCreateRequest storageRequest);

    public Storage updateStorage(StorageAlternateKeyDto storageAlternateKey, StorageUpdateRequest storageRequest);

    public Storage getStorage(StorageAlternateKeyDto storageAlternateKey);

    public Storage deleteStorage(StorageAlternateKeyDto storageAlternateKey);

    /**
     * Gets a list of storage keys for all storages defined in the system.
     *
     * @return the storage keys
     */
    public StorageKeys getStorages();

    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, Date uploadDate);

    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        Date uploadDate);
}
