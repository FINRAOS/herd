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

import java.util.List;

import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.jpa.StorageEntity;

public interface StorageDao extends BaseJpaDao
{
    /**
     * Gets a storage by it's name.
     *
     * @param storageName the storage name (case-insensitive)
     *
     * @return the storage for the specified name
     */
    public StorageEntity getStorageByName(String storageName);

    /**
     * Gets a list of storage keys for all storages defined in the system.
     *
     * @return the list of storage keys
     */
    public List<StorageKey> getStorages();
}
