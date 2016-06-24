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

import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.StoragePolicyEntity;

public interface StoragePolicyDao extends BaseJpaDao
{
    /**
     * Retrieves a storage policy entity by alternate key.
     *
     * @param key the storage policy key (case-insensitive)
     *
     * @return the storage policy entity
     */
    public StoragePolicyEntity getStoragePolicyByAltKey(StoragePolicyKey key);

    /**
     * Retrieves a storage policy entity by alternate key.
     *
     * @param key the storage policy key (case-insensitive)
     * @param storagePolicyVersion the storage policy version, may be null
     *
     * @return the storage policy entity
     */
    public StoragePolicyEntity getStoragePolicyByAltKeyAndVersion(StoragePolicyKey key, Integer storagePolicyVersion);
}
