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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.StoragePolicy;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyKeys;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;

/**
 * The storage policy service.
 */
public interface StoragePolicyService
{
    /**
     * Creates a new storage policy.
     *
     * @param request the information needed to create a storage policy
     *
     * @return the newly created storage policy
     */
    public StoragePolicy createStoragePolicy(StoragePolicyCreateRequest request);

    /**
     * Updates an existing storage policy by key.
     *
     * @param storagePolicyKey the storage policy key
     * @param request the information needed to update the storage policy
     *
     * @return the updated storage policy
     */
    public StoragePolicy updateStoragePolicy(StoragePolicyKey storagePolicyKey, StoragePolicyUpdateRequest request);

    /**
     * Gets an existing storage policy by key.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the storage policy information
     */
    public StoragePolicy getStoragePolicy(StoragePolicyKey storagePolicyKey);


    /**
     * Gets a list of keys for all storage policies defined in the system for the specified namespace.
     *
     * @param namespace the namespace
     *
     * @return the storage policy keys
     */
    public StoragePolicyKeys getStoragePolicyKeys(String namespace);
    StoragePolicy getStoragePolicy(StoragePolicyKey storagePolicyKey);

    /**
     * Deletes an existing storage policy by key.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the storage policy information
     */
    StoragePolicy deleteStoragePolicy(StoragePolicyKey storagePolicyKey);
}
