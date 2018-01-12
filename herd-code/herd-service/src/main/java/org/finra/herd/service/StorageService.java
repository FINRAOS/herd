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

import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageAttributesUpdateRequest;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;

/**
 * The storage service.
 */
public interface StorageService
{
    /**
     * Creates a new storage.
     *
     * @param storageCreateRequest the information needed to create storage
     *
     * @return the created storage information
     */
    Storage createStorage(StorageCreateRequest storageCreateRequest);

    /**
     * Deletes an existing storage by storage key.
     *
     * @param storageKey the storage key (case-insensitive)
     *
     * @return the storage information of the storage that got deleted
     */
    Storage deleteStorage(StorageKey storageKey);

    /**
     * Gets a list of storage keys for all storage defined in the system.
     *
     * @return the list of storage keys
     */
    StorageKeys getAllStorage();

    /**
     * Gets an existing storage by storage key.
     *
     * @param storageKey the storage key (case-insensitive)
     *
     * @return the storage information
     */
    Storage getStorage(StorageKey storageKey);

    /**
     * Updates an existing storage by storage key.
     *
     * @param storageKey the storage key (case-insensitive)
     * @param storageUpdateRequest the information needed to update the storage
     *
     * @return the updated storage information
     */
    Storage updateStorage(StorageKey storageKey, StorageUpdateRequest storageUpdateRequest);

    /**
     * Updates an existing storage attributes by storage key.
     *
     * @param storageKey the storage key (case-insensitive)
     * @param storageAttributesUpdateRequest the information needed to update storage attributes
     *
     * @return the updated storage information
     */
    Storage updateStorageAttributes(StorageKey storageKey, StorageAttributesUpdateRequest storageAttributesUpdateRequest);
}
