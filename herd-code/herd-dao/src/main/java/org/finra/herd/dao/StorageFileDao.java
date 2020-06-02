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

import org.apache.commons.collections4.MultiValuedMap;

import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

public interface StorageFileDao extends BaseJpaDao
{
    /**
     * Retrieves storage file by storage name and file path.
     *
     * @param storageName the storage name (case-insensitive)
     * @param filePath the file path
     *
     * @return the storage file
     */
    StorageFileEntity getStorageFileByStorageNameAndFilePath(String storageName, String filePath);

    /**
     * Retrieves storage file by storage unit entity and file path.
     *
     * @param storageUnitEntity the storage unit entity
     * @param filePath the file path
     *
     * @return the storage file
     */
    StorageFileEntity getStorageFileByStorageUnitEntityAndFilePath(StorageUnitEntity storageUnitEntity, String filePath);

    /**
     * Counts all storage files matching the file path prefix in the specified storage.
     *
     * @param storageName the storage name (case-insensitive)
     * @param filePathPrefix the file path prefix that file paths should match
     *
     * @return the storage file count
     */
    Long getStorageFileCount(String storageName, String filePathPrefix);

    /**
     * Retrieves a map of storage unit ids to their corresponding storage file paths.
     *
     * @param storageUnitIds the list of storage unit identifiers
     *
     * @return the map of storage unit ids to their corresponding storage file paths.
     */
    MultiValuedMap<Integer, String> getStorageFilePathsByStorageUnitIds(List<Integer> storageUnitIds);

    /**
     * Saves a list of storage file entities.
     *
     * @param storageFileEntities the list of storage file entities to save.
     */
    void saveStorageFiles(List<StorageFileEntity> storageFileEntities);
}
