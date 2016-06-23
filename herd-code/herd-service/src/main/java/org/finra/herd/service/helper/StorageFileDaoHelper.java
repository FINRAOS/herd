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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

/**
 * Helper for storage file related operations which require DAO.
 */
@Component
public class StorageFileDaoHelper
{
    @Autowired
    private StorageFileDao storageFileDao;

    /**
     * Creates storage file entities from the list of storage files.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageFiles the list of storage files
     *
     * @return the list storage file entities
     */
    public List<StorageFileEntity> createStorageFileEntitiesFromStorageFiles(StorageUnitEntity storageUnitEntity, List<StorageFile> storageFiles)
    {
        List<StorageFileEntity> storageFileEntities = new ArrayList<>();

        for (StorageFile storageFile : storageFiles)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntities.add(storageFileEntity);
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(storageFile.getFilePath());
            storageFileEntity.setFileSizeBytes(storageFile.getFileSizeBytes());
            storageFileEntity.setRowCount(storageFile.getRowCount());
            storageFileDao.saveAndRefresh(storageFileEntity);
        }

        return storageFileEntities;
    }

    /**
     * Retrieves storage file by storage name and file path.
     *
     * @param storageName the storage name
     * @param filePath the file path
     *
     * @return the storage file
     * @throws org.finra.herd.model.ObjectNotFoundException if the storage file doesn't exist
     * @throws IllegalArgumentException if more than one storage file matching the file path exist in the storage
     */
    public StorageFileEntity getStorageFileEntity(String storageName, String filePath) throws ObjectNotFoundException, IllegalArgumentException
    {
        StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageNameAndFilePath(storageName, filePath);

        if (storageFileEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage file \"%s\" doesn't exist in \"%s\" storage.", filePath, storageName));
        }

        return storageFileEntity;
    }
}
