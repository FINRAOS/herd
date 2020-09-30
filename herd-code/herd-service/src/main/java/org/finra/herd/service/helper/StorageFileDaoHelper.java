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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
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
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private StorageFileDao storageFileDao;

    /**
     * Creates storage file entities from the list of storage files.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageFiles the list of storage files
     * @param directoryPath the directory path for the storage file
     *
     * @return the list storage file entities
     */
    public List<StorageFileEntity> createStorageFileEntitiesFromStorageFiles(StorageUnitEntity storageUnitEntity, List<StorageFile> storageFiles,
        String directoryPath)
    {
        List<StorageFileEntity> storageFileEntities = new ArrayList<>();

        for (StorageFile storageFile : storageFiles)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntities.add(storageFileEntity);
            storageFileEntity.setStorageUnit(storageUnitEntity);

            if (StringUtils.isNotBlank(directoryPath))
            {
                // Handle empty folder S3 marker as a special case.
                if (StringUtils.equals(storageFile.getFilePath(), directoryPath + StorageFileEntity.S3_EMPTY_PARTITION))
                {
                    storageFileEntity.setPath(StorageFileEntity.S3_EMPTY_PARTITION);
                }
                // Otherwise, minimize the file path.
                else
                {
                    // Minimize the file path.
                    storageFileEntity.setPath(HerdStringUtils.getMinimizedFilePath(storageFile.getFilePath(), directoryPath));
                }
            }

            storageFileEntity.setFileSizeBytes(storageFile.getFileSizeBytes());
            storageFileEntity.setRowCount(storageFile.getRowCount());
        }

        storageFileDao.saveStorageFiles(storageFileEntities);

        return storageFileEntities;
    }

    /**
     * Retrieves storage file by storage name and file path.
     *
     * @param storageName the storage name
     * @param filePath the file path
     *
     * @return the storage file
     *
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

    /**
     * Retrieves storage file by storage unit entity and file path.
     *
     * @param storageUnitEntity the storage unit entity
     * @param filePath the file path
     * @param businessObjectDataKey the business object data key
     *
     * @return the storage file entity
     *
     * @throws org.finra.herd.model.ObjectNotFoundException if the storage file doesn't exist
     * @throws IllegalArgumentException if more than one storage file matching the file path exist in the storage
     */
    public StorageFileEntity getStorageFileEntity(StorageUnitEntity storageUnitEntity, String filePath, BusinessObjectDataKey businessObjectDataKey)
        throws ObjectNotFoundException, IllegalArgumentException
    {
        StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath);

        if (storageFileEntity == null && StringUtils.isNotBlank(storageUnitEntity.getDirectoryPath()) &&
            StringUtils.startsWith(filePath, StringUtils.appendIfMissing(storageUnitEntity.getDirectoryPath(), "/")))
        {
            // Attempt to retrieve the storage file with the file-only prefix.
            storageFileEntity = storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity,
                StringUtils.remove(filePath, StringUtils.appendIfMissing(storageUnitEntity.getDirectoryPath(), "/")));
        }

        if (storageFileEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Storage file \"%s\" doesn't exist in \"%s\" storage. Business object data: {%s}", filePath, storageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        return storageFileEntity;
    }
}
