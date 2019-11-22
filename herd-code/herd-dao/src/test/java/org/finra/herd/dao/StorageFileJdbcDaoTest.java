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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.transaction.AfterTransaction;

import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageFileJdbcDaoTest extends AbstractDaoTest
{
    private StorageUnitEntity storageUnitEntity;

    @Test
    @Rollback(false)
    public void testSaveStorageFiles()
    {
        // Set up test data within the transaction
        storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Use the after transaction methods to validate the save storage files functionality.
    }

    @AfterTransaction
    private void validateSaveStorageFiles()
    {
        List<StorageFileEntity> storageFileEntities = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(file);
            storageFileEntity.setFileSizeBytes(FILE_SIZE_1_KB);
            storageFileEntity.setRowCount(ROW_COUNT_1000);
            storageFileEntities.add(storageFileEntity);
        }

        // Batch save the storage files list.
        storageFileDao.saveStorageFiles(storageFileEntities);

        // Retrieve the relative storage file entities and validate the results.
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, file);
            assertTrue(storageFileEntity.getPath().compareTo(file) == 0);
            assertTrue(storageFileEntity.getFileSizeBytes().compareTo(FILE_SIZE_1_KB) == 0);
            assertTrue(storageFileEntity.getRowCount().compareTo(ROW_COUNT_1000) == 0);
        }

        // Confirm negative results when using wrong input parameters.
        assertNull(storageFileDao.getStorageFileByStorageNameAndFilePath("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));
        assertNull(storageFileDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));

    }

    @AfterTransaction
    private void validateSaveStorageFilesNullValues()
    {
        List<StorageFileEntity> storageFileEntities = new ArrayList<>();
        for (String file : LOCAL_FILES_2)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(file);
            storageFileEntity.setFileSizeBytes(null);
            storageFileEntity.setRowCount(null);
            storageFileEntities.add(storageFileEntity);
        }

        // Batch save the storage files list.
        storageFileDao.saveStorageFiles(storageFileEntities);

        // Retrieve the relative storage file entities and validate the results.
        for (String file : LOCAL_FILES_2)
        {
            StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, file);
            assertTrue(storageFileEntity.getPath().compareTo(file) == 0);
            assertTrue(storageFileEntity.getFileSizeBytes() == null);
            assertTrue(storageFileEntity.getRowCount() == null);
        }

        // Insert null path test
        storageFileEntities = new ArrayList<>();
        for (int i = 0; i < LOCAL_FILES_2.size(); i++)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(null);
            storageFileEntity.setFileSizeBytes(null);
            storageFileEntity.setRowCount(null);
            storageFileEntities.add(storageFileEntity);
        }

        // Batch save the storage files list.
        storageFileDao.saveStorageFiles(storageFileEntities);
    }
}
