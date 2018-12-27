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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.MultiValuedMap;
import org.junit.Test;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageFileDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStorageFileByStorageNameAndFilePath()
    {
        // Create relative database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        for (String file : LOCAL_FILES)
        {
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

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

    @Test
    public void testGetStorageFileByStorageNameAndFilePathDuplicateFiles()
    {
        // Create relative database entities.
        BusinessObjectDataEntity businessObjectDataEntity1 = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, Boolean.TRUE, BDATA_STATUS);
        BusinessObjectDataEntity businessObjectDataEntity2 = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION + 1, Boolean.TRUE, BDATA_STATUS);

        StorageEntity storageEntity = storageDao.getStorageByName(StorageEntity.MANAGED_STORAGE);

        StorageUnitEntity storageUnitEntity1 = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity1, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        StorageUnitEntity storageUnitEntity2 = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity2, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity1, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);
        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity2, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        try
        {
            // Try to retrieve storage file.
            storageFileDao.getStorageFileByStorageNameAndFilePath(StorageEntity.MANAGED_STORAGE, LOCAL_FILE);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Found more than one storage file with parameters"));
        }
    }

    @Test
    public void testGetStorageFileByStorageUnitEntityAndFilePath()
    {
        // Create relative database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, LOCAL_FILE);
        assertThat("Actual path does not equal expected path.", storageFileEntity.getPath(), is(LOCAL_FILE));
        assertThat("Actual file size does not equal expected file size.", storageFileEntity.getFileSizeBytes(), is(FILE_SIZE_1_KB));
        assertThat("Actual row count does not equal expected row count.", storageFileEntity.getRowCount(), is(ROW_COUNT_1000));

        // Confirm negative results when using wrong input parameters.
        assertNull("Expected null value.", storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, LOCAL_FILE.toUpperCase()));
        assertNull("Expected null value.", storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, "I_DO_NOT_EXIST"));
        assertNull("Expected null value.", storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(new StorageUnitEntity(), LOCAL_FILES.get(0)));
    }

    @Test
    public void testGetStorageFileCount()
    {
        // Create relative database entities.
        createDatabaseEntitiesForStorageFilesTesting();

        // Validate that we can get correct count for each file.
        for (String file : LOCAL_FILES)
        {
            assertEquals(Long.valueOf(1L), storageFileDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, file));
        }

        // Validate that we can get correct file count using upper and lower storage name.
        assertEquals(Long.valueOf(1L), storageFileDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)));
        assertEquals(Long.valueOf(1L), storageFileDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing storage.
        assertEquals(Long.valueOf(0L), storageFileDao.getStorageFileCount("I_DO_NOT_EXIST", LOCAL_FILES.get(0)));

        // Get 0 file count by specifying non-existing file path prefix.
        assertEquals(Long.valueOf(0L), storageFileDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST"));

        // Validate that we can get correct count of files from the LOCAL_FILES list that match "folder" file path prefix.
        assertEquals(Long.valueOf(3L), storageFileDao.getStorageFileCount(StorageEntity.MANAGED_STORAGE, "folder"));
    }

    @Test
    public void testGetStorageFilesByStorageAndFilePathPrefix()
    {
        // Create relative database entities.
        createDatabaseEntitiesForStorageFilesTesting();

        List<String> storageFilePaths;

        // Validate that we can retrieve each file.
        for (String file : LOCAL_FILES)
        {
            storageFilePaths = storageFileDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, file);
            assertEquals(1, storageFilePaths.size());
            assertEquals(file, storageFilePaths.get(0));
        }

        // Validate that we can retrieve the test file using upper and lower storage name.
        assertEquals(LOCAL_FILES.get(0),
            storageFileDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toUpperCase(), LOCAL_FILES.get(0)).get(0));
        assertEquals(LOCAL_FILES.get(0),
            storageFileDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE.toLowerCase(), LOCAL_FILES.get(0)).get(0));

        // Try to get file entities by specifying non-existing storage.
        assertEquals(0, storageFileDao.getStorageFilesByStorageAndFilePathPrefix("I_DO_NOT_EXIST", LOCAL_FILES.get(0)).size());

        // Try to get file entities by specifying non-existing file path prefix.
        assertEquals(0, storageFileDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "I_DO_NOT_EXIST").size());

        // Validate that we can retrieve the last 3 files in the expected order from the LOCAL_FILES list that match "folder" file path prefix.
        storageFilePaths = storageFileDao.getStorageFilesByStorageAndFilePathPrefix(StorageEntity.MANAGED_STORAGE, "folder");
        List<String> expectedFiles = Arrays.asList(LOCAL_FILES.get(5), LOCAL_FILES.get(4), LOCAL_FILES.get(3));
        assertEquals(expectedFiles.size(), storageFilePaths.size());
        for (int i = 0; i < expectedFiles.size(); i++)
        {
            assertEquals(expectedFiles.get(i), storageFilePaths.get(i));
        }
    }

    @Test
    public void testGetStoragePathsByStorageUnitIds() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_FILE_PATHS_QUERY_PAGINATION_SIZE.getKey(), LOCAL_FILES.size() / 2);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Create database entities required for testing.
            StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
            for (String file : LOCAL_FILES)
            {
                storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
            }

            // Retrieve storage file paths by storage unit ids.
            MultiValuedMap<Integer, String> result = storageFileDao.getStorageFilePathsByStorageUnitIds(Lists.newArrayList(storageUnitEntity.getId()));

            // Validate the results.
            assertEquals(LOCAL_FILES.size(), result.get(storageUnitEntity.getId()).size());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    private void createDatabaseEntitiesForStorageFilesTesting()
    {
        // Create relative database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        for (String file : LOCAL_FILES)
        {
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }
    }
}
