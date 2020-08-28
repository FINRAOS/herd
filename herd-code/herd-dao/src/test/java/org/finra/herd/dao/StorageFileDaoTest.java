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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageFileDaoTest extends AbstractDaoTest
{
    @Autowired
    JdbcTemplate jdbcTemplate;

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
    public void testGetStorageFilePathsByStorageUnitIdsChunkSizeOne() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(1, null);
    }

    @Test
    public void testGetStorageFilePathsByStorageUnitIdsDefaultChunkSizeAndPageSize() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(null, null);
    }

    @Test
    public void testGetStorageFilePathsByStorageUnitIdsMultipleChunks() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(2, null);
    }

    @Test
    public void testGetStorageFilePathsByStorageUnitIdsMultiplePages() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(null, LOCAL_FILES.size() / 2);
    }

    @Test
    public void testGetStorageFilePathsByStorageUnitIdsPageSizeOne() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(null, 1);
    }

    @Test
    public void testGetStorageFilePathsByStorageUnitIdsSingleChunkAndSinglePage() throws Exception
    {
        validateGetStoragePathsByStorageUnitIds(LOCAL_FILES.size() * LOCAL_FILES.size(), LOCAL_FILES.size() * LOCAL_FILES.size());
    }

    private void validateGetStoragePathsByStorageUnitIds(Integer chunkSize, Integer pageSize) throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_FILE_PATHS_QUERY_IN_CLAUSE_CHUNK_SIZE.getKey(), chunkSize);
        overrideMap.put(ConfigurationValue.STORAGE_FILE_PATHS_QUERY_PAGINATION_SIZE.getKey(), pageSize);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Create database entities required for testing.
            Map<Integer, Integer> expectedStorageFileCounts = new HashMap<>();
            for (int i = 0; i < LOCAL_FILES.size(); i++)
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                        PARTITION_VALUE + i, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
                for (String file : LOCAL_FILES.subList(0, LOCAL_FILES.size() - i))
                {
                    storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, file, FILE_SIZE_1_KB, ROW_COUNT_1000);
                }
                expectedStorageFileCounts.put(storageUnitEntity.getId(), LOCAL_FILES.size() - i);
            }

            // Retrieve storage file paths by created above storage unit ids along with one non-exiting storage unit id.
            List<Integer> storageUnitIds = new ArrayList<>();
            storageUnitIds.addAll(expectedStorageFileCounts.keySet());
            storageUnitIds.add(-1);
            MultiValuedMap<Integer, String> results = storageFileDao.getStorageFilePathsByStorageUnitIds(storageUnitIds);

            // Validate the results.
            assertEquals(expectedStorageFileCounts.keySet(), results.keySet());
            for (Integer storageUnitId : results.keySet())
            {
                assertEquals(Long.valueOf(expectedStorageFileCounts.get(storageUnitId)), Long.valueOf(CollectionUtils.size(results.get(storageUnitId))));
            }
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
