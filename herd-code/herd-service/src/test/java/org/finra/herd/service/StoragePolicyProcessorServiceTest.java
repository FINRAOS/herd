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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.dao.impl.MockGlacierOperationsImpl;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests various functionality within the storage policy processor service.
 */
public class StoragePolicyProcessorServiceTest extends AbstractServiceTest
{
    private final String S3_KEY_PREFIX =
        getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, DATA_VERSION);

    private static final Path LOCAL_TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"), "herd-storage-policy-processor-service-test-" + RANDOM_SUFFIX);

    /**
     * Sets up the test environment.
     */
    @Before
    public void setup() throws Exception
    {
        // Create local temp directories.
        LOCAL_TEMP_DIR.toFile().mkdirs();

        // Create local test files.
        for (String filePath : LOCAL_FILES)
        {
            createLocalFile(LOCAL_TEMP_DIR.toString(), filePath, FILE_SIZE_1_KB);
        }

        // Upload test file to S3. Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        s3Dao.uploadDirectory(
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(S3_KEY_PREFIX + "/").localPath(LOCAL_TEMP_DIR.toString())
                .recursive(true).build());

        // Clean up the temporary directory.
        FileUtils.cleanDirectory(LOCAL_TEMP_DIR.toFile());
    }

    /**
     * Cleans up the test environment.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Delete the local temporary directory.
        FileUtils.deleteDirectory(LOCAL_TEMP_DIR.toFile());

        // Delete test files from the S3 storage. Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(S3_KEY_PREFIX + "/").localPath(LOCAL_TEMP_DIR.toString())
                .recursive(true).build();
        if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
        {
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
    }

    @Test
    public void testProcessStoragePolicySelectionMessage() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_TEMP_DIR.getKey(), LOCAL_TEMP_DIR.toString());
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Create and persist the relative database entities.
            createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION);

            // Create and persist a storage unit in the source storage.
            StorageUnitEntity sourceStorageUnitEntity =
                createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

            // Add storage files to the source storage unit.
            for (String filePath : LOCAL_FILES)
            {
                createStorageFileEntity(sourceStorageUnitEntity, S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
            }

            // Create a storage policy key.
            StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

            // Create and persist a storage policy entity.
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

            // Perform a storage policy transition.
            storagePolicyProcessorService.processStoragePolicySelectionMessage(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));

            // Validate the status of the source storage unit.
            assertEquals(StorageUnitStatusEntity.DISABLED, sourceStorageUnitEntity.getStatus().getCode());

            // Retrieve and validate the destination storage unit.
            StorageUnitEntity destinationStorageUnitEntity =
                herdDao.getStorageUnitByBusinessObjectDataAndStorageName(sourceStorageUnitEntity.getBusinessObjectData(), STORAGE_NAME_2);
            assertEquals(StorageUnitStatusEntity.ENABLED, destinationStorageUnitEntity.getStatus().getCode());
            assertEquals(1, destinationStorageUnitEntity.getStorageFiles().size());
            StorageFileEntity destinationStorageFileEntity = destinationStorageUnitEntity.getStorageFiles().iterator().next();
            assertNotNull(destinationStorageFileEntity);
            assertTrue(destinationStorageFileEntity.getPath().startsWith(Integer.toString(sourceStorageUnitEntity.getId()) + "-"));
            assertNull(destinationStorageFileEntity.getRowCount());
            assertEquals(MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID, destinationStorageFileEntity.getArchiveId());

            // Validate that source S3 data is deleted.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(S3_KEY_PREFIX + "/").build();
            assertTrue(s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }
}
