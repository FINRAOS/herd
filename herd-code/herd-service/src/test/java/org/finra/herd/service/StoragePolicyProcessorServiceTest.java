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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.StorageFileHelper;

/**
 * This class tests various functionality within the storage policy processor service.
 */
public class StoragePolicyProcessorServiceTest extends AbstractServiceTest
{
    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    @Qualifier(value = "storagePolicyProcessorServiceImpl")
    private StoragePolicyProcessorService storagePolicyProcessorServiceImpl;

    @Test
    public void testProcessStoragePolicySelectionMessage() throws Exception
    {
        // Build the expected S3 key prefix for test business object data.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Create S3FileTransferRequestParamsDto to access the source S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto sourceS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(s3KeyPrefix + "/").build();

        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Add storage files to the source storage unit.
        for (String filePath : LOCAL_FILES)
        {
            storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, s3KeyPrefix + "/" + filePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Get the source storage files.
        List<StorageFile> sourceStorageFiles = storageFileHelper.createStorageFilesFromEntities(sourceStorageUnitEntity.getStorageFiles());

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        try
        {
            // Put relative S3 files into the source S3 bucket.
            for (StorageFile storageFile : sourceStorageFiles)
            {
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), null), null);
            }

            // Perform a storage policy transition.
            storagePolicyProcessorService
                .processStoragePolicySelectionMessage(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));

            // Validate the status of the source storage unit.
            assertEquals(StorageUnitStatusEntity.ARCHIVED, sourceStorageUnitEntity.getStatus().getCode());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(sourceS3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(sourceS3FileTransferRequestParamsDto);
            }
            s3Operations.rollback();
        }
    }

    /**
     * This method is to get coverage for the storage policy processor service method that has an explicit annotation for transaction propagation.
     */
    @Test
    public void testStoragePolicyProcessorServiceMethodsNewTransactionPropagation() throws Exception
    {
        try
        {
            storagePolicyProcessorServiceImpl.processStoragePolicySelectionMessage(null);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy selection must be specified.", e.getMessage());
        }
    }
}
