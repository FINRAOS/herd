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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
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

        // Create S3FileTransferRequestParamsDto to access the S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(s3KeyPrefix + "/").build();

        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Collections.singletonList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE,
                BDEF_NAME, Collections.singletonList(FORMAT_FILE_TYPE_CODE), Collections.singletonList(STORAGE_NAME),
                Collections.singletonList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Add storage files to the storage unit.
        for (String filePath : LOCAL_FILES)
        {
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, s3KeyPrefix + "/" + filePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Get the storage files.
        List<StorageFile> storageFiles = storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles());

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Put relative S3 files into the S3 bucket.
            for (StorageFile storageFile : storageFiles)
            {
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), null), null);
            }

            // Add one more S3 file, which is an unregistered zero byte file.
            // The validation is expected not to fail when detecting an unregistered zero byte S3 file.
            String unregisteredS3FilePath = s3KeyPrefix + "/unregistered.txt";
            s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, unregisteredS3FilePath, new ByteArrayInputStream(new byte[0]), null), null);

            // Assert that we got all files listed under the test S3 prefix.
            assertEquals(storageFiles.size() + 1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());

            // Perform a storage policy transition.
            storagePolicyProcessorService
                .processStoragePolicySelectionMessage(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));

            // Create an expected S3 tag.
            Tag expectedTag = new Tag((String) ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_KEY.getDefaultValue(),
                (String) ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_VALUE.getDefaultValue());

            // Validate that all registered S3 files are now tagged.
            for (StorageFile storageFile : storageFiles)
            {
                GetObjectTaggingResult getObjectTaggingResult =
                    s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, storageFile.getFilePath()), null);
                assertEquals(Collections.singletonList(expectedTag), getObjectTaggingResult.getTagSet());
            }

            // Validate that unregistered S3 file is now tagged.
            GetObjectTaggingResult getObjectTaggingResult =
                s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, unregisteredS3FilePath), null);
            assertEquals(Collections.singletonList(expectedTag), getObjectTaggingResult.getTagSet());

            // Validate the status of the storage unit.
            assertEquals(StorageUnitStatusEntity.ARCHIVED, storageUnitEntity.getStatus().getCode());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }
            s3Operations.rollback();

            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testProcessStoragePolicySelectionMessageRuntimeException() throws Exception
    {
        // Build the expected S3 key prefix for test business object data.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Create S3FileTransferRequestParamsDto to access the S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(s3KeyPrefix + "/").build();

        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Collections.singletonList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE,
                BDEF_NAME, Collections.singletonList(FORMAT_FILE_TYPE_CODE), Collections.singletonList(STORAGE_NAME),
                Collections.singletonList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Add storage files to the storage unit.
        for (String filePath : LOCAL_FILES)
        {
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, s3KeyPrefix + "/" + filePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        // Get the storage files.
        List<StorageFile> storageFiles = storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles());

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Put relative S3 files into the S3 bucket, except for the last one.  This should trigger a RuntimeException.
            for (StorageFile storageFile : storageFiles.subList(0, storageFiles.size() - 1))
            {
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), null), null);
            }

            // Try to perform a storage policy transition.
            storagePolicyProcessorService
                .processStoragePolicySelectionMessage(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (RuntimeException e)
        {
            // Validate the exception error message.
            assertEquals(String
                    .format("Registered file \"%s\" does not exist in \"%s\" storage.", storageFiles.get(storageFiles.size() - 1).getFilePath(), STORAGE_NAME),
                e.getMessage());

            // Validate the StoragePolicyTransitionFailedAttempts value.
            assertEquals(Integer.valueOf(1), storageUnitEntity.getStoragePolicyTransitionFailedAttempts());

            // Validate the status of the storage unit.
            assertEquals(StorageUnitStatusEntity.ARCHIVING, storageUnitEntity.getStatus().getCode());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }
            s3Operations.rollback();

            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
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
