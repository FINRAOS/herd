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

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class BusinessObjectDataServiceDestroyBusinessObjectDataTest extends AbstractServiceTest
{
    @Test
    public void testDestroyBusinessObjectData() throws Exception
    {
        // Create a primary partition value that satisfies the retention threshold check.
        String primaryPartitionValue =
            DateFormatUtils.format(DateUtils.addDays(new Date(), -1 * (RETENTION_PERIOD_DAYS + 1)), AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);

        // Build the expected S3 key prefix for test business object data.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                primaryPartitionValue, null, null, DATA_VERSION);

        // Create S3FileTransferRequestParamsDto to access the S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(s3KeyPrefix + "/").build();

        // Create an S3 storage with the relative attributes.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString())));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
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

        // Get the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat();

        // Set the retention information for the business object format, which is the latest version business object format.
        businessObjectFormatEntity.setRetentionType(retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE));
        businessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_OBJECT_DELETE_TAG_KEY.getKey(), S3_OBJECT_TAG_KEY);
        overrideMap.put(ConfigurationValue.S3_OBJECT_DELETE_TAG_VALUE.getKey(), S3_OBJECT_TAG_VALUE);
        overrideMap.put(ConfigurationValue.S3_OBJECT_DELETE_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_OBJECT_DELETE_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Put relative S3 files into the S3 bucket.
            for (StorageFile storageFile : storageFiles)
            {
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), null), null);
            }

            // Add one more S3 file, which is an unregistered non-empty file.
            // The business object data destroy logic expected not to fail when detecting an unregistered S3 file.
            String unregisteredS3FilePath = s3KeyPrefix + "/unregistered.txt";
            s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, unregisteredS3FilePath, new ByteArrayInputStream(new byte[1]), null), null);

            // Assert that we got all files listed under the test S3 prefix.
            assertEquals(storageFiles.size() + 1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());

            // Request to destroy business object data.
            BusinessObjectData result = businessObjectDataService.destroyBusinessObjectData(businessObjectDataKey);

            // Validate the result.
            assertNotNull(result);
            assertEquals(storageUnitEntity.getBusinessObjectData().getId(), Long.valueOf(result.getId()));

            // Validate the status of the storage unit entity.
            assertEquals(StorageUnitStatusEntity.DISABLED, storageUnitEntity.getStatus().getCode());

            // Validate the status of the business object data entity.
            assertEquals(BusinessObjectDataStatusEntity.DELETED, storageUnitEntity.getBusinessObjectData().getStatus().getCode());

            // Validate that all registered S3 files are now tagged.
            for (StorageFile storageFile : storageFiles)
            {
                GetObjectTaggingResult getObjectTaggingResult =
                    s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, storageFile.getFilePath()), null);
                assertEquals(Collections.singletonList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE)), getObjectTaggingResult.getTagSet());
            }

            // Validate that unregistered S3 file is now tagged.
            GetObjectTaggingResult getObjectTaggingResult =
                s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, unregisteredS3FilePath), null);
            assertEquals(Collections.singletonList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE)), getObjectTaggingResult.getTagSet());
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
}
