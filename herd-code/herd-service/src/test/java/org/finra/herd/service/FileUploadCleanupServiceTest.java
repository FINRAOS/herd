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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.FileUploadCleanupServiceImpl;

/**
 * This class tests functionality within the FileUploadCleanupService.
 */
public class FileUploadCleanupServiceTest extends AbstractServiceTest
{
    private String s3BucketName;

    @Before
    public void before()
    {
        s3BucketName = storageDaoTestHelper.getS3ManagedBucketName();
    }

    /**
     * Cleans up the S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the destination S3 folder.
        s3Dao.deleteDirectory(s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());

        s3Operations.rollback();
    }

    @Test
    public void testDeleteBusinessObjectData() throws Exception
    {
        // Prepare database entries required for testing without creating an S3 file.
        StorageEntity storageEntity = createTestStorageEntity(STORAGE_NAME, s3BucketName);
        List<BusinessObjectDataKey> testBusinessObjectKeys = Arrays.asList(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION),
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
                DATA_VERSION),
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION_2),
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2, SUBPARTITION_VALUES,
                DATA_VERSION_2));
        createTestDatabaseEntities(testBusinessObjectKeys.get(0), BusinessObjectDataStatusEntity.UPLOADING, storageEntity, TARGET_S3_KEY, 15);
        createTestDatabaseEntities(testBusinessObjectKeys.get(1), BusinessObjectDataStatusEntity.UPLOADING, storageEntity, TARGET_S3_KEY, 5);
        createTestDatabaseEntities(testBusinessObjectKeys.get(2), BusinessObjectDataStatusEntity.RE_ENCRYPTING, storageEntity, TARGET_S3_KEY, 20);
        createTestDatabaseEntities(testBusinessObjectKeys.get(3), BDATA_STATUS, storageEntity, TARGET_S3_KEY, 20);

        // Delete the business object data.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

        // Validate the results - only business object data number 0 and 3 are expected to be deleted.
        assertNotNull(resultBusinessObjectDataKeys);
        assertEquals(Arrays.asList(testBusinessObjectKeys.get(2), testBusinessObjectKeys.get(0)), resultBusinessObjectDataKeys);

        // Validate business object data statuses.
        validateBusinessObjectDataStatus(testBusinessObjectKeys.get(0), BusinessObjectDataStatusEntity.DELETED);
        validateBusinessObjectDataStatus(testBusinessObjectKeys.get(1), BusinessObjectDataStatusEntity.UPLOADING);
        validateBusinessObjectDataStatus(testBusinessObjectKeys.get(2), BusinessObjectDataStatusEntity.DELETED);
        validateBusinessObjectDataStatus(testBusinessObjectKeys.get(3), BDATA_STATUS);
    }

    @Test
    public void testDeleteBusinessObjectDataS3FileExists() throws Exception
    {
        // Prepare database entries required for testing.
        StorageEntity storageEntity = createTestStorageEntity(STORAGE_NAME, s3BucketName);
        BusinessObjectDataKey testBusinessObjectKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        createTestDatabaseEntities(testBusinessObjectKey, BusinessObjectDataStatusEntity.UPLOADING, storageEntity, TARGET_S3_KEY, 15);

        // Put a file in S3.
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);

        // Delete the business object data.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataKeys);
        assertTrue(resultBusinessObjectDataKeys.isEmpty());
        validateBusinessObjectDataStatus(testBusinessObjectKey, BusinessObjectDataStatusEntity.UPLOADING);
    }

    @Test
    public void testDeleteBusinessObjectDataAmazonServiceException() throws Exception
    {
        // Prepare database entries required for testing.
        StorageEntity storageEntity = createTestStorageEntity(STORAGE_NAME, s3BucketName);
        BusinessObjectDataKey testBusinessObjectKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        createTestDatabaseEntities(testBusinessObjectKey, BusinessObjectDataStatusEntity.UPLOADING, storageEntity,
            MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, 15);

        executeWithoutLogging(FileUploadCleanupServiceImpl.class, () -> {
            // Delete the business object data.
            List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

            // Validate the results.
            assertNotNull(resultBusinessObjectDataKeys);
            assertTrue(resultBusinessObjectDataKeys.isEmpty());
            validateBusinessObjectDataStatus(testBusinessObjectKey, BusinessObjectDataStatusEntity.UPLOADING);
        });
    }

    private void createTestDatabaseEntities(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus, StorageEntity storageEntity,
        String storageFilePath, int createdOnTimestampMinutesOffset) throws Exception
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, true, businessObjectDataStatus);
        // Apply the offset in minutes to createdOn value.
        businessObjectDataEntity.setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - createdOnTimestampMinutesOffset * 60 * 1000));
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, storageFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
        herdDao.saveAndRefresh(businessObjectDataEntity);
    }

    private StorageEntity createTestStorageEntity(String storageName, String bucketName) throws Exception
    {
        List<Attribute> attributes = new ArrayList<>();

        // If specified, populate bucket name attribute for this storage.
        if (StringUtils.isNotBlank(bucketName))
        {
            attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), bucketName));
        }

        return storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);
    }

    private void validateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String expectedBusinessObjectDataStatus)
    {
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);
        assertEquals(expectedBusinessObjectDataStatus, businessObjectDataEntity.getStatus().getCode());
    }
}
