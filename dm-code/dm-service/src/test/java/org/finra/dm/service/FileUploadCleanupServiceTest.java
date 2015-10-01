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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.List;

import com.amazonaws.AmazonServiceException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.finra.dm.dao.impl.MockS3OperationsImpl;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.StorageAttributeEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * This class tests functionality within the FileUploadCleanupService.
 */
public class FileUploadCleanupServiceTest extends AbstractServiceTest
{
    private String s3BucketName;
    
    @Before
    public void before()
    {
        s3BucketName = getS3ManagedBucketName();
    }
    
    @Test
    public void testDeleteBusinessObjectData() throws Exception
    {
        // Prepare database entries required for testing.
        BusinessObjectDataKey testBusinessObjectKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        createTestDatabaseEntities(testBusinessObjectKey, STORAGE_NAME, s3BucketName, MockS3OperationsImpl.MOCK_S3_FILE_NAME_NOT_FOUND, 15);

        // Delete the business object data.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataKeys);
        assertEquals(1, resultBusinessObjectDataKeys.size());
        assertEquals(testBusinessObjectKey, resultBusinessObjectDataKeys.get(0));
        validateBusinessObjectDataStatus(testBusinessObjectKey, BusinessObjectDataStatusEntity.DELETED);
    }

    @Test
    public void testDeleteBusinessObjectDataS3FileExists() throws Exception
    {
        // Prepare database entries required for testing.
        BusinessObjectDataKey testBusinessObjectKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        createTestDatabaseEntities(testBusinessObjectKey, STORAGE_NAME, s3BucketName, LOCAL_FILE, 15);

        // Delete the business object data.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataKeys);
        assertTrue(resultBusinessObjectDataKeys.isEmpty());
        validateBusinessObjectDataStatus(testBusinessObjectKey, BDATA_STATUS);
    }

    @Test
    public void testDeleteBusinessObjectDataAmazonServiceException() throws Exception
    {
        // Prepare database entries required for testing.
        BusinessObjectDataKey testBusinessObjectKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        createTestDatabaseEntities(testBusinessObjectKey, STORAGE_NAME, s3BucketName, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, 15);

        // Delete the business object data.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = fileUploadCleanupService.deleteBusinessObjectData(STORAGE_NAME, 10);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataKeys);
        assertTrue(resultBusinessObjectDataKeys.isEmpty());
        validateBusinessObjectDataStatus(testBusinessObjectKey, BDATA_STATUS);
    }

    @Test
    public void testAbortMultipartUploads() throws Exception
    {
        // Prepare database entities required for testing.
        createTestStorageEntity(STORAGE_NAME, s3BucketName);

        // Abort multipart uploads started more that 10 minutes ago.
        int resultAbortedMultipartUploadsCount = fileUploadCleanupService.abortMultipartUploads(STORAGE_NAME, 10);

        // Validate the result. The mocked multipart listing should list 2 multipart uploads initiated more than 10 minutes ago.
        assertEquals(2, resultAbortedMultipartUploadsCount);
    }

    @Test
    public void testAbortMultipartUploadsTruncatedMultipartListing() throws Exception
    {
        // Prepare database entities required for testing.
        createTestStorageEntity(STORAGE_NAME, MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_TRUNCATED_MULTIPART_LISTING);

        // Abort multipart uploads started more that 10 minutes ago.
        int resultAbortedMultipartUploadsCount = fileUploadCleanupService.abortMultipartUploads(STORAGE_NAME, 10);

        // Validate the result. The mocked truncated multipart listing should list 4 multipart uploads initiated more than 10 minutes ago.
        assertEquals(4, resultAbortedMultipartUploadsCount);
    }

    @Test
    public void testAbortMultipartUploadsAmazonServiceException() throws Exception
    {
        // Prepare database entities required for testing.
        createTestStorageEntity(STORAGE_NAME, MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_SERVICE_EXCEPTION);

        // Try to abort multipart uploads.
        try
        {
            fileUploadCleanupService.abortMultipartUploads(STORAGE_NAME, 10);
            fail("Should throw an AmazonServiceException.");
        }
        catch (AmazonServiceException e)
        {
            assertEquals("null (Service: null; Status Code: 0; Error Code: null; Request ID: null)", e.getMessage());
        }
    }

    private void createTestDatabaseEntities(BusinessObjectDataKey businessObjectDataKey, String storageName, String bucketName, String storageFilePath,
        int createdOnTimestampMinutesOffset) throws Exception
    {
        // Create a storage entity.
        StorageEntity storageEntity = createTestStorageEntity(storageName, bucketName);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectDataEntity(businessObjectDataKey, true, BDATA_STATUS);
        // Apply the offset in minutes to createdOn value.
        businessObjectDataEntity.setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - createdOnTimestampMinutesOffset * 60 * 1000));
        StorageUnitEntity storageUnitEntity = createStorageUnitEntity(storageEntity, businessObjectDataEntity);
        createStorageFileEntity(storageUnitEntity, storageFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
        dmDao.saveAndRefresh(businessObjectDataEntity);
    }

    private StorageEntity createTestStorageEntity(String storageName, String bucketName) throws Exception
    {
        StorageEntity storageEntity = createStorageEntity(storageName);
        // If specified, populate bucket name attribute for this storage.
        if (StringUtils.isNotBlank(bucketName))
        {
            createStorageAttributeEntity(storageEntity, StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, bucketName);
            dmDao.saveAndRefresh(storageEntity);
        }
        return storageEntity;
    }

    private void validateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String expectedBusinessObjectDataStatus)
    {
        BusinessObjectDataEntity businessObjectDataEntity = dmDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);
        assertEquals(expectedBusinessObjectDataStatus, businessObjectDataEntity.getStatus().getCode());
    }
}
