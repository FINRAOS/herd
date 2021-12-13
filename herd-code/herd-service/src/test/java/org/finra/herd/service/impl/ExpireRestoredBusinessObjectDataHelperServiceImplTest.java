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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * This class tests functionality within the expire restored business object data helper service implementation.
 */
public class ExpireRestoredBusinessObjectDataHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private ExpireRestoredBusinessObjectDataHelperServiceImpl expireRestoredBusinessObjectDataHelperServiceImpl;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCompleteStorageUnitExpiration()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a DTO for business object data restore parameters.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Lists.newArrayList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT)), NO_EXCEPTION, ARCHIVE_RETRIEVAL_OPTION,
                NO_BUSINESS_OBJECT_DATA);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a new storage unit status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.ARCHIVED);

        // Create an old storage unit status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.EXPIRING);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the storage unit entity.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                // Update the storage unit status.
                storageUnitEntity.setStatus(newStorageUnitStatusEntity);
                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVED);

        // Call the method under test.
        expireRestoredBusinessObjectDataHelperServiceImpl.completeStorageUnitExpiration(businessObjectDataRestoreDto);

        // Verify the external calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVED);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(StorageUnitStatusEntity.ARCHIVED, businessObjectDataRestoreDto.getNewStorageUnitStatus());
        assertEquals(StorageUnitStatusEntity.EXPIRING, businessObjectDataRestoreDto.getOldStorageUnitStatus());
    }

    @Test
    public void testExecuteS3SpecificSteps()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of storage files.
        List<StorageFile> storageFiles = Arrays.asList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT), new StorageFile(S3_KEY_2, FILE_SIZE_2, ROW_COUNT_2));

        // Create a DTO for business object data restore parameters.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, STORAGE_UNIT_STATUS_2,
                STORAGE_UNIT_STATUS, storageFiles, NO_EXCEPTION, ARCHIVE_RETRIEVAL_OPTION, NO_BUSINESS_OBJECT_DATA);

        // Create an initial instance of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an updated version of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto updatedS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        updatedS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        updatedS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        updatedS3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX + "/");

        // Create a mock S3 object summary for S3 object that belongs to Glacier storage class.
        S3ObjectSummary glacierS3ObjectSummary = mock(S3ObjectSummary.class);
        when(glacierS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Glacier.toString());

        // Create a mock S3 object summary for S3 object that does not belong to Glacier storage class.
        S3ObjectSummary standardS3ObjectSummary = mock(S3ObjectSummary.class);
        when(standardS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Standard.toString());

        // Create a list of S3 files.
        List<S3ObjectSummary> s3Files = Arrays.asList(glacierS3ObjectSummary, standardS3ObjectSummary);

        // Create a list of S3 objects that belong to Glacier storage class.
        List<S3ObjectSummary> glacierS3Files = Lists.newArrayList(glacierS3ObjectSummary);

        // Create a list of storage files that represent S3 objects of Glacier storage class.
        List<StorageFile> glacierStorageFiles = Lists.newArrayList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT));

        // Create a list of files.
        List<File> files = Lists.newArrayList(new File(S3_KEY));

        // Create a final version of DTO for business object data restore parameters.
        S3FileTransferRequestParamsDto finalS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        finalS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        finalS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        finalS3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX + "/");
        finalS3FileTransferRequestParamsDto.setFiles(files);

        // Mock the external calls.
        when(storageHelper.getS3FileTransferRequestParamsDto()).thenReturn(s3FileTransferRequestParamsDto);
        when(s3Service.listDirectory(updatedS3FileTransferRequestParamsDto, true)).thenReturn(s3Files);
        when(storageFileHelper.createStorageFilesFromS3ObjectSummaries(glacierS3Files)).thenReturn(glacierStorageFiles);
        when(storageFileHelper.getFiles(glacierStorageFiles)).thenReturn(files);

        // Call the method under test.
        expireRestoredBusinessObjectDataHelperServiceImpl.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // Verify the external calls.
        verify(storageHelper).getS3FileTransferRequestParamsDto();
        verify(s3Service).listDirectory(any(S3FileTransferRequestParamsDto.class), eq(true));
        verify(storageFileHelper).validateRegisteredS3Files(storageFiles, s3Files, STORAGE_NAME, businessObjectDataKey);
        verify(storageFileHelper).createStorageFilesFromS3ObjectSummaries(glacierS3Files);
        verify(storageFileHelper).getFiles(glacierStorageFiles);
        verify(s3Service).restoreObjects(finalS3FileTransferRequestParamsDto, 1, null);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testExecuteS3SpecificStepsDirectoryOnlyRegistration()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a DTO for business object data restore parameters.
        // Using an empty list for storage files to test directory only registration scenario.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, STORAGE_UNIT_STATUS_2,
                STORAGE_UNIT_STATUS, new ArrayList<>(), NO_EXCEPTION, ARCHIVE_RETRIEVAL_OPTION, NO_BUSINESS_OBJECT_DATA);

        // Create an initial instance of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an updated version of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto updatedS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        updatedS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        updatedS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        updatedS3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX + "/");

        // Create a mock S3 object summary for S3 object that belongs to Glacier storage class.
        S3ObjectSummary glacierS3ObjectSummary = mock(S3ObjectSummary.class);
        when(glacierS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Glacier.toString());

        // Create a mock S3 object summary for S3 object that does not belong to Glacier storage class.
        S3ObjectSummary standardS3ObjectSummary = mock(S3ObjectSummary.class);
        when(standardS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Standard.toString());

        // Create a list of S3 files.
        List<S3ObjectSummary> s3Files = Arrays.asList(glacierS3ObjectSummary, standardS3ObjectSummary);

        // Create a list of S3 objects that belong to Glacier storage class.
        List<S3ObjectSummary> glacierS3Files = Lists.newArrayList(glacierS3ObjectSummary);

        // Create a list of storage files that represent S3 objects of Glacier storage class.
        List<StorageFile> glacierStorageFiles = Lists.newArrayList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT));

        // Create a list of files.
        List<File> files = Lists.newArrayList(new File(S3_KEY));

        // Create a final version of DTO for business object data restore parameters.
        S3FileTransferRequestParamsDto finalS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        finalS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        finalS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        finalS3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX + "/");
        finalS3FileTransferRequestParamsDto.setFiles(files);

        // Mock the external calls.
        when(storageHelper.getS3FileTransferRequestParamsDto()).thenReturn(s3FileTransferRequestParamsDto);
        when(s3Service.listDirectory(updatedS3FileTransferRequestParamsDto, true)).thenReturn(s3Files);
        when(storageFileHelper.createStorageFilesFromS3ObjectSummaries(glacierS3Files)).thenReturn(glacierStorageFiles);
        when(storageFileHelper.getFiles(glacierStorageFiles)).thenReturn(files);

        // Call the method under test.
        expireRestoredBusinessObjectDataHelperServiceImpl.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // Verify the external calls.
        verify(storageHelper).getS3FileTransferRequestParamsDto();
        verify(s3Service).listDirectory(any(S3FileTransferRequestParamsDto.class), eq(true));
        verify(storageFileHelper).createStorageFilesFromS3ObjectSummaries(glacierS3Files);
        verify(storageFileHelper).getFiles(glacierStorageFiles);
        verify(s3Service).restoreObjects(finalS3FileTransferRequestParamsDto, 1, null);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageUnitStorageUnitNotRestored()
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a storage unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(STORAGE_UNIT_STATUS);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Mock the external calls.
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            expireRestoredBusinessObjectDataHelperServiceImpl.getStorageUnit(STORAGE_NAME, businessObjectDataEntity);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    STORAGE_NAME, StorageUnitStatusEntity.RESTORED, STORAGE_UNIT_STATUS, BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataEntityAltKeyToString(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testPrepareToExpireStorageUnit()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create a new storage unit status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.EXPIRING);

        // Create an old storage unit status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.RESTORED);

        // Create a list of storage file entities.
        List<StorageFileEntity> storageFileEntities = Lists.newArrayList(new StorageFileEntity());

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);
        storageUnitEntity.setStorageFiles(storageFileEntities);

        // Create a list of storage files.
        List<StorageFile> storageFiles = Lists.newArrayList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT));

        // Mock the external calls.
        when(businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey)).thenReturn(businessObjectDataKey);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME)).thenReturn(S3_ATTRIBUTE_NAME_BUCKET_NAME);
        when(storageHelper.getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true)).thenReturn(S3_BUCKET_NAME);
        when(s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey)).thenReturn(S3_KEY_PREFIX);
        when(storageFileHelper.getAndValidateStorageFiles(storageUnitEntity, S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey, false))
            .thenReturn(storageFiles);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the storage unit entity.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                // Update the storage unit status.
                storageUnitEntity.setStatus(newStorageUnitStatusEntity);
                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.EXPIRING, StorageUnitStatusEntity.EXPIRING);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Call the method under test.
        BusinessObjectDataRestoreDto result = expireRestoredBusinessObjectDataHelperServiceImpl.prepareToExpireStorageUnit(storageUnitKey);

        // Verify the external calls.
        verify(businessObjectDataHelper).createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME);
        verify(storageHelper).getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);
        verify(storageFileHelper).getAndValidateStorageFiles(storageUnitEntity, S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey, false);
        verify(storageUnitDaoHelper)
            .validateNoExplicitlyRegisteredSubPartitionInStorageForBusinessObjectData(storageEntity, businessObjectFormatEntity, businessObjectDataKey,
                S3_KEY_PREFIX);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.EXPIRING, StorageUnitStatusEntity.EXPIRING);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ENDPOINT);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, StorageUnitStatusEntity.EXPIRING,
                StorageUnitStatusEntity.RESTORED, storageFiles, NO_EXCEPTION, null, NO_BUSINESS_OBJECT_DATA), result);
    }

    @Test
    public void testPrepareToExpireStorageUnitDirectoryOnlyRegistration()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create a new storage unit status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.EXPIRING);

        // Create an old storage unit status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.RESTORED);

        // Create a list of storage file entities.
        List<StorageFileEntity> storageFileEntities = new ArrayList<>();

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);
        storageUnitEntity.setStorageFiles(storageFileEntities);

        // Create a list of storage files.
        List<StorageFile> storageFiles = new ArrayList<>();

        // Mock the external calls.
        when(businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey)).thenReturn(businessObjectDataKey);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME)).thenReturn(S3_ATTRIBUTE_NAME_BUCKET_NAME);
        when(storageHelper.getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true)).thenReturn(S3_BUCKET_NAME);
        when(s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey)).thenReturn(S3_KEY_PREFIX);
        when(storageFileHelper.getAndValidateStorageFiles(storageUnitEntity, S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey, false))
            .thenReturn(storageFiles);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the storage unit entity.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                // Update the storage unit status.
                storageUnitEntity.setStatus(newStorageUnitStatusEntity);
                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.EXPIRING, StorageUnitStatusEntity.EXPIRING);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Call the method under test.
        BusinessObjectDataRestoreDto result = expireRestoredBusinessObjectDataHelperServiceImpl.prepareToExpireStorageUnit(storageUnitKey);

        // Verify the external calls.
        verify(businessObjectDataHelper).createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME);
        verify(storageHelper).getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);
        verify(storageFileHelper).getAndValidateStorageFiles(storageUnitEntity, S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey, false);
        verify(storageUnitDaoHelper)
            .validateNoExplicitlyRegisteredSubPartitionInStorageForBusinessObjectData(storageEntity, businessObjectFormatEntity, businessObjectDataKey,
                S3_KEY_PREFIX);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.EXPIRING, StorageUnitStatusEntity.EXPIRING);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ENDPOINT);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, StorageUnitStatusEntity.EXPIRING,
                StorageUnitStatusEntity.RESTORED, storageFiles, NO_EXCEPTION, null, NO_BUSINESS_OBJECT_DATA), result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataHelper, configurationHelper, s3KeyPrefixHelper, s3Service, storageFileHelper,
            storageHelper, storageUnitDaoHelper);
    }
}
