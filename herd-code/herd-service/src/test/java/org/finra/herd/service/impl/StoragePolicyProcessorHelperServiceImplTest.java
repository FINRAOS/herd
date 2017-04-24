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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StoragePolicyDaoHelper;
import org.finra.herd.service.helper.StoragePolicyHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * This class tests functionality within the storage policy processor helper service implementation.
 */
public class StoragePolicyProcessorHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageFileDao storageFileDao;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StoragePolicyDaoHelper storagePolicyDaoHelper;

    @Mock
    private StoragePolicyHelper storagePolicyHelper;

    @InjectMocks
    private StoragePolicyProcessorHelperServiceImpl storagePolicyProcessorHelperServiceImpl;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCompleteStoragePolicyTransitionImpl()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create a storage unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ARCHIVING);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Create a list of storage files.
        List<StorageFile> storageFiles = Arrays.asList(new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Create a storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
                StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ARCHIVING, storageFiles, S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE,
                S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the new storage unit status.
                String storageUnitStatus = (String) invocation.getArguments()[1];

                // Create a storage unit status entity for the new storage unit status.
                StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
                storageUnitStatusEntity.setCode(storageUnitStatus);

                // Update the storage unit with the new status.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                storageUnitEntity.setStatus(storageUnitStatusEntity);

                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVED);

        // Call the method under test.
        storagePolicyProcessorHelperServiceImpl.completeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);

        // Verify the external calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper, times(2)).businessObjectDataKeyToString(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVED);

        // Validate the results.
        assertEquals(new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
            StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVING, storageFiles, S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE,
            S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME), storagePolicyTransitionParamsDto);
    }

    @Test
    public void testExecuteStoragePolicyTransitionImpl()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage file path.
        String storageFilePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Create a list of storage files to be passed as an input.
        List<StorageFile> storageFiles = Arrays.asList(new StorageFile(storageFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Create a storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
                StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ENABLED, storageFiles, S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE,
                S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME);

        // Create an S3 file transfer parameters DTO to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer parameters DTO to be used for S3 object tagging operation.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create a list of S3 object summaries selected without zero byte directory markers.
        List<S3ObjectSummary> actualS3FilesWithoutZeroByteDirectoryMarkers = Arrays.asList(new S3ObjectSummary());

        // Create a list of all S3 files matching the S3 key prefix form the S3 bucket.
        List<S3ObjectSummary> actualS3Files = Arrays.asList(new S3ObjectSummary());

        // Create a list of storage files selected for S3 object tagging.
        List<StorageFile> storageFilesSelectedForTagging = Arrays.asList(new StorageFile());

        // Create a list of storage files selected for S3 object tagging.
        List<File> filesSelectedForTagging = Arrays.asList(new File(storageFilePath));

        // Create an updated S3 file transfer parameters DTO to access the S3 bucket.
        S3FileTransferRequestParamsDto updatedS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        updatedS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        updatedS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        updatedS3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        updatedS3FileTransferRequestParamsDto.setFiles(filesSelectedForTagging);

        // Create an updated S3 file transfer parameters DTO to be used for S3 object tagging operation.
        S3FileTransferRequestParamsDto updatedS3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        updatedS3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        updatedS3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        updatedS3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);
        updatedS3ObjectTaggerParamsDto.setS3Endpoint(S3_ENDPOINT);

        // Mock the external calls.
        when(storageHelper.getS3FileTransferRequestParamsDto()).thenReturn(s3FileTransferRequestParamsDto);
        when(storageHelper.getS3FileTransferRequestParamsDtoByRole(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME))
            .thenReturn(s3ObjectTaggerParamsDto);
        when(s3Service.listDirectory(s3FileTransferRequestParamsDto, true)).thenReturn(actualS3FilesWithoutZeroByteDirectoryMarkers);
        when(s3Service.listDirectory(s3FileTransferRequestParamsDto, false)).thenReturn(actualS3Files);
        when(storageFileHelper.createStorageFilesFromS3ObjectSummaries(actualS3Files)).thenReturn(storageFilesSelectedForTagging);
        when(storageFileHelper.getFiles(storageFilesSelectedForTagging)).thenReturn(filesSelectedForTagging);

        // Call the method under test.
        storagePolicyProcessorHelperServiceImpl.executeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);

        // Verify the external calls.
        verify(storageHelper).getS3FileTransferRequestParamsDto();
        verify(storageHelper).getS3FileTransferRequestParamsDtoByRole(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, true);
        verify(storageFileHelper).validateRegisteredS3Files(storageFiles, actualS3FilesWithoutZeroByteDirectoryMarkers, STORAGE_NAME, businessObjectDataKey);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, true);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, false);
        verify(storageFileHelper).createStorageFilesFromS3ObjectSummaries(actualS3Files);
        verify(storageFileHelper).getFiles(storageFilesSelectedForTagging);
        verify(s3Service).tagObjects(updatedS3FileTransferRequestParamsDto, updatedS3ObjectTaggerParamsDto, new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE));
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
            StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ENABLED, storageFiles, S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
            S3_OBJECT_TAGGER_ROLE_SESSION_NAME), storagePolicyTransitionParamsDto);
    }

    @Test
    public void testInitiateStoragePolicyTransitionImpl()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create a storage platform entity.
        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(StoragePlatformEntity.S3);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setStoragePlatform(storagePlatformEntity);
        storageEntity.setName(STORAGE_NAME);

        // Create a storage policy entity.
        StoragePolicyTransitionTypeEntity storagePolicyTransitionTypeEntity = new StoragePolicyTransitionTypeEntity();
        storagePolicyTransitionTypeEntity.setCode(StoragePolicyTransitionTypeEntity.GLACIER);

        // Create a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = new StoragePolicyEntity();
        storagePolicyEntity.setStorage(storageEntity);
        storagePolicyEntity.setStoragePolicyTransitionType(storagePolicyTransitionTypeEntity);

        // Create a list of storage file entities.
        List<StorageFileEntity> storageFileEntities = Arrays.asList(new StorageFileEntity());

        // Create a storage unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ENABLED);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorageFiles(storageFileEntities);
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Create a storage file path.
        String storageFilePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Create a list of storage file paths.
        List<String> storageFilePaths = Arrays.asList(storageFilePath);

        // Create a list of storage files.
        List<StorageFile> storageFiles = Arrays.asList(new StorageFile(storageFilePath, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storagePolicyDaoHelper.getStoragePolicyEntityByKeyAndVersion(storagePolicyKey, STORAGE_POLICY_VERSION)).thenReturn(storagePolicyEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE))
            .thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME)).thenReturn(S3_ATTRIBUTE_NAME_BUCKET_NAME);
        when(storageHelper.getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true)).thenReturn(S3_BUCKET_NAME);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_KEY)).thenReturn(S3_OBJECT_TAG_KEY);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_VALUE)).thenReturn(S3_OBJECT_TAG_VALUE);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN)).thenReturn(S3_OBJECT_TAGGER_ROLE_ARN);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME))
            .thenReturn(S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey)).thenReturn(TEST_S3_KEY_PREFIX);
        when(storageFileHelper.createStorageFilesFromEntities(storageFileEntities)).thenReturn(storageFiles);
        when(storageFileHelper.getFilePathsFromStorageFiles(storageFiles)).thenReturn(storageFilePaths);
        when(storageFileDao.getStorageFileCount(STORAGE_NAME, TEST_S3_KEY_PREFIX + "/")).thenReturn(Long.valueOf(storageFileEntities.size()));
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the new storage unit status.
                String storageUnitStatus = (String) invocation.getArguments()[1];

                // Create a storage unit status entity for the new storage unit status.
                StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
                storageUnitStatusEntity.setCode(storageUnitStatus);

                // Update the storage unit with the new status.
                StorageUnitEntity storageUnitEntity = (StorageUnitEntity) invocation.getArguments()[0];
                storageUnitEntity.setStatus(storageUnitStatusEntity);

                return null;
            }
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ARCHIVING);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Call the method under test.
        StoragePolicyTransitionParamsDto result = storagePolicyProcessorHelperServiceImpl
            .initiateStoragePolicyTransitionImpl(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, STORAGE_POLICY_VERSION));

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(storagePolicyHelper).validateStoragePolicyKey(storagePolicyKey);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper, times(3)).businessObjectDataKeyToString(businessObjectDataKey);
        verify(storagePolicyDaoHelper).getStoragePolicyEntityByKeyAndVersion(storagePolicyKey, STORAGE_POLICY_VERSION);
        verify(storagePolicyHelper, times(2)).storagePolicyKeyAndVersionToString(storagePolicyKey, STORAGE_POLICY_VERSION);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME);
        verify(storageHelper).getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_KEY);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_VALUE);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);
        verify(storageFileHelper).createStorageFilesFromEntities(storageFileEntities);
        verify(storageFileHelper).getFilePathsFromStorageFiles(storageFiles);
        verify(storageFileHelper).validateStorageFiles(storageFilePaths, TEST_S3_KEY_PREFIX, businessObjectDataEntity, STORAGE_NAME);
        verify(storageFileDao).getStorageFileCount(STORAGE_NAME, TEST_S3_KEY_PREFIX + "/");
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ARCHIVING);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ENDPOINT);
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
            StorageUnitStatusEntity.ARCHIVING, StorageUnitStatusEntity.ENABLED, storageFiles, S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
            S3_OBJECT_TAGGER_ROLE_SESSION_NAME), result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataHelper, configurationHelper, s3KeyPrefixHelper, s3Service, storageFileDao,
            storageFileHelper, storageHelper, storagePolicyDaoHelper, storagePolicyHelper, storageUnitDaoHelper);
    }
}
