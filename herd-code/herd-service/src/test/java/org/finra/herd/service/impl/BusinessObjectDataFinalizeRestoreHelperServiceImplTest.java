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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * This class tests functionality within the business object data finalize restore helper service implementation.
 */
public class BusinessObjectDataFinalizeRestoreHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @InjectMocks
    private BusinessObjectDataFinalizeRestoreHelperServiceImpl businessObjectDataFinalizeRestoreHelperServiceImpl;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageFileDaoHelper storageFileDaoHelper;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCompleteFinalizeRestore()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a DTO for business object data restore parameters.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT)), NO_EXCEPTION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a new storage unit status entity.
        StorageUnitStatusEntity newStorageUnitStatusEntity = new StorageUnitStatusEntity();
        newStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.RESTORED);

        // Create an old storage unit status entity.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = new StorageUnitStatusEntity();
        oldStorageUnitStatusEntity.setCode(StorageUnitStatusEntity.RESTORING);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(oldStorageUnitStatusEntity);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.RESTORED)).thenReturn(newStorageUnitStatusEntity);

        // Call the method under test.
        businessObjectDataFinalizeRestoreHelperServiceImpl.completeFinalizeRestore(businessObjectDataRestoreDto);

        // Verify the external calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(storageUnitStatusDaoHelper).getStorageUnitStatusEntity(StorageUnitStatusEntity.RESTORED);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, newStorageUnitStatusEntity, StorageUnitStatusEntity.RESTORED);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(StorageUnitStatusEntity.RESTORED, businessObjectDataRestoreDto.getNewStorageUnitStatus());
        assertEquals(StorageUnitStatusEntity.RESTORING, businessObjectDataRestoreDto.getOldStorageUnitStatus());
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
                STORAGE_UNIT_STATUS, storageFiles, NO_EXCEPTION);

        // Create an initial instance of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an updated version of S3 file transfer parameters DTO.
        S3FileTransferRequestParamsDto updatedS3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        updatedS3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        updatedS3FileTransferRequestParamsDto.setS3Endpoint(S3_ENDPOINT);
        updatedS3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX + "/");

        // Create a mock S3 object summary for S3 object that does not belong to Glacier storage class.
        S3ObjectSummary glacierS3ObjectSummary = mock(S3ObjectSummary.class);
        when(glacierS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Glacier.toString());

        // Create a mock S3 object summary for S3 object that does not belong to Glacier storage class.
        S3ObjectSummary standardS3ObjectSummary = mock(S3ObjectSummary.class);
        when(standardS3ObjectSummary.getStorageClass()).thenReturn(StorageClass.Standard.toString());

        // Create a list of S3 files.
        List<S3ObjectSummary> s3Files = Arrays.asList(glacierS3ObjectSummary, standardS3ObjectSummary);

        // Create a list of S3 objects that belong to Glacier storage class.
        List<S3ObjectSummary> glacierS3Files = Arrays.asList(glacierS3ObjectSummary);

        // Create a list of storage files that represent S3 objects of Glacier storage class.
        List<StorageFile> glacierStorageFiles = Arrays.asList(new StorageFile(S3_KEY, FILE_SIZE, ROW_COUNT));

        // Create a list of files.
        List<File> files = Arrays.asList(new File(S3_KEY));

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
        businessObjectDataFinalizeRestoreHelperServiceImpl.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // Verify the external calls.
        verify(storageHelper).getS3FileTransferRequestParamsDto();
        verify(s3Service).listDirectory(any(S3FileTransferRequestParamsDto.class), eq(true));
        verify(storageFileHelper).validateRegisteredS3Files(storageFiles, s3Files, STORAGE_NAME, businessObjectDataKey);
        verify(storageFileHelper).createStorageFilesFromS3ObjectSummaries(glacierS3Files);
        verify(storageFileHelper).getFiles(glacierStorageFiles);
        verify(s3Service).validateGlacierS3FilesRestored(finalS3FileTransferRequestParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataHelper, configurationHelper, s3KeyPrefixHelper, s3Service, storageFileDaoHelper,
            storageFileHelper, storageHelper, storageUnitDaoHelper, storageUnitStatusDaoHelper);
    }
}
