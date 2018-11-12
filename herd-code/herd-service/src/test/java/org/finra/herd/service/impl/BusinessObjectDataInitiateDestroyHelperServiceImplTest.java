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

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
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
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataDestroyDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

public class BusinessObjectDataInitiateDestroyHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @InjectMocks
    private BusinessObjectDataInitiateDestroyHelperServiceImpl businessObjectDataInitiateDestroyHelperServiceImpl;

    @Mock
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private HerdDao herdDao;

    @Mock
    private HerdStringHelper herdStringHelper;

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
    private StorageUnitDao storageUnitDao;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExecuteInitiateDestroyAfterStep()
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
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.DISABLING);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Create a business object data destroy parameters DTO.
        BusinessObjectDataDestroyDto businessObjectDataDestroyDto =
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

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
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLED, StorageUnitStatusEntity.DISABLED);
        when(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataInitiateDestroyHelperServiceImpl.executeInitiateDestroyAfterStep(businessObjectDataDestroyDto);

        // Verify the external calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLED, StorageUnitStatusEntity.DISABLED);
        verify(businessObjectDataHelper).createBusinessObjectDataFromEntity(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
        assertEquals(
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLED, StorageUnitStatusEntity.DISABLING, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS),
            businessObjectDataDestroyDto);
    }

    @Test
    public void testExecuteInitiateDestroyAfterStepInvalidStorageUnitStatus()
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

        // Create a storage unit status entity with an invalid status (the expected storage unit status is DISABLING).
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ARCHIVING);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Create a business object data destroy parameters DTO.
        BusinessObjectDataDestroyDto businessObjectDataDestroyDto =
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity)).thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.executeInitiateDestroyAfterStep(businessObjectDataDestroyDto);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Storage unit status is \"%s\", but must be \"%s\". Storage: {%s}, business object data: {%s}", StorageUnitStatusEntity.ARCHIVING,
                    StorageUnitStatusEntity.DISABLING, STORAGE_NAME, BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageUnitDaoHelper).getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS),
            businessObjectDataDestroyDto);
    }

    @Test
    public void testExecuteS3SpecificSteps()
    {
        runExecuteS3SpecificStepsTest();
    }

    @Test
    public void testExecuteS3SpecificStepsWithLoggerLevelSetToInfo()
    {
        String loggerName = BusinessObjectDataInitiateDestroyHelperServiceImpl.class.getName();
        LogLevel origLoggerLevel = getLogLevel(loggerName);
        setLogLevel(loggerName, LogLevel.INFO);

        try
        {
            runExecuteS3SpecificStepsTest();
        }
        finally
        {
            setLogLevel(loggerName, origLoggerLevel);
        }
    }

    @Test
    public void testGetAndValidateFinalDestroyInDaysNonPositiveValue()
    {
        // Mock the external calls.
        when(herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS)).thenReturn(0);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.getAndValidateFinalDestroyInDays();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Configuration \"%s\" must be a positive integer.", ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS.getKey()),
                e.getMessage());
        }

        // Verify the external calls.
        verify(herdStringHelper).getConfigurationValueAsInteger(ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetAndValidateStorageUnitInvalidStorageUnitStatus()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setId(ID);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a storage unit status entity with an invalid value.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(INVALID_VALUE);

        // Create a storage unit entity with an invalid storage unit status.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity))
            .thenReturn(Arrays.asList(storageUnitEntity));
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.getAndValidateStorageUnit(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Storage unit status \"%s\" is not supported by the business object data destroy feature. Storage: {%s}, business object data: {%s}",
                    INVALID_VALUE, STORAGE_NAME, BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetAndValidateStorageUnitMultipleS3StorageUnits()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setId(ID);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity))
            .thenReturn(Arrays.asList(new StorageUnitEntity(), new StorageUnitEntity()));
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.getAndValidateStorageUnit(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has multiple (%s) %s storage units. Business object data: {%s}", 2, StoragePlatformEntity.S3,
                BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetAndValidateStorageUnitNoS3StorageUnit()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setId(ID);

        // Mock the external calls.
        when(storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity))
            .thenReturn(new ArrayList<>());
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.getAndValidateStorageUnit(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no S3 storage unit. Business object data: {%s}", BUSINESS_OBJECT_DATA_KEY_AS_STRING),
                e.getMessage());
        }

        // Verify the external calls.
        verify(storageUnitDao).getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testPrepareToInitiateDestroy()
    {
        // Create an empty business object data destroy parameters DTO.
        BusinessObjectDataDestroyDto businessObjectDataDestroyDto = new BusinessObjectDataDestroyDto();

        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a retention type entity.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        businessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Create a business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create a storage platform entity.
        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(StoragePlatformEntity.S3);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setStoragePlatform(storagePlatformEntity);
        storageEntity.setName(STORAGE_NAME);

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

        // Create a list of storage files.
        List<StorageFile> storageFiles = Arrays.asList(new StorageFile(S3_KEY, FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Create a current timestamp.
        Timestamp currentTimestamp = new Timestamp(new Date().getTime());

        // Create a date representing the primary partition value that satisfies the retention threshold check.
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -(RETENTION_PERIOD_DAYS + 1));
        Date primaryPartitionValueDate = calendar.getTime();

        // Mock the external calls.
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_KEY)).thenReturn(S3_OBJECT_TAG_KEY);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_VALUE)).thenReturn(S3_OBJECT_TAG_VALUE);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_ARN)).thenReturn(S3_OBJECT_TAGGER_ROLE_ARN);
        when(configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_SESSION_NAME)).thenReturn(S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        when(herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS))
            .thenReturn(BDATA_FINAL_DESTROY_DELAY_IN_DAYS);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getDateFromString(PARTITION_VALUE)).thenReturn(primaryPartitionValueDate);
        when(herdDao.getCurrentTimestamp()).thenReturn(currentTimestamp);
        when(storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity))
            .thenReturn(Arrays.asList(storageUnitEntity));
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(true);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME)).thenReturn(S3_ATTRIBUTE_NAME_BUCKET_NAME);
        when(storageHelper.getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true)).thenReturn(S3_BUCKET_NAME);
        when(s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey)).thenReturn(TEST_S3_KEY_PREFIX);
        when(storageFileHelper.getAndValidateStorageFilesIfPresent(storageUnitEntity, TEST_S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey))
            .thenReturn(storageFiles);
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
        }).when(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.DISABLING);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the new storage unit status.
                String businessObjectDataStatus = (String) invocation.getArguments()[1];

                // Create a business object data status entity for the new business object data status.
                BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
                businessObjectDataStatusEntity.setCode(businessObjectDataStatus);

                // Update the business object data entity with the new status.
                BusinessObjectDataEntity businessObjectDataEntity = (BusinessObjectDataEntity) invocation.getArguments()[0];
                businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

                return null;
            }
        }).when(businessObjectDataDaoHelper).updateBusinessObjectDataStatus(businessObjectDataEntity, BusinessObjectDataStatusEntity.DELETED);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Call the method under test.
        businessObjectDataInitiateDestroyHelperServiceImpl.prepareToInitiateDestroy(businessObjectDataDestroyDto, businessObjectDataKey);

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_KEY);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_VALUE);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_ARN);
        verify(configurationHelper).getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_SESSION_NAME);
        verify(herdStringHelper).getConfigurationValueAsInteger(ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataHelper).getDateFromString(PARTITION_VALUE);
        verify(herdDao).getCurrentTimestamp();
        verify(storageUnitDao).getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME);
        verify(storageHelper).getStorageAttributeValueByName(S3_ATTRIBUTE_NAME_BUCKET_NAME, storageEntity, true);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);
        verify(storageFileHelper).getAndValidateStorageFilesIfPresent(storageUnitEntity, TEST_S3_KEY_PREFIX, STORAGE_NAME, businessObjectDataKey);
        verify(storageFileDaoHelper).validateStorageFilesCount(STORAGE_NAME, businessObjectDataKey, TEST_S3_KEY_PREFIX, storageFileEntities.size());
        verify(storageUnitDaoHelper).updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.DISABLING);
        verify(businessObjectDataDaoHelper).updateBusinessObjectDataStatus(businessObjectDataEntity, BusinessObjectDataStatusEntity.DELETED);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ENDPOINT);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS),
            businessObjectDataDestroyDto);
    }

    @Test
    public void testValidateBusinessObjectDataInvalidPrimaryPartitionValue()
    {
        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a retention type entity.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        businessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);

        // Mock the external calls.
        when(businessObjectDataHelper.getDateFromString(PARTITION_VALUE)).thenReturn(null);
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Primary partition value \"%s\" cannot get converted to a valid date. Business object data: {%s}", PARTITION_VALUE,
                BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper).getDateFromString(PARTITION_VALUE);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateBusinessObjectDataInvalidRetentionType()
    {
        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a retention type entity.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(INVALID_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        businessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);

        // Mock the external calls.
        when(businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)).thenReturn(BUSINESS_OBJECT_FORMAT_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Retention type \"%s\" is not supported by the business object data destroy feature. Business object format: {%s}", INVALID_VALUE,
                    BUSINESS_OBJECT_FORMAT_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatHelper).businessObjectFormatKeyToString(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateBusinessObjectDataNoRetentionInformation()
    {
        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a retention type entity.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(PARTITION_VALUE);

        // Create a business object format entity which is not the latest format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        businessObjectFormatEntity.setLatestVersion(false);

        // Create a latest version business object format entity with missing retention type.
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = new BusinessObjectFormatEntity();
        latestVersionBusinessObjectFormatEntity.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        latestVersionBusinessObjectFormatEntity.setLatestVersion(true);
        latestVersionBusinessObjectFormatEntity.setRetentionType(null);
        latestVersionBusinessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);

        // Mock the external calls.
        when(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey)).thenReturn(latestVersionBusinessObjectFormatEntity);
        when(businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)).thenReturn(BUSINESS_OBJECT_FORMAT_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Retention information is not configured for the business object format. Business object format: {%s}",
                BUSINESS_OBJECT_FORMAT_KEY_AS_STRING), e.getMessage());
        }

        // Update the latest business object format version to have retention type - PARTITION_VALUE specified but without a retention period.
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);
        latestVersionBusinessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        latestVersionBusinessObjectFormatEntity.setRetentionPeriodInDays(null);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE),
                e.getMessage());
        }

        // Update the latest business object format version to have retention type - BDATA_RETENTION_DATE specified but with a retention period.
        // Retention period in days value must only be specified for PARTITION_VALUE retention type.
        retentionTypeEntity.setCode(RetentionTypeEntity.BDATA_RETENTION_DATE);
        latestVersionBusinessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        latestVersionBusinessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A retention period in days cannot be specified for %s retention type.", RetentionTypeEntity.BDATA_RETENTION_DATE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatDao, times(3)).getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        verify(businessObjectFormatHelper, times(1)).businessObjectFormatKeyToString(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateBusinessObjectDataRetentionThresholdCheckFails()
    {
        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a retention type entity.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);
        businessObjectFormatEntity.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(PARTITION_VALUE);

        // Create a current timestamp.
        Timestamp currentTimestamp = new Timestamp(new Date().getTime());

        // Create a date representing the primary partition value that does not satisfy the retention threshold check.
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -(RETENTION_PERIOD_DAYS - 1));
        Date primaryPartitionValueDate = calendar.getTime();

        // Mock the external calls.
        when(businessObjectDataHelper.getDateFromString(PARTITION_VALUE)).thenReturn(primaryPartitionValueDate);
        when(herdDao.getCurrentTimestamp()).thenReturn(currentTimestamp);
        when(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Business object data fails retention threshold check for retention type \"%s\" with retention period of %d days. Business object data: {%s}",
                RetentionTypeEntity.PARTITION_VALUE, RETENTION_PERIOD_DAYS, BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper).getDateFromString(PARTITION_VALUE);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verify(herdDao).getCurrentTimestamp();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateStorageValidatePathPrefixNotEnabled()
    {
        // Create a storage entity without any attributes.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX)).thenReturn(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        when(storageHelper.getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true)).thenReturn(false);

        // Try to call the method under test.
        try
        {
            businessObjectDataInitiateDestroyHelperServiceImpl.validateStorage(storageEntity);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Path prefix validation must be enabled on \"%s\" storage.", STORAGE_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX);
        verify(storageHelper).getBooleanStorageAttributeValueByName(S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX, storageEntity, false, true);
        verifyNoMoreInteractionsHelper();
    }

    private void runExecuteS3SpecificStepsTest()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage file path.
        String storageFilePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Create a business object data destroy parameters DTO.
        BusinessObjectDataDestroyDto businessObjectDataDestroyDto =
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS);

        // Create an S3 file transfer parameters DTO to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer parameters DTO to be used for S3 object tagging operation.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

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
        when(s3Service.listDirectory(s3FileTransferRequestParamsDto, false)).thenReturn(actualS3Files);
        when(storageFileHelper.createStorageFilesFromS3ObjectSummaries(actualS3Files)).thenReturn(storageFilesSelectedForTagging);
        when(storageFileHelper.getFiles(storageFilesSelectedForTagging)).thenReturn(filesSelectedForTagging);

        // Call the method under test.
        businessObjectDataInitiateDestroyHelperServiceImpl.executeS3SpecificSteps(businessObjectDataDestroyDto);

        // Verify the external calls.
        verify(storageHelper).getS3FileTransferRequestParamsDto();
        verify(storageHelper).getS3FileTransferRequestParamsDtoByRole(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        verify(s3Service).listDirectory(s3FileTransferRequestParamsDto, false);
        verify(storageFileHelper).createStorageFilesFromS3ObjectSummaries(actualS3Files);
        verify(storageFileHelper).getFiles(storageFilesSelectedForTagging);
        verify(s3Service).tagObjects(updatedS3FileTransferRequestParamsDto, updatedS3ObjectTaggerParamsDto, new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE));
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS),
            businessObjectDataDestroyDto);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */

    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDaoHelper, businessObjectDataHelper, businessObjectFormatDao, businessObjectFormatHelper,
            configurationHelper, herdDao, herdStringHelper, s3KeyPrefixHelper, s3Service, storageFileDaoHelper, storageFileHelper, storageHelper,
            storageUnitDao, storageUnitDaoHelper);
    }
}
