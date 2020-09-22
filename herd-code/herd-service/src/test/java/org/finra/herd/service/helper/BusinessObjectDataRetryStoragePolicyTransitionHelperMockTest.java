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
package org.finra.herd.service.helper;

import static org.finra.herd.core.AbstractCoreTest.INTEGER_VALUE;
import static org.finra.herd.core.AbstractCoreTest.STRING_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.AWS_SQS_QUEUE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.DATA_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.MESSAGE_TEXT;
import static org.finra.herd.dao.AbstractDaoTest.NO_SUBPARTITION_VALUES;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_POLICY_NAME;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_POLICY_NAMESPACE_CD;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_POLICY_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.TEST_S3_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class BusinessObjectDataRetryStoragePolicyTransitionHelperMockTest
{
    @Mock
    private AwsHelper awsHelper;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @InjectMocks
    private BusinessObjectDataRetryStoragePolicyTransitionHelper businessObjectDataRetryStoragePolicyTransitionHelperImpl;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private SqsDao sqsDao;

    @Mock
    private StoragePolicyDaoHelper storagePolicyDaoHelper;

    @Mock
    private StoragePolicyHelper storagePolicyHelper;

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
    public void testPrepareToInitiateDestroy()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create business object data retry storage policy transition request.
        BusinessObjectDataRetryStoragePolicyTransitionRequest businessObjectDataRetryStoragePolicyTransitionRequest =
            new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey);

        // Create storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create storage unit status entity with ARCHIVING status.
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(StorageUnitStatusEntity.ARCHIVING);

        // Create a storage file entity.
        StorageFileEntity storageFileEntity = new StorageFileEntity();

        // Create storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        storageUnitEntity.setStorageFiles(Collections.singletonList(storageFileEntity));

        // Create storage policy entity.
        StoragePolicyEntity storagePolicyEntity = new StoragePolicyEntity();
        storagePolicyEntity.setStorage(storageEntity);
        storagePolicyEntity.setVersion(STORAGE_POLICY_VERSION);

        // Create AWS parameter DTO.
        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setAwsAccessKeyId(STRING_VALUE);

        // Create a storage policy selection.
        StoragePolicySelection storagePolicySelection = new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, STORAGE_POLICY_VERSION);

        // Create business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(INTEGER_VALUE);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storagePolicyDaoHelper.getStoragePolicyEntityByKey(storagePolicyKey)).thenReturn(storagePolicyEntity);
        when(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity)).thenReturn(storageUnitEntity);
        when(s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey)).thenReturn(TEST_S3_KEY_PREFIX);
        when(configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME)).thenReturn(AWS_SQS_QUEUE_NAME);
        when(storagePolicyHelper.getStoragePolicyKey(storagePolicyEntity)).thenReturn(storagePolicyKey);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(jsonHelper.objectToJson(storagePolicySelection)).thenReturn(MESSAGE_TEXT);
        when(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataRetryStoragePolicyTransitionHelperImpl
            .retryStoragePolicyTransition(businessObjectDataKey, businessObjectDataRetryStoragePolicyTransitionRequest);

        // Validate the results.
        assertEquals(businessObjectData, result);

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(storagePolicyHelper).validateStoragePolicyKey(storagePolicyKey);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storagePolicyDaoHelper).getStoragePolicyEntityByKey(storagePolicyKey);
        verify(storagePolicyHelper).storagePolicyKeyAndVersionToString(storagePolicyKey, STORAGE_POLICY_VERSION);
        verify(businessObjectDataHelper, times(2)).businessObjectDataEntityAltKeyToString(businessObjectDataEntity);
        verify(storagePolicyDaoHelper).validateStoragePolicyFilterStorage(storageEntity);
        verify(storageUnitDao).getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);
        verify(s3KeyPrefixHelper).buildS3KeyPrefix(storageEntity, businessObjectFormatEntity, businessObjectDataKey);
        verify(businessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);
        verify(storageUnitDaoHelper)
            .validateNoExplicitlyRegisteredSubPartitionInStorageForBusinessObjectData(storageEntity, businessObjectFormatEntity, businessObjectDataKey,
                TEST_S3_KEY_PREFIX);
        verify(configurationHelper).getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);
        verify(storagePolicyHelper).getStoragePolicyKey(storagePolicyEntity);
        verify(awsHelper).getAwsParamsDto();
        verify(jsonHelper).objectToJson(storagePolicySelection);
        verify(sqsDao).sendMessage(awsParamsDto, AWS_SQS_QUEUE_NAME, MESSAGE_TEXT, null);
        verify(businessObjectDataHelper).createBusinessObjectDataFromEntity(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsHelper, businessObjectDataDaoHelper, businessObjectDataHelper, configurationHelper, jsonHelper, s3KeyPrefixHelper, sqsDao,
            storagePolicyDaoHelper, storagePolicyHelper, storageUnitDao, storageUnitDaoHelper);
    }
}
