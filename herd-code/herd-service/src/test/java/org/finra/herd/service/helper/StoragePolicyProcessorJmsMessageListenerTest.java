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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the StoragePolicyProcessorJmsMessageListener.
 */
public class StoragePolicyProcessorJmsMessageListenerTest extends AbstractServiceTest
{
    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    StoragePolicyProcessorJmsMessageListener storagePolicyProcessorJmsMessageListener;


    @Configuration
    static class ContextConfiguration
    {
        @Bean(name = "org.springframework.jms.config.internalJmsListenerEndpointRegistry")
        JmsListenerEndpointRegistry registry()
        {
            //if Mockito not found return null
            try
            {
                Class.forName("org.mockito.Mockito");
            }
            catch (ClassNotFoundException ignored)
            {
                return null;
            }

            return Mockito.mock(JmsListenerEndpointRegistry.class);
        }
    }

    @Test
    public void testProcessMessage() throws Exception
    {
        // Build the expected S3 key prefix for test business object data.
        String s3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Create S3FileTransferRequestParamsDto to access the source and destination S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto sourceS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(s3KeyPrefix + "/").build();
        S3FileTransferRequestParamsDto destinationS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_2).s3KeyPrefix(S3_BUCKET_NAME + "/" + s3KeyPrefix + "/").build();

        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

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
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        try
        {
            // Put relative S3 files into the source S3 bucket.
            for (StorageFile storageFile : sourceStorageFiles)
            {
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), null), null);
            }

            // Perform a storage policy transition.
            storagePolicyProcessorJmsMessageListener
                .processMessage(jsonHelper.objectToJson(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION)), null);

            // Validate the status of the source storage unit.
            assertEquals(StorageUnitStatusEntity.DISABLED, sourceStorageUnitEntity.getStatus().getCode());

            // Retrieve and validate the destination storage unit.
            StorageUnitEntity destinationStorageUnitEntity =
                storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(sourceStorageUnitEntity.getBusinessObjectData(), STORAGE_NAME_2);
            assertEquals(StorageUnitStatusEntity.ENABLED, destinationStorageUnitEntity.getStatus().getCode());
            assertEquals(0, destinationStorageUnitEntity.getStorageFiles().size());

            // Validate that source S3 data is deleted.
            assertTrue(s3Dao.listDirectory(sourceS3FileTransferRequestParamsDto).isEmpty());

            // Validate that we have the copied S3 files at the expected S3 location.
            assertEquals(sourceStorageFiles.size(), s3Dao.listDirectory(destinationS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(sourceS3FileTransferRequestParamsDto, destinationS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testProcessMessageBusinessObjectDataNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Perform a storage policy transition.
        executeWithoutLogging(StoragePolicyProcessorJmsMessageListener.class, () -> {
            storagePolicyProcessorJmsMessageListener
                .processMessage(jsonHelper.objectToJson(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION)), null);
        });
    }

    @Test
    public void testControlListener()
    {
        configurationHelper = Mockito.mock(ConfigurationHelper.class);

        ReflectionTestUtils.setField(storagePolicyProcessorJmsMessageListener, "configurationHelper", configurationHelper);
        MessageListenerContainer mockMessageListenerContainer = Mockito.mock(MessageListenerContainer.class);

        //The listener is not enabled
        when(configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_JMS_LISTENER_ENABLED)).thenReturn("false");
        JmsListenerEndpointRegistry registry = ApplicationContextHolder.getApplicationContext()
            .getBean("org.springframework.jms.config.internalJmsListenerEndpointRegistry", JmsListenerEndpointRegistry.class);
        when(registry.getListenerContainer(HerdJmsDestinationResolver.SQS_DESTINATION_STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE))
            .thenReturn(mockMessageListenerContainer);
        //the listener is not running, nothing happened
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        storagePolicyProcessorJmsMessageListener.controlStoragePolicyProcessorJmsMessageListener();
        verify(mockMessageListenerContainer, Mockito.times(0)).stop();
        verify(mockMessageListenerContainer, Mockito.times(0)).start();
        // the listener is running, but it is not enable, should stop
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        storagePolicyProcessorJmsMessageListener.controlStoragePolicyProcessorJmsMessageListener();
        verify(mockMessageListenerContainer).stop();

        //The listener is enabled
        when(configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_JMS_LISTENER_ENABLED)).thenReturn("true");
        //the listener is running, should not call the start method
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        storagePolicyProcessorJmsMessageListener.controlStoragePolicyProcessorJmsMessageListener();
        verify(mockMessageListenerContainer, Mockito.times(0)).start();
        // the listener is not running, but it is enabled, should start        
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        storagePolicyProcessorJmsMessageListener.controlStoragePolicyProcessorJmsMessageListener();
        verify(mockMessageListenerContainer).start();
    }
}