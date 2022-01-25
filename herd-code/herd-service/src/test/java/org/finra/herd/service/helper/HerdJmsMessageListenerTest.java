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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
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
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;
import org.finra.herd.service.impl.UploadDownloadServiceImpl;

/**
 * This class tests functionality within the HerdJmsMessageListener.
 */
public class HerdJmsMessageListenerTest extends AbstractServiceTest
{
    @Autowired
    HerdJmsMessageListener herdJmsMessageListener;

    @Autowired
    JsonHelper jsonHelper;

    @Autowired
    UploadDownloadService uploadDownloadService;

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
    public void testS3Message() throws Exception
    {
        setLogLevel(UploadDownloadHelperServiceImpl.class, LogLevel.OFF);

        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse =
            uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper.createUploadSingleInitiationRequest());

        String filePath = resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity(filePath, 0L, null, null, null), null);

        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);
        setLogLevel(HerdJmsMessageListener.class, LogLevel.DEBUG);

        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageS3FileNoExists() throws Exception
    {
        setLogLevel(UploadDownloadHelperServiceImpl.class, LogLevel.OFF);

        uploadDownloadServiceTestHelper.createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadDownloadServiceTestHelper
            .createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, TARGET_S3_KEY));

        String filePath = resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity(filePath, 0L, null, null, null), null);

        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);
        setLogLevel(HerdJmsMessageListener.class, LogLevel.OFF);

        // Try to process an S3 JMS message, when source S3 file does not exist.
        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageNoKey() throws Exception
    {
        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity("key_does_not_exist", 0L, null, null, null), null);
        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);
        setLogLevel(HerdJmsMessageListener.class, LogLevel.OFF);

        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageWrongMessage() throws Exception
    {
        setLogLevel(UploadDownloadServiceImpl.class, LogLevel.OFF);
        setLogLevel(HerdJmsMessageListener.class, LogLevel.OFF);

        herdJmsMessageListener.processMessage("WRONG_MESSAGE", null);
    }

    @Test
    public void testControlListener()
    {
        configurationHelper = Mockito.mock(ConfigurationHelper.class);

        ReflectionTestUtils.setField(herdJmsMessageListener, "configurationHelper", configurationHelper);
        MessageListenerContainer mockMessageListenerContainer = Mockito.mock(MessageListenerContainer.class);

        //The listener is not enabled
        when(configurationHelper.getProperty(ConfigurationValue.JMS_LISTENER_ENABLED)).thenReturn("false");
        JmsListenerEndpointRegistry registry = ApplicationContextHolder.getApplicationContext()
            .getBean("org.springframework.jms.config.internalJmsListenerEndpointRegistry", JmsListenerEndpointRegistry.class);
        when(registry.getListenerContainer(HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING)).thenReturn(mockMessageListenerContainer);
        //the listener is not running, nothing happened
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        herdJmsMessageListener.controlHerdJmsMessageListener();
        verify(mockMessageListenerContainer, Mockito.times(0)).stop();
        verify(mockMessageListenerContainer, Mockito.times(0)).start();
        // the listener is running, but it is not enable, should stop
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        herdJmsMessageListener.controlHerdJmsMessageListener();
        verify(mockMessageListenerContainer).stop();

        //The listener is enabled
        when(configurationHelper.getProperty(ConfigurationValue.JMS_LISTENER_ENABLED)).thenReturn("true");
        //the listener is running, should not call the start method
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        herdJmsMessageListener.controlHerdJmsMessageListener();
        verify(mockMessageListenerContainer, Mockito.times(0)).start();
        // the listener is not running, but it is enabled, should start
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        herdJmsMessageListener.controlHerdJmsMessageListener();
        verify(mockMessageListenerContainer).start();
    }
}
