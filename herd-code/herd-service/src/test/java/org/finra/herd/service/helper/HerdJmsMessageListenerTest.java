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

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
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
    HerdHelper herdHelper;

    @Autowired
    JsonHelper jsonHelper;

    @Autowired
    UploadDownloadService uploadDownloadService;

    @Test
    public void testSystemMonitorMessage() throws Exception
    {
        herdJmsMessageListener.processMessage(getTestSystemMonitorIncomingMessage(), null);
    }

    @Test
    public void testS3Message() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        String filePath = resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity(filePath, 0L, null, null), null);

        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);
        Logger.getLogger(HerdJmsMessageListener.class).setLevel(Level.OFF);

        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageS3FileSizeMismatch() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(
            createUploadSingleInitiationRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, MockS3OperationsImpl.MOCK_S3_FILE_NAME_0_BYTE_SIZE));

        String filePath = resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity(filePath, 0L, null, null), null);

        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);
        Logger.getLogger(HerdJmsMessageListener.class).setLevel(Level.OFF);

        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageNoKey() throws Exception
    {
        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity("key_does_not_exist", 0L, null, null), null);
        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);
        Logger.getLogger(HerdJmsMessageListener.class).setLevel(Level.OFF);

        herdJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
    }

    @Test
    public void testS3MessageWrongMessage() throws Exception
    {
        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);
        Logger.getLogger(HerdJmsMessageListener.class).setLevel(Level.OFF);

        herdJmsMessageListener.processMessage("WRONG_MESSAGE", null);
    }
}