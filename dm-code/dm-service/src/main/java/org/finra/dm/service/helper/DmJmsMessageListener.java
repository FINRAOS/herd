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
package org.finra.dm.service.helper;

import java.net.URLDecoder;
import java.util.Map;

import com.amazonaws.services.s3.event.S3EventNotification;
import org.apache.commons.lang.CharEncoding;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import org.finra.dm.service.SqsNotificationEventService;
import org.finra.dm.service.UploadDownloadService;
import org.finra.dm.service.impl.UploadDownloadServiceImpl.CompleteUploadSingleMessageResult;

/*
 * DM JMS message listener.
 */
@Component
public class DmJmsMessageListener
{
    private static final Logger LOGGER = Logger.getLogger(DmJmsMessageListener.class);

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private UploadDownloadService uploadDownloadService;

    @Autowired
    private SqsNotificationEventService sqsNotificationEventService;

    /**
     * Processes a JMS message.
     *
     * @param payload the message payload.
     * @param allHeaders the JMS headers.
     */
    @JmsListener(destination = DmJmsDestinationResolver.SQS_DESTINATION_DM_INCOMING)
    public void processMessage(String payload, @Headers Map<Object, Object> allHeaders)
    {
        LOGGER.info(String
            .format("JMS message received from \"%s\" queue. Headers: \"%s\" Payload: \"%s\"", DmJmsDestinationResolver.SQS_DESTINATION_DM_INCOMING, allHeaders,
                payload));

        // Process the message as S3 notification.
        boolean messageProcessed = processS3Notification(payload);

        // If message is not processed as S3 notification, then process it as ESB system monitor message.
        if (!messageProcessed)
        {
            messageProcessed = processEsbSystemMonitorMessage(payload);
        }

        if (!messageProcessed)
        {
            // The message was not processed, log the error.
            LOGGER.error(String
                .format("Failed to process JMS message from \"%s\" queue. Payload: \"%s\"", DmJmsDestinationResolver.SQS_DESTINATION_DM_INCOMING, payload));
        }
    }

    /**
     * Process the message as S3 notification.
     *
     * @param payload the JMS message payload.
     *
     * @return boolean whether message was processed.
     */
    private boolean processS3Notification(String payload)
    {
        boolean messageProcessed = false;

        try
        {
            // Process messages coming from S3 bucket.
            S3EventNotification s3EventNotification = S3EventNotification.parseJson(payload);
            String objectKey = URLDecoder.decode(s3EventNotification.getRecords().get(0).getS3().getObject().getKey(), CharEncoding.UTF_8);

            // Perform the complete upload single file.
            CompleteUploadSingleMessageResult returnValues = uploadDownloadService.performCompleteUploadSingleMessage(objectKey);

            LOGGER.debug(String.format("completeUploadSingleMessageResult- SourceBusinessObjectDataKey: \"%s\", sourceOldStatus: \"%s\", " +
                "sourceNewStatus: \"%s\", TargetBusinessObjectDataKey: \"%s\", targetOldStatus: \"%s\", targetNewStatus: \"%s\"",
                dmHelper.businessObjectDataKeyToString(returnValues.getSourceBusinessObjectDataKey()), returnValues.getSourceOldStatus(),
                returnValues.getSourceNewStatus(), dmHelper.businessObjectDataKeyToString(returnValues.getTargetBusinessObjectDataKey()),
                returnValues.getTargetOldStatus(), returnValues.getTargetNewStatus()));

            messageProcessed = true;
        }
        catch (Exception e)
        {
            LOGGER.debug(String.format("Failed to process JMS message from \"%s\" queue. Payload: \"%s\" for an S3 notification.",
                DmJmsDestinationResolver.SQS_DESTINATION_DM_INCOMING, payload), e);
        }

        return messageProcessed;
    }

    /**
     * Process the message as system monitor.
     *
     * @param payload the JMS message payload.
     *
     * @return boolean whether message was processed.
     */
    private boolean processEsbSystemMonitorMessage(String payload)
    {
        boolean messageProcessed = false;

        try
        {
            sqsNotificationEventService.processSystemMonitorNotificationEvent(payload);
            messageProcessed = true;
        }
        catch (Exception e)
        {
            LOGGER.debug(String.format("Failed to process JMS message from \"%s\" queue. Payload: \"%s\" for a system monitor request.",
                DmJmsDestinationResolver.SQS_DESTINATION_DM_INCOMING, payload), e);
        }

        return messageProcessed;
    }
}
