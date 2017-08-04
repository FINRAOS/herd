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
package org.finra.herd.dao;

import java.util.List;

import com.amazonaws.services.sqs.model.SendMessageResult;

import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.MessageHeader;

/**
 * A DAO for Amazon AWS SQS.
 */
public interface SqsDao
{
    /**
     * Delivers a message to the specified queue.
     *
     * @param awsParamsDto the AWS related parameters that contain optional proxy information
     * @param queueName the name of the Amazon SQS queue to which a message is sent
     * @param messageText the text of the message
     * @param messageHeaders the optional list of message headers
     *
     * @return the result the send message operation returned by the service
     */
    public SendMessageResult sendMessage(AwsParamsDto awsParamsDto, String queueName, String messageText, List<MessageHeader> messageHeaders);
}
