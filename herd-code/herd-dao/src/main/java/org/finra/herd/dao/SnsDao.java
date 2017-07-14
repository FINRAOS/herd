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

import com.amazonaws.services.sns.model.PublishResult;

import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.MessageHeader;

/**
 * A DAO for Amazon AWS SNS.
 */
public interface SnsDao
{
    /**
     * Sends a message to all of a topic's subscribed endpoints.
     *
     * @param awsParamsDto the AWS related parameters that contain optional proxy information
     * @param topicArn the topic to publish the message to
     * @param messageText the text of the message
     * @param messageHeaders the optional list of message headers
     *
     * @return the result of the publish operation returned by the service
     */
    public PublishResult publish(AwsParamsDto awsParamsDto, String topicArn, String messageText, List<MessageHeader> messageHeaders);
}
