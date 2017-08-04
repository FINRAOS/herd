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
package org.finra.herd.dao.impl;

import java.util.Map;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.finra.herd.dao.SqsOperations;

public class SqsOperationsImpl implements SqsOperations
{
    @Override
    public SendMessageResult sendMessage(String queueName, String messageText, Map<String, MessageAttributeValue> messageAttributes, AmazonSQS amazonSQS)
    {
        try
        {
            return amazonSQS.sendMessage(new SendMessageRequest().withQueueUrl(amazonSQS.getQueueUrl(queueName).getQueueUrl()).withMessageBody(messageText)
                .withMessageAttributes(messageAttributes));
        }
        catch (QueueDoesNotExistException e)
        {
            throw new IllegalStateException(String.format("AWS SQS queue with \"%s\" name not found.", queueName), e);
        }
    }
}
