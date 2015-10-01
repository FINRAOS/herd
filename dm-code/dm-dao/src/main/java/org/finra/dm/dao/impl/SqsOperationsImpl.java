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
package org.finra.dm.dao.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

import org.finra.dm.dao.SqsOperations;

public class SqsOperationsImpl implements SqsOperations
{
    @Override
    public void sendSqsTextMessage(ClientConfiguration clientConfiguration, String queueName, String messageText)
    {
        try
        {
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(clientConfiguration);
            GetQueueUrlResult queueUrlResult = amazonSQSClient.getQueueUrl(queueName);
            amazonSQSClient.sendMessage(queueUrlResult.getQueueUrl(), messageText);
        }
        catch (QueueDoesNotExistException ex)
        {
            throw new IllegalStateException(String.format("AWS SQS queue with \"%s\" name not found.", queueName), ex);
        }
    }
}
