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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.SqsOperations;

/**
 * Mock implementation of AWS SQS operations.
 */
public class MockSqsOperationsImpl implements SqsOperations
{
    public static final String MOCK_SQS_QUEUE_NOT_FOUND_NAME = "mock_sqs_queue_not_found_name";

    @Override
    public SendMessageResult sendMessage(String queueName, String messageText, AmazonSQS amazonSQS)
    {
        // Throw a throttling exception for a specific queue name for testing purposes.
        if (queueName.equals(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
        {
            AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
            throttlingException.setErrorCode("ThrottlingException");
            throw throttlingException;
        }

        // Throw an illegal state exception for a specific queue name for testing purposes.
        if (queueName.equals(MOCK_SQS_QUEUE_NOT_FOUND_NAME))
        {
            throw new IllegalStateException(String.format("AWS SQS queue with \"%s\" name not found.", queueName));
        }

        // Nothing else to do in the normal case since our unit tests aren't reading messages once they have been published.
        return new SendMessageResult().withMessageId(AbstractDaoTest.MESSAGE_ID);
    }
}
