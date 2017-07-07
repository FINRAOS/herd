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

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import org.finra.herd.dao.SnsOperations;

public class SnsOperationsImpl implements SnsOperations
{
    @Override
    public PublishResult publish(String topicArn, String messageText, Map<String, MessageAttributeValue> messageAttributes, AmazonSNS amazonSNS)
    {
        return amazonSNS.publish(new PublishRequest().withTopicArn(topicArn).withMessage(messageText).withMessageAttributes(messageAttributes));
    }
}
