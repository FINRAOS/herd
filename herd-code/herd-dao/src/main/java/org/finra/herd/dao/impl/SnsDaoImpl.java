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

import com.amazonaws.services.sns.model.PublishResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.SnsDao;
import org.finra.herd.dao.SnsOperations;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * The SNS DAO implementation.
 */
@Repository
public class SnsDaoImpl implements SnsDao
{
    @Autowired
    private AwsClientFactory awsClientFactory;

    @Autowired
    private SnsOperations snsOperations;

    @Override
    public PublishResult publish(AwsParamsDto awsParamsDto, String topicArn, String messageText)
    {
        return snsOperations.publish(topicArn, messageText, awsClientFactory.getAmazonSNSClient(awsParamsDto));
    }
}
