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
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.dm.dao.SqsDao;
import org.finra.dm.dao.SqsOperations;
import org.finra.dm.model.dto.AwsParamsDto;

/**
 * The SQS DAO implementation.
 */
@Repository
public class SqsDaoImpl implements SqsDao
{
    @Autowired
    private SqsOperations sqsOperations;

    /**
     * Sends a text message to the specified AWS SQS queue.
     */
    @Override
    public void sendSqsTextMessage(AwsParamsDto awsParamsDto, String queueName, String messageText)
    {
        // Create the connection factory based on the specified proxy configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Only set the proxy hostname and/or port if they're configured.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()))
        {
            clientConfiguration.setProxyHost(awsParamsDto.getHttpProxyHost());
        }
        if (awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.setProxyPort(awsParamsDto.getHttpProxyPort());
        }

        // Send the message.
        sqsOperations.sendSqsTextMessage(clientConfiguration, queueName, messageText);
    }
}
