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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.AwsParamsDto;

@Component
public class AwsClientFactory
{
    @Autowired
    private RetryPolicyFactory retryPolicyFactory;
    
    /**
     * Create the EMR client with the given proxy and access key details.
     *
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details.
     *
     * @return the AmazonElasticMapReduceClient object.
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto)
    {
        // TODO Building EMR client every time requested, if this becomes a performance issue,
        // might need to consider storing a singleton or building the client once per request.

        ClientConfiguration clientConfiguration = new ClientConfiguration().withRetryPolicy(retryPolicyFactory.getRetryPolicy());

        // Create an EMR client with HTTP proxy information.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()) && awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.withProxyHost(awsParamsDto.getHttpProxyHost()).withProxyPort(awsParamsDto.getHttpProxyPort());
        }

        // Return the client.
        return new AmazonElasticMapReduceClient(clientConfiguration);
    }
    
    /**
     * Create the EC2 client with the given proxy and access key details This is the main AmazonEC2Client object
     *
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details
     *
     * @return the AmazonEC2Client object
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonEC2Client getEc2Client(AwsParamsDto awsParamsDto)
    {
        // TODO Building EC2 client every time requested, if this becomes a performance issue,
        // might need to consider storing a singleton or building the client once per request.

        ClientConfiguration clientConfiguration = new ClientConfiguration().withRetryPolicy(retryPolicyFactory.getRetryPolicy());

        // Create an EC2 client with HTTP proxy information.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()) && awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.withProxyHost(awsParamsDto.getHttpProxyHost()).withProxyPort(awsParamsDto.getHttpProxyPort());
        }

        // Return the client.
        return new AmazonEC2Client(clientConfiguration);
    }
}
