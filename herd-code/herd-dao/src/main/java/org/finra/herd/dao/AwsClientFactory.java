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
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.AwsParamsDto;

@Component
public class AwsClientFactory
{
    @Autowired
    private AwsHelper awsHelper;

    /**
     * Creates a client for accessing Amazon SNS.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional proxy information
     *
     * @return the Amazon SNS client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonSNS getAmazonSNSClient(AwsParamsDto awsParamsDto)
    {
        // Construct and return a new client to invoke service methods on Amazon SNS using default credentials provider chain.
        return new AmazonSNSClient(awsHelper.getClientConfiguration(awsParamsDto));
    }

    /**
     * Creates a client for accessing Amazon SQS.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional proxy information
     *
     * @return the Amazon SQS client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonSQS getAmazonSQSClient(AwsParamsDto awsParamsDto)
    {
        // Construct and return a new client to invoke service methods on Amazon SQS using default credentials provider chain.
        return new AmazonSQSClient(awsHelper.getClientConfiguration(awsParamsDto));
    }

    /**
     * Creates a client for accessing Amazon EC2 service.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional AWS credentials and proxy information
     *
     * @return the Amazon EC2 client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonEC2Client getEc2Client(AwsParamsDto awsParamsDto)
    {
        // Get client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsParamsDto);

        // If specified, use the AWS credentials passed in.
        if (StringUtils.isNotBlank(awsParamsDto.getAwsAccessKeyId()))
        {
            return new AmazonEC2Client(
                new BasicSessionCredentials(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken()),
                clientConfiguration);
        }
        // Otherwise, use the default AWS credentials provider chain.
        else
        {
            return new AmazonEC2Client(clientConfiguration);
        }
    }

    /**
     * Creates a client for accessing Amazon EMR service.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional AWS credentials and proxy information
     *
     * @return the Amazon EMR client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto)
    {
        // Get client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsParamsDto);

        // If specified, use the AWS credentials passed in.
        if (StringUtils.isNotBlank(awsParamsDto.getAwsAccessKeyId()))
        {
            return new AmazonElasticMapReduceClient(
                new BasicSessionCredentials(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken()),
                clientConfiguration);
        }
        // Otherwise, use the default AWS credentials provider chain.
        else
        {
            return new AmazonElasticMapReduceClient(clientConfiguration);
        }
    }
}
