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
package org.finra.herd.dao.helper;

import com.amazonaws.ClientConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

/**
 * A base helper class that provides AWS functions.
 */
@Component
public class AwsHelper
{
    private static final int BITS_PER_BYTE = 8;
    private static final String SIGNER_OVERRIDE_V4 = "AWSS3V4SignerType";

    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected HerdStringHelper herdStringHelper;

    @Autowired
    protected RetryPolicyFactory retryPolicyFactory;

    /**
     * Constructs awsParamsDto with AWS parameters.
     *
     * @return the AWS params DTO object.
     */
    public AwsParamsDto getAwsParamsDto()
    {
        // Get HTTP proxy configuration settings.
        String httpProxyHost = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        Integer httpProxyPort = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);

        // Get AWS region name.
        String awsRegionName = configurationHelper.getProperty(ConfigurationValue.AWS_REGION_NAME);

        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
        awsParamsDto.setAwsRegionName(awsRegionName);

        return awsParamsDto;
    }

    /**
     * Creates a client configuration object that contains client configuration options such as proxy settings and max retry attempts.
     *
     * @param awsParamsDto the AWS related parameters that contain optional proxy information
     *
     * @return the client configuration object
     */
    public ClientConfiguration getClientConfiguration(AwsParamsDto awsParamsDto)
    {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Set a retry policy.
        clientConfiguration.withRetryPolicy(retryPolicyFactory.getRetryPolicy());

        // If the proxy hostname and port both are configured, set the HTTP proxy information.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()) && awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.withProxyHost(awsParamsDto.getHttpProxyHost()).withProxyPort(awsParamsDto.getHttpProxyPort());
        }

        return clientConfiguration;
    }

    /**
     * Creates a client configuration object for AWS S3 client that contains client configuration options such as proxy settings and max retry attempts.
     *
     * @param params the AWS related parameters that contain optional proxy information
     *
     * @return the client configuration object
     */
    public ClientConfiguration getS3ClientConfiguration(S3FileTransferRequestParamsDto params)
    {
        ClientConfiguration clientConfiguration = getClientConfiguration(params);

        // Set the proxy configuration, if proxy is specified.
        if (StringUtils.isNotBlank(params.getHttpProxyHost()) && params.getHttpProxyPort() != null)
        {
            clientConfiguration.setProxyHost(params.getHttpProxyHost());
            clientConfiguration.setProxyPort(params.getHttpProxyPort());
        }

        // Sign all S3 API's with V4 signing.
        // AmazonS3Client.upgradeToSigV4 already has some scenarios where it will "upgrade" the signing approach to use V4 if not already present (e.g.
        // GetObjectRequest and KMS PutObjectRequest), but setting it here (especially when KMS is used) will ensure it isn't missed when required (e.g.
        // copying objects between KMS encrypted buckets). Otherwise, AWS will return a bad request error and retry which isn't desirable.
        clientConfiguration.setSignerOverride(SIGNER_OVERRIDE_V4);

        // Set the optional socket timeout, if configured.
        if (params.getSocketTimeout() != null)
        {
            clientConfiguration.setSocketTimeout(params.getSocketTimeout());
        }

        return clientConfiguration;
    }

    /**
     * Returns transfer rate in kBytes/s. Please note that bytes->kBytes and ms->seconds conversions cancel each other (both use conversion factor of 1000).
     *
     * @param totalBytesTransferred Number of bytes transferred.
     * @param durationMillis Duration in milliseconds.
     *
     * @return the transfer rate in kBytes/s.
     */
    public Double getTransferRateInKilobytesPerSecond(Long totalBytesTransferred, Long durationMillis)
    {
        return totalBytesTransferred.doubleValue() / durationMillis;
    }

    /**
     * Returns transfer rate in Mbit/s (Decimal prefix: 1 Mbit/s = 1,000,000 bit/s).
     *
     * @param totalBytesTransferred Number of bytes transferred.
     * @param durationMillis Duration in milliseconds.
     *
     * @return the transfer rate in Mbit/s.
     */
    public Double getTransferRateInMegabitsPerSecond(Long totalBytesTransferred, Long durationMillis)
    {
        return totalBytesTransferred.doubleValue() * BITS_PER_BYTE / durationMillis / 1000;
    }
}
