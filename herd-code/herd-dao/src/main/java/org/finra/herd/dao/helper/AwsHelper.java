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

/**
 * A base helper class that provides AWS functions.
 */
@Component
public class AwsHelper
{
    private static final int BITS_PER_BYTE = 8;

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

        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);

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
