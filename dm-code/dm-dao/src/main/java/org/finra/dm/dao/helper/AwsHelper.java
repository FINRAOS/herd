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
package org.finra.dm.dao.helper;

import com.amazonaws.ClientConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * A base helper class that provides AWS functions.
 */
@Component
public class AwsHelper
{
    @Autowired
    protected ConfigurationHelper configurationHelper;

    // dmHelper to access helper methods
    @Autowired
    protected DmStringHelper dmStringHelper;

    /**
     * Constructs awsParamsDto with AWS parameters.
     *
     * @return the AWS params DTO object.
     */
    public AwsParamsDto getAwsParamsDto()
    {
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Get HTTP proxy configuration settings.
        String httpProxyHost = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        Integer httpProxyPort = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);

        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);

        return awsParamsDto;
    }

    /**
     * Returns client configuration options that might contain proxy settings set per specified AWS parameters DTO.
     *
     * @param awsParamsDto the AWS params DTO object
     *
     * @return the client configuration options
     */
    public ClientConfiguration getClientConfiguration(AwsParamsDto awsParamsDto)
    {
        ClientConfiguration clientConfiguration;

        // Only set the proxy hostname and port if they're both configured.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()) && awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration = new ClientConfiguration().withProxyHost(awsParamsDto.getHttpProxyHost()).withProxyPort(awsParamsDto.getHttpProxyPort());
        }
        else
        {
            clientConfiguration = new ClientConfiguration();
        }

        return clientConfiguration;
    }
}
