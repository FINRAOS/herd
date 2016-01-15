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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import com.amazonaws.ClientConfiguration;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the AwsHelper class.
 */
public class AwsHelperTest extends AbstractDaoTest
{
    @Autowired
    private AwsHelper awsHelper;

    @Test
    public void testGetAwsParamsDto() throws Exception
    {
        // Get AWS parameters DTO.
        AwsParamsDto resultAwsParamsDto = awsHelper.getAwsParamsDto();

        // Validate the results.
        // Since local users could set environment variables with a real HTTP proxy to test the real application, we can't update the environment to test
        // specific values. Instead, we can only test that the returned DTO contains the values in the environment.
        assertEquals(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST), resultAwsParamsDto.getHttpProxyHost());
        assertEquals(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class), resultAwsParamsDto.getHttpProxyPort());
        assertNotNull(resultAwsParamsDto);
    }

    @Test
    public void testGetClientConfiguration() throws Exception
    {
        // Try to get AWS parameters using all possible permutations of HTTP proxy settings.
        for (String testHttpProxyHost : Arrays.asList(STRING_VALUE, BLANK_TEXT, null))
        {
            for (Integer testHttpProxyPort : Arrays.asList(INTEGER_VALUE, null))
            {
                // Create AWS parameters DTO.
                AwsParamsDto testAwsParamsDto = awsHelper.getAwsParamsDto();
                testAwsParamsDto.setHttpProxyHost(testHttpProxyHost);
                testAwsParamsDto.setHttpProxyPort(testHttpProxyPort);

                // Get client configuration.
                ClientConfiguration resultClientConfiguration = awsHelper.getClientConfiguration(testAwsParamsDto);

                // Validate the results.
                assertNotNull(resultClientConfiguration);
                // The proxy settings are set only when both host and port are specified in the AWS parameters DTO.
                if (STRING_VALUE.equals(testHttpProxyHost) && INTEGER_VALUE.equals(testHttpProxyPort))
                {
                    assertEquals(testHttpProxyHost, resultClientConfiguration.getProxyHost());
                    assertEquals(testHttpProxyPort, Integer.valueOf(resultClientConfiguration.getProxyPort()));
                }
                else
                {
                    assertNull(resultClientConfiguration.getProxyHost());
                    assertEquals(-1, resultClientConfiguration.getProxyPort());
                }
            }
        }
    }
}
