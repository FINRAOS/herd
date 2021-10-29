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
package org.finra.herd.tools.common.config;

import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.tools.common.MockApiClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.Log4jOverridableConfigurer;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.dao.helper.HerdCharacterEscapeHandler;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.MockStsOperationsImpl;

/**
 * Data Bridge environment test specific Spring module configuration.
 */
@Configuration
public class DataBridgeEnvTestSpringModuleConfig
{
    /**
     * The Log4J configuration location for the JUnit tests.
     */
    public static final String TEST_LOG4J_CONFIG_RESOURCE_LOCATION = "classpath:herd-log4j-test.xml";

    @Bean
    public S3Operations s3Operations()
    {
        return new MockS3OperationsImpl();
    }

    @Bean
    public StsOperations stsOperations()
    {
        return new MockStsOperationsImpl();
    }

    @Bean
    public HttpClientOperations httpClientOperations()
    {
        return new MockHttpClientOperationsImpl();
    }

    @Bean
    public ApiClient apiClient()
    {
        return new MockApiClient();
    }

    // This is needed in MockHttpClientOperationsImpl.
    @Bean
    public XmlHelper xmlHelper()
    {
        return new XmlHelper();
    }

    // This is needed in XmlHelper.
    @Bean
    public HerdCharacterEscapeHandler herdCharacterEscapeHandler()
    {
        return new HerdCharacterEscapeHandler();
    }

    @Bean
    public static Log4jOverridableConfigurer log4jConfigurer()
    {
        Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
        log4jConfigurer.setDefaultResourceLocation(TEST_LOG4J_CONFIG_RESOURCE_LOCATION);
        log4jConfigurer.setOverrideResourceLocation("non_existent_override_location");
        return log4jConfigurer;
    }
}
