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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.dao.impl.HttpClientOperationsImpl;
import org.finra.herd.dao.impl.S3OperationsImpl;
import org.finra.herd.dao.impl.StsOperationsImpl;

/**
 * Data Bridge environment specific Spring module configuration.
 */
@Configuration
public class DataBridgeEnvSpringModuleConfig
{
    @Bean
    public S3Operations s3Operations()
    {
        return new S3OperationsImpl();
    }

    @Bean
    public StsOperations stsOperations()
    {
        return new StsOperationsImpl();
    }

    @Bean
    public HttpClientOperations httpClientOperations()
    {
        return new HttpClientOperationsImpl();
    }

    @Bean
    public ApiClient apiClient()
    {
        return new ApiClient();
    }
}
