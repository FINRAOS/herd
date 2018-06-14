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

import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.SimpleExponentialBackoffStrategy;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.HttpClientHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.S3DaoImpl;
import org.finra.herd.dao.impl.StsDaoImpl;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.impl.S3ServiceImpl;

/**
 * Data Bridge Spring module configuration. We are only defining specific beans we require to run the uploader and downloader applications.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = {"org.finra.herd.tools.uploader", "org.finra.herd.tools.downloader", "org.finra.herd.tools.retention.exporter",
    "org.finra.herd.tools.retention.destroyer",
    "org.finra.herd.tools.common.databridge"}, excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
    "org\\.finra\\.herd\\.tools\\.uploader\\.config\\..*", "org\\.finra\\.herd\\.tools\\.downloader\\.config\\..*",
    "org\\.finra\\.herd\\.tools\\.retention\\.exporter\\.config\\..*", "org\\.finra\\.herd\\.tools\\.common\\.config\\..*"}))
@Import(DataBridgeAopSpringModuleConfig.class)
public class DataBridgeSpringModuleConfig
{
    @Bean
    public AlternateKeyHelper alternateKeyHelper()
    {
        return new AlternateKeyHelper();
    }

    @Bean
    public AwsHelper awsHelper()
    {
        return new AwsHelper();
    }

    @Bean
    public BackoffStrategy backoffStrategy()
    {
        return new SimpleExponentialBackoffStrategy();
    }

    @Bean
    public BusinessObjectDataHelper businessObjectDataHelper()
    {
        return new BusinessObjectDataHelper();
    }

    // This dependency is required when HerdStringHelper is used.
    @Bean
    public ConfigurationHelper configurationHelper()
    {
        return new ConfigurationHelper();
    }

    @Bean
    public HerdStringHelper herdStringHelper()
    {
        return new HerdStringHelper();
    }

    @Bean
    public HttpClientHelper httpClientHelper()
    {
        return new HttpClientHelper();
    }

    // This dependency is required when S3Dao is used.
    @Bean
    public JavaPropertiesHelper javaPropertiesHelper()
    {
        return new JavaPropertiesHelper();
    }

    @Bean
    public JsonHelper jsonHelper()
    {
        return new JsonHelper();
    }

    @Bean
    public RetryPolicyFactory retryPolicyFactory()
    {
        return new RetryPolicyFactory();
    }

    @Bean
    public S3Dao s3Dao()
    {
        return new S3DaoImpl();
    }

    @Bean
    public S3Service s3Service()
    {
        return new S3ServiceImpl();
    }

    @Bean
    public StorageFileHelper storageFileHelper()
    {
        return new StorageFileHelper();
    }

    @Bean
    public StorageHelper storageHelper()
    {
        return new StorageHelper();
    }

    @Bean
    public StorageUnitHelper storageUnitHelper()
    {
        return new StorageUnitHelper();
    }

    @Bean
    public StsDao stsDao()
    {
        return new StsDaoImpl();
    }
}
