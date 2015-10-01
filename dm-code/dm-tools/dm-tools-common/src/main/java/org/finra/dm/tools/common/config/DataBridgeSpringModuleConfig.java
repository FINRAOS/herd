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
package org.finra.dm.tools.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.helper.DmStringHelper;
import org.finra.dm.dao.helper.JavaPropertiesHelper;
import org.finra.dm.dao.impl.S3DaoImpl;
import org.finra.dm.service.S3Service;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.service.helper.StorageFileHelper;
import org.finra.dm.service.impl.S3ServiceImpl;

/**
 * Data Bridge Spring module configuration. We are only defining specific beans we require to run the uploader and downloader applications.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = {"org.finra.dm.tools.uploader", "org.finra.dm.tools.downloader", "org.finra.dm.tools.common.databridge"},
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
        pattern = {"org\\.finra\\.dm\\.tools\\.uploader\\.config\\..*", "org\\.finra\\.dm\\.tools\\.downloader\\.config\\..*",
            "org\\.finra\\.dm\\.tools\\.common\\.config\\..*"}))
public class DataBridgeSpringModuleConfig
{
    @Bean
    public DmHelper dmHelper()
    {
        return new DmHelper();
    }

    @Bean
    public StorageFileHelper storageFileHelper()
    {
        return new StorageFileHelper();
    }

    @Bean
    public DmStringHelper dmStringHelper()
    {
        return new DmStringHelper();
    }

    // This dependency is required when DmStringHelper is used.
    @Bean
    public ConfigurationHelper configurationHelper()
    {
        return new ConfigurationHelper();
    }

    @Bean
    public S3Service s3Service()
    {
        return new S3ServiceImpl();
    }

    @Bean
    public S3Dao s3Dao()
    {
        return new S3DaoImpl();
    }

    // This dependency is required when S3Dao is used.
    @Bean
    public JavaPropertiesHelper javaPropertiesHelper()
    {
        return new JavaPropertiesHelper();
    }
}