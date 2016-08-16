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
package org.finra.herd.core.config;

import java.net.MalformedURLException;

import javax.annotation.PostConstruct;
import javax.xml.parsers.FactoryConfigurationError;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import org.finra.herd.core.helper.LoggingHelper;

/**
 * This is a testing convenience configuration that imports the appropriate test configurations.
 */
@Configuration
@Import({CoreSpringModuleConfig.class, CoreEnvTestSpringModuleConfig.class})
public class CoreTestSpringModuleConfig
{
    @Autowired
    private LoggingHelper loggingHelper;

    @PostConstruct
    public void initialize() throws MalformedURLException, FactoryConfigurationError
    {
        /*
         * Explicitly set the log4j configuration file location. This configuration is only necessary at the core layer of the application. The higher layers
         * will use the configurer override provided by the DAO layer test configurations.
         */
        loggingHelper.initLogging("test", "classpath:herd-log4j-test.xml");
    }
}
