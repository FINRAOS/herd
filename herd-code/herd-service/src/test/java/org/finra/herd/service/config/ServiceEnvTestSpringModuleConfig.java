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
package org.finra.herd.service.config;

import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This is a testing convenience configuration that imports the appropriate test configurations.
 */
@Configuration
public class ServiceEnvTestSpringModuleConfig
{
    /**
     * Returns the Activiti database schema update approach. Values should come from a "DB_SCHEMA_UPDATE_*" constant in ProcessEngineConfiguration or
     * ProcessEngineConfigurationImpl.
     *
     * @return the the Activiti database schema update approach.
     */
    @Bean
    public String activitiDbSchemaUpdateParam()
    {
        // Create a new schema for testing purposes.
        return ProcessEngineConfigurationImpl.DB_SCHEMA_UPDATE_CREATE;
    }

    /**
     * Returns whether Quartz tables should be created.
     *
     * @return true.
     */
    @Bean
    public Boolean createQuartzTables()
    {
        // Create Quartz tables for JUnits.
        return true;
    }
}
