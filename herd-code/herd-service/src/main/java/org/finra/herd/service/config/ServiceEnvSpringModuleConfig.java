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

import org.activiti.engine.ProcessEngineConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;

/**
 * Service environment specific Spring module configuration.
 */
@Configuration
@EnableJms
@Import({ServiceSchedulingModuleConfig.class})
public class ServiceEnvSpringModuleConfig
{
    /**
     * Returns the Activiti database schema update approach. Values should come from a "DB_SCHEMA_UPDATE_*" constant in ProcessEngineConfiguration or
     * ProcessEngineConfigurationImpl.
     *
     * @return the Activiti database schema update approach.
     */
    @Bean
    public String activitiDbSchemaUpdateParam()
    {
        // Don't create or update the database schema in the main application. This should be explicitly handled by a DBA.
        return ProcessEngineConfiguration.DB_SCHEMA_UPDATE_FALSE;
    }

    /**
     * Returns whether Quartz tables should be created.
     *
     * @return false.
     */
    @Bean
    public Boolean createQuartzTables()
    {
        // Don't create Quartz tables in the main application since they are created by the DBA.
        return false;
    }
}
