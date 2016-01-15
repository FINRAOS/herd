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
package org.finra.herd.service.activiti;

import org.activiti.engine.impl.variable.LongStringType;

import org.activiti.engine.impl.variable.VariableType;
import org.apache.commons.lang3.StringUtils;
import org.activiti.engine.impl.variable.StringType;
import static org.junit.Assert.assertEquals;
import org.activiti.engine.impl.variable.DefaultVariableTypes;
import org.activiti.spring.SpringProcessEngineConfiguration;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the HerdProcessEngineConfigurator class.
 *
 */
public class HerdProcessEngineConfiguratorTest extends AbstractServiceTest
{
    @Autowired
    HerdProcessEngineConfigurator herdProcessEngineConfigurator;

    /**
     * Activiti by default configures StringType and LongStringType VariableType in configuration.
     * This method tests the scenarios where no StringType and LongStringType is already configured in configuration.
     */
    @Test
    public void testNoStringAndLongStringType() throws Exception
    {
        SpringProcessEngineConfiguration configuration = new SpringProcessEngineConfiguration();
        configuration.setVariableTypes(new DefaultVariableTypes());
        
        herdProcessEngineConfigurator.configure(configuration);
        VariableType type = configuration.getVariableTypes().findVariableType(StringUtils.repeat("a", 2000));
        assertEquals(StringType.class, type.getClass());
        
        type = configuration.getVariableTypes().findVariableType(StringUtils.repeat("a", 2001));
        assertEquals(LongStringType.class, type.getClass());
    }
}