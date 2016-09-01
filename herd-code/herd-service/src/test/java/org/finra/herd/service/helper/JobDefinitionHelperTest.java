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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.JobDefinitionAlternateKeyDto;
import org.finra.herd.service.AbstractServiceTest;

public class JobDefinitionHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetActivitiJobDefinitionTemplateUsingConfiguredActivitiIdTemplate() throws Exception
    {
        // Set up test values.
        String testActivitiIdTemplate = "testActivitiIdTemplate";

        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), testActivitiIdTemplate);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            assertEquals(testActivitiIdTemplate, jobDefinitionHelper.getActivitiJobDefinitionTemplate());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testGetActivitiJobDefinitionTemplateUsingDefaultActivitiIdTemplate()
    {
        assertEquals("~namespace~.~jobName~", jobDefinitionHelper.getActivitiJobDefinitionTemplate());
    }

    @Test
    public void testGetJobDefinitionKey()
    {
        // Set up test values.
        String testProcessDefinitionKey = String.format("%s.%s", NAMESPACE, JOB_NAME);

        // Validate the happy path scenario.
        assertEquals(new JobDefinitionAlternateKeyDto(NAMESPACE, JOB_NAME), jobDefinitionHelper.getJobDefinitionKey(testProcessDefinitionKey));
    }

    @Test
    public void testGetJobDefinitionKeyInvalidProcessDefinitionKey()
    {
        // Set up test values.
        String testProcessDefinitionKey = "INVALID_PROCESS_DEFINITION_KEY";

        // Try to get the job definition key when process definition key not match the expected pattern.
        try
        {
            jobDefinitionHelper.getJobDefinitionKey(testProcessDefinitionKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Process definition key \"%s\" does not match the expected pattern.", testProcessDefinitionKey), e.getMessage());
        }
    }
}
