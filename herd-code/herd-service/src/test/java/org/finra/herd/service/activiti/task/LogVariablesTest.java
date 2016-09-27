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
package org.finra.herd.service.activiti.task;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the LogVariables class.
 */
public class LogVariablesTest extends AbstractServiceTest
{
    @Test
    public void testLogVariables() throws Exception
    {
        // Run a job with Activiti XML that will log variables. The XML will only log 1 of the 2 variables present in the test workflow.
        // Since we can't really test what got logged, we'll just ensure that no exceptions were thrown and that the job isn't null.
        Job job = jobServiceTestHelper.createJob(ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH);
        assertNotNull(job);
    }

    @Test
    public void testLogVariablesDm() throws Exception
    {
        // This does the same thing as the testLogVariables test above, but uses a workflow that uses a deprecated service task.
        // This test can be removed once all deprecated service tasks are removed.
        Job job = jobServiceTestHelper.createJob(ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH_DM);
        assertNotNull(job);
    }

    @Test
    public void testLogVariablesNoRegex() throws Exception
    {
        // Run a job with Activiti XML that will log variables. The XML will log all variables present in the test workflow because no Regex is specified.
        Job job = jobServiceTestHelper.createJob(ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH);
        assertNotNull(job);
    }
}
