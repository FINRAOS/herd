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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the AddEmrMasterSecurityGroup class.
 */
public class AddEmrMasterSecurityGroupTest extends AbstractServiceTest
{
    /**
     * This method tests the happy path scenario to add Security Groups.
     */
    @Test
    public void testAddSecurityGroup() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("securityGroupIds", "sg-12345|sg-54321");
        parameters.add(parameter);

        // Run a job with Activiti XML that will add SecurityGroups EMR master node.
        Job job = createJobForCreateCluster(ACTIVITI_XML_ADD_EMR_MASTER_SECURITY_GROUPS_WITH_CLASSPATH, parameters);
        assertNotNull(job);
    }

    /**
     * This method tests the scenario where no security groups are passed.
     */
    @Test
    public void testAddSecurityGroupNoGroups() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("securityGroupIds", "");
        parameters.add(parameter);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            // Run a job with Activiti XML that will add SecurityGroups EMR master node.
            Job job = createJobForCreateCluster(ACTIVITI_XML_ADD_EMR_MASTER_SECURITY_GROUPS_WITH_CLASSPATH, parameters);

            HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
            Map<String, Object> variables = hisInstance.getProcessVariables();
            String securityGroupTaskStatus =
                (String) variables.get("addSecurityGroupServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);

            assertEquals(securityGroupTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        });
    }
}