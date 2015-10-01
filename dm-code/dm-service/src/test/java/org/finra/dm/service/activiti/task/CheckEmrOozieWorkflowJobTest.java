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
package org.finra.dm.service.activiti.task;

import static org.junit.Assert.assertNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.dm.dao.impl.MockAwsOperationsHelper;
import org.finra.dm.dao.impl.MockOozieOperationsImpl;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.OozieWorkflowJob;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.service.activiti.ActivitiHelper;

/**
 * Tests the CheckEmrOozieWorkflowJob class.
 */
public class CheckEmrOozieWorkflowJobTest extends AbstractServiceTest
{
    @Test
    public void testCheckOozieWorkflowJob() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("oozieWorkflowJobId", MockOozieOperationsImpl.CASE_1_JOB_ID));
        parameters.add(new Parameter("verbose", "false"));

        // Run a job with Activiti XML that will check the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_CHECK_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("checkOozieWorkflowTask" + ActivitiHelper.TASK_VARIABLE_MARKER + ActivitiHelper.VARIABLE_STATUS);
        String jsonResponse = (String) variables.get("checkOozieWorkflowTask" + ActivitiHelper.TASK_VARIABLE_MARKER + BaseJavaDelegate.VARIABLE_JSON_RESPONSE);

        assertEquals(oozieJobTaskStatus, ActivitiHelper.TASK_STATUS_SUCCESS);
        assertNotNull(jsonResponse);
        
        OozieWorkflowJob oozieWorkflowJob = jsonHelper.unmarshallJsonToObject(OozieWorkflowJob.class, jsonResponse);
        
        assertNotNull(oozieWorkflowJob);
        
        assertEquals("job ID", MockOozieOperationsImpl.CASE_1_JOB_ID, oozieWorkflowJob.getId());
        assertEquals("EMR cluster name", clusterName, oozieWorkflowJob.getEmrClusterName());
        assertNotNull("job start time is null", oozieWorkflowJob.getStartTime());
        assertNull("job end time is not null", oozieWorkflowJob.getEndTime());
        assertNull("actions is not null", oozieWorkflowJob.getWorkflowActions());
    }

    @Test
    public void testCheckOozieWorkflowJobVerbose() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("oozieWorkflowJobId", MockOozieOperationsImpl.CASE_1_JOB_ID));
        parameters.add(new Parameter("verbose", "true"));

        // Run a job with Activiti XML that will check the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_CHECK_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("checkOozieWorkflowTask" + ActivitiHelper.TASK_VARIABLE_MARKER + ActivitiHelper.VARIABLE_STATUS);
        String jsonResponse = (String) variables.get("checkOozieWorkflowTask" + ActivitiHelper.TASK_VARIABLE_MARKER + BaseJavaDelegate.VARIABLE_JSON_RESPONSE);

        assertEquals(oozieJobTaskStatus, ActivitiHelper.TASK_STATUS_SUCCESS);
        assertNotNull(jsonResponse);
        
        OozieWorkflowJob oozieWorkflowJob = jsonHelper.unmarshallJsonToObject(OozieWorkflowJob.class, jsonResponse);
        
        assertNotNull(oozieWorkflowJob);
        
        assertEquals("job ID", MockOozieOperationsImpl.CASE_1_JOB_ID, oozieWorkflowJob.getId());
        assertEquals("EMR cluster name", clusterName, oozieWorkflowJob.getEmrClusterName());
        assertNotNull("job start time is null", oozieWorkflowJob.getStartTime());
        assertNull("job end time is not null", oozieWorkflowJob.getEndTime());
        assertNotNull("actions is null", oozieWorkflowJob.getWorkflowActions());
    }
}