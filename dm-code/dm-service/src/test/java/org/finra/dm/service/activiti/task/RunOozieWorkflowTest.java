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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.dm.dao.impl.MockAwsOperationsHelper;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.model.api.xml.RunOozieWorkflowRequest;
import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the RunOozieWorkflow class.
 */
public class RunOozieWorkflowTest extends AbstractServiceTest
{
    @Test
    public void testRunOozieWorkflowXml() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        RunOozieWorkflowRequest runOozieRequest = new RunOozieWorkflowRequest(TEST_ACTIVITI_NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, 
            clusterName, OOZIE_WORKFLOW_LOCATION, null);
        
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "xml"));
        parameters.add(new Parameter("runOozieWorkflowRequest", xmlHelper.objectToXml(runOozieRequest)));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_SUCCESS);
        assertNotNull(oozieJobId);
    }

    @Test
    public void testRunOozieWorkflowJson() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        RunOozieWorkflowRequest runOozieRequest = new RunOozieWorkflowRequest(TEST_ACTIVITI_NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, 
            clusterName, OOZIE_WORKFLOW_LOCATION, null);
        
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "json"));
        parameters.add(new Parameter("runOozieWorkflowRequest", jsonHelper.objectToJson(runOozieRequest)));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_SUCCESS);
        assertNotNull(oozieJobId);
    }

    @Test
    public void testRunOozieWorkflowWrongContentType() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "wrong_content_type"));
        parameters.add(new Parameter("runOozieWorkflowRequest", "some_request"));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        assertNull(oozieJobId);
    }

    @Test
    public void testRunOozieWorkflowNoRequest() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "xml"));
        parameters.add(new Parameter("runOozieWorkflowRequest", ""));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        assertNull(oozieJobId);
    }
    
    @Test
    public void testRunOozieWorkflowWrongXmlRequest() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "xml"));
        parameters.add(new Parameter("runOozieWorkflowRequest", "wrong_xml_request"));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        assertNull(oozieJobId);
    }

    @Test
    public void testRunOozieWorkflowWrongJsonRequest() throws Exception
    {
        String clusterName = "testCluster" + Math.random();
        List<Parameter> parameters = new ArrayList<>();
        
        parameters.add(new Parameter("clusterName", clusterName));
        parameters.add(new Parameter("contentType", "json"));
        parameters.add(new Parameter("runOozieWorkflowRequest", "wrong_json_request"));

        // Run a job with Activiti XML that will run the oozie workflow.
        Job job = createJobForCreateCluster(ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH, parameters, MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);
        
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String oozieJobTaskStatus = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        String oozieJobId = (String) variables.get("runOozieWorkflowTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + RunOozieWorkflow.VARIABLE_ID);

        assertEquals(oozieJobTaskStatus, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        assertNull(oozieJobId);
    }
}