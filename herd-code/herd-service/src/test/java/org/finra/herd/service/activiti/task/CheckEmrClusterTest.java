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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.dao.impl.MockEmrOperationsImpl;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the CheckEmrCluster Activiti task wrapper.
 */
public class CheckEmrClusterTest extends AbstractServiceTest
{
    public static final String taskName = "checkClusterServiceTask";

    @Before
    public void createDatabaseEntities()
    {
        // Create EC2 on-demand pricing entities required for testing.
        ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntities();
    }

    /**
     * This method tests the check EMR cluster activiti task with cluster Id and step Id specified
     */
    @Test
    public void testCheckClusterByClusterIdStepId() throws Exception
    {
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();

        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job =
            jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(true, "false", "false"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNull(emrStepJarLocation);

        String shellStepId = (String) variables.get("addShellStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(shellStepId);
        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertEquals(shellStepId, activeStepId);
        String activeStepJarLocation =
            (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_jarLocation");
        assertNull(activeStepJarLocation);
    }

    /**
     * This method tests the check EMR cluster activiti task with cluster Id and step Id specified and requested step contains no id in response
     */
    @Test
    public void testCheckClusterByClusterIdStepIdRequestedStepHasNoId() throws Exception
    {
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();
        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter("clusterName", "testCluster1"));
        parameters.add(new Parameter("hiveStepName", MockEmrOperationsImpl.MOCK_STEP_WITHOUT_ID_NAME));
        parameters.add(new Parameter("hiveScriptLocation", "a_hive_step_location"));
        parameters.add(new Parameter("shellStepName", MockEmrOperationsImpl.MOCK_STEP_RUNNING_NAME));
        parameters.add(new Parameter("shellScriptLocation", "a_shell_step_location"));
        parameters.add(new Parameter("verbose", "false"));
        parameters.add(new Parameter("retrieveInstanceFleets", "false"));

        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job = jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(hiveStepId);
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertNull(emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNull(emrStepJarLocation);

        String shellStepId = (String) variables.get("addShellStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(shellStepId);
        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertEquals(shellStepId, activeStepId);
        String activeStepJarLocation =
            (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_jarLocation");
        assertNull(activeStepJarLocation);
    }

    /**
     * This method tests the check EMR cluster activiti task with cluster Id and step Id specified
     */
    @Test
    public void testCheckClusterByClusterIdStepIdVerbose() throws Exception
    {
        // Run a job with Activiti XML that will start cluster, check status and terminate.
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();

        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        Job job =
            jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(true, "true", "false"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(hiveStepId);
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNotNull(emrStepJarLocation);

        String shellStepId = (String) variables.get("addShellStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(shellStepId);
        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertEquals(shellStepId, activeStepId);
        String activeStepJarLocation =
            (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_jarLocation");
        assertNotNull(activeStepJarLocation);
    }

    @Test
    public void testCheckClusterByClusterIdStepIdNoActiveStep() throws Exception
    {
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();

        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job =
            jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(false, "false", "false"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(hiveStepId);
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNull(emrStepJarLocation);

        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertNull(activeStepId);
    }

    @Test
    public void testCheckClusterByClusterIdStepIdActiveStepHasNoId() throws Exception
    {
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();
        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter("clusterName", "testCluster1"));
        parameters.add(new Parameter("hiveStepName", "a_hive_step"));
        parameters.add(new Parameter("hiveScriptLocation", "a_hive_step_location"));
        parameters.add(new Parameter("shellStepName", MockEmrOperationsImpl.MOCK_STEP_RUNNING_WITHOUT_ID_NAME));
        parameters.add(new Parameter("shellScriptLocation", "a_shell_step_location"));
        parameters.add(new Parameter("verbose", "false"));
        parameters.add(new Parameter("retrieveInstanceFleets", "false"));

        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job = jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        assertNotNull(hiveStepId);
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNull(emrStepJarLocation);

        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertNull(activeStepId);
    }

    @Test
    public void testCheckCluster() throws Exception
    {
        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job = jobServiceTestHelper.createJobForCreateCluster(ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH, getParameters(true, "", "false"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        //check to be sure fields exist.  These should exist whether verbose is set or not
        assertTrue(variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_ID));
        assertTrue(variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_STATUS));
        assertTrue(variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_CREATION_TIME));
        assertTrue(variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_READY_TIME));
        assertTrue(variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_END_TIME));
        assertTrue(
            variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_STATUS_CHANGE_REASON_CODE));
        assertTrue(
            variables.containsKey(taskName + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + CheckEmrCluster.VARIABLE_EMR_CLUSTER_STATUS_CHANGE_REASON_MESSAGE));
    }

    @Test
    public void testCheckClusterByRetrieveInstanceFleets() throws Exception
    {
        // Run a job with Activiti XML that will start cluster, check status and terminate.
        List<FieldExtension> fieldExtensions = getOptionalFieldExtensions();

        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrStepId");
        fieldExtension.setExpression("${addHiveStepServiceTask_emrStepId}");
        fieldExtensions.add(fieldExtension);

        Job job =
            jobServiceTestHelper.createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(true, "true", "true"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();
        String emrClusterInstanceFleetJson = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "instance_fleets");
        assertNotNull(emrClusterInstanceFleetJson);
    }

    private List<Parameter> getParameters(boolean isShellStepRunning, String verbose, String retrieveInstanceFleets)
    {
        List<Parameter> parameters = new ArrayList<>();

        parameters.add(new Parameter("clusterName", "testCluster1"));
        parameters.add(new Parameter("hiveStepName", "a_hive_step"));
        parameters.add(new Parameter("hiveScriptLocation", "a_hive_step_location"));
        parameters.add(new Parameter("shellStepName", isShellStepRunning ? MockEmrOperationsImpl.MOCK_STEP_RUNNING_NAME : "a_shell_step"));
        parameters.add(new Parameter("shellScriptLocation", "a_shell_step_location"));
        parameters.add(new Parameter("verbose", verbose));
        parameters.add(new Parameter("retrieveInstanceFleets", retrieveInstanceFleets));

        return parameters;
    }

    private String getCheckClusterActivitiXml(List<FieldExtension> checkClusterFieldExtensions) throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("checkClusterServiceTask");

        serviceTask.getFieldExtensions().addAll(checkClusterFieldExtensions);

        return getActivitiXmlFromBpmnModel(bpmnModel);
    }

    private List<FieldExtension> getOptionalFieldExtensions()
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        FieldExtension fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("emrClusterId");
        fieldExtension.setExpression("${createClusterServiceTask_emrClusterId}");
        fieldExtensionList.add(fieldExtension);

        fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("verbose");
        fieldExtension.setExpression("${verbose}");
        fieldExtensionList.add(fieldExtension);

        fieldExtension = new FieldExtension();
        fieldExtension.setFieldName("retrieveInstanceFleets");
        fieldExtension.setExpression("${retrieveInstanceFleets}");
        fieldExtensionList.add(fieldExtension);

        return fieldExtensionList;
    }
}
