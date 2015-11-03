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

import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.dm.dao.impl.MockEmrOperationsImpl;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the CheckEmrCluster Activiti task wrapper.
 */
public class CheckEmrClusterTest extends AbstractServiceTest
{
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
        Job job = createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(true, "false"));
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
        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertEquals(shellStepId, activeStepId);
        String activeStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_jarLocation");
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

        Job job = createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(true, "true"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNotNull(emrStepJarLocation);

        String shellStepId = (String) variables.get("addShellStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        String activeStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_id");
        assertEquals(shellStepId, activeStepId);
        String activeStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "activeStep_jarLocation");
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
        Job job = createJobForCreateClusterForActivitiXml(getCheckClusterActivitiXml(fieldExtensions), getParameters(false, "false"));
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String hiveStepId = (String) variables.get("addHiveStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "emrStepId");
        String emrStepId = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_id");
        assertEquals(hiveStepId, emrStepId);
        String emrStepJarLocation = (String) variables.get("checkClusterServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + "step_jarLocation");
        assertNull(emrStepJarLocation);
    }

    @Test
    public void testCheckCluster() throws Exception
    {
        // Run a job with Activiti XML that will start cluster, check status and terminate.
        Job job = createJobForCreateCluster(ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH, getParameters(true, ""));
        assertNotNull(job);
    }

    private List<Parameter> getParameters(boolean isShellStepRunning, String verbose)
    {
        List<Parameter> parameters = new ArrayList<>();

        parameters.add(new Parameter("clusterName", "testCluster1"));
        parameters.add(new Parameter("hiveStepName", "a_hive_step"));
        parameters.add(new Parameter("hiveScriptLocation", "a_hive_step_location"));
        parameters.add(new Parameter("shellStepName", isShellStepRunning ? MockEmrOperationsImpl.MOCK_STEP_RUNNING_NAME : "a_shell_step"));
        parameters.add(new Parameter("shellScriptLocation", "a_shell_step_location"));
        parameters.add(new Parameter("verbose", verbose));

        return parameters;
    }

    private String getCheckClusterActivitiXml(List<FieldExtension> checkClusterFieldExtensions) throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("checkClusterServiceTask");

        serviceTask.getFieldExtensions().addAll(checkClusterFieldExtensions);

        return new String(new BpmnXMLConverter().convertToXML(bpmnModel));
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

        return fieldExtensionList;
    }
}