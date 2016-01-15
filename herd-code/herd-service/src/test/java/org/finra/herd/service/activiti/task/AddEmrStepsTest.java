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

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

public class AddEmrStepsTest extends AbstractServiceTest
{

    @Test
    public void testAddShellStep() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>(getCommonParameters("Shell Step"));

        Parameter parameter = new Parameter("scriptLocation", "s3://test-bucket-managed/app-a/test/test_script.sh");
        parameters.add(parameter);

        parameter = new Parameter("scriptArguments", "arg1|arg/|withpipe|arg2|arg3");
        parameters.add(parameter);

        testActivitiAddEmrStepSuccess(AddEmrShellStep.class.getCanonicalName(), getScriptStepsFieldExtension(), parameters);
    }

    @Test
    public void testAddHiveStep() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>(getCommonParameters("Hive Step"));

        Parameter parameter = new Parameter("scriptLocation", "s3://test-bucket-managed/app-a/test/test_hive.hql");
        parameters.add(parameter);

        parameter = new Parameter("scriptArguments", "arg1|arg/|withpipe|arg2|arg3");
        parameters.add(parameter);

        testActivitiAddEmrStepSuccess(AddEmrHiveStep.class.getCanonicalName(), getScriptStepsFieldExtension(), parameters);
    }

    @Test
    public void testAddPigStep() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>(getCommonParameters("Pig Step"));

        Parameter parameter = new Parameter("scriptLocation", "s3://test-bucket-managed/app-a/test/test_pig.pig");
        parameters.add(parameter);

        parameter = new Parameter("scriptArguments", "arg1|arg/|withpipe|arg2|arg3");
        parameters.add(parameter);

        testActivitiAddEmrStepSuccess(AddEmrPigStep.class.getCanonicalName(), getScriptStepsFieldExtension(), parameters);
    }

    @Test
    public void testAddOozieStep() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("workflowXmlLocation");
        exceptionField.setExpression("${workflowXmlLocation}");
        fieldExtensionList.add(exceptionField);

        exceptionField = new FieldExtension();
        exceptionField.setFieldName("ooziePropertiesFileLocation");
        exceptionField.setExpression("${ooziePropertiesFileLocation}");
        fieldExtensionList.add(exceptionField);

        List<Parameter> parameters = new ArrayList<>(getCommonParameters("Oozie Step"));

        Parameter parameter = new Parameter("workflowXmlLocation", "s3://test-bucket-managed/app-a/test/workflow.xml");
        parameters.add(parameter);

        parameter = new Parameter("ooziePropertiesFileLocation", "s3://test-bucket-managed/app-a/test/job.properties");
        parameters.add(parameter);

        testActivitiAddEmrStepSuccess(AddEmrOozieStep.class.getCanonicalName(), fieldExtensionList, parameters);
    }

    @Test
    public void testAddHadoopJarStep() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("jarLocation");
        exceptionField.setExpression("${jarLocation}");
        fieldExtensionList.add(exceptionField);

        exceptionField = new FieldExtension();
        exceptionField.setFieldName("mainClass");
        exceptionField.setExpression("${mainClass}");
        fieldExtensionList.add(exceptionField);

        exceptionField = new FieldExtension();
        exceptionField.setFieldName("scriptArguments");
        exceptionField.setExpression("${scriptArguments}");
        fieldExtensionList.add(exceptionField);

        List<Parameter> parameters = new ArrayList<>(getCommonParameters("Hadoop jar Step"));

        Parameter parameter = new Parameter("jarLocation", "s3://test-bucket-managed/app-a/test/hadoop-mapreduce-examples-2.4.0.jar");
        parameters.add(parameter);

        parameter = new Parameter("mainClass", "wordcount");
        parameters.add(parameter);

        parameter = new Parameter("scriptArguments", "arg1|arg/|withpipe|arg2|arg3");
        parameters.add(parameter);

        testActivitiAddEmrStepSuccess(AddEmrHadoopJarStep.class.getCanonicalName(), fieldExtensionList, parameters);
    }

    @Test
    public void testAddShellStepNoStepName() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("continueOnError", "");
        parameters.add(parameter);

        parameter = new Parameter("stepName", "");
        parameters.add(parameter);

        testActivitiAddEmrStepFailure(AddEmrShellStep.class.getCanonicalName(), new ArrayList<FieldExtension>(), parameters);
    }

    @Test
    public void testAddShellStepNoScriptLocation() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("stepName", "Shell Step");
        parameters.add(parameter);

        parameter = new Parameter("continueOnError", "");
        parameters.add(parameter);

        testActivitiAddEmrStepFailure(AddEmrShellStep.class.getCanonicalName(), new ArrayList<FieldExtension>(), parameters);
    }

    @Test
    public void testAddShellStepWrongContinueOnError() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("stepName", "Shell Step");
        parameters.add(parameter);

        parameter = new Parameter("continueOnError", "sfsdfsd");
        parameters.add(parameter);

        parameter = new Parameter("scriptLocation", "A_SCRIPT_LOCATION");
        parameters.add(parameter);

        testActivitiAddEmrStepFailure(AddEmrShellStep.class.getCanonicalName(), getScriptStepsFieldExtension(), parameters);
    }

    @Test
    public void testAddOozieStepNoWorkflowXml() throws Exception
    {
        testActivitiAddEmrStepFailure(AddEmrOozieStep.class.getCanonicalName(), new ArrayList<FieldExtension>(), getCommonParameters("Oozie Step"));
    }

    @Test
    public void testAddOozieStepNoOoziePropertiesLocation() throws Exception
    {

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("workflowXmlLocation");
        exceptionField.setExpression("${workflowXmlLocation}");
        fieldExtensionList.add(exceptionField);

        List<Parameter> parameters = getCommonParameters("Oozie Step");

        Parameter parameter = new Parameter("workflowXmlLocation", "workflow_xml_location");
        parameters.add(parameter);

        testActivitiAddEmrStepFailure(AddEmrOozieStep.class.getCanonicalName(), fieldExtensionList, parameters);
    }

    @Test
    public void testAddHadoopJarStepNoJar() throws Exception
    {
        testActivitiAddEmrStepFailure(AddEmrHadoopJarStep.class.getCanonicalName(), new ArrayList<FieldExtension>(), getCommonParameters("Hadoop jar Step"));
    }

    private void testActivitiAddEmrStepSuccess(String implementation, List<FieldExtension> fieldExtensionList, List<Parameter> parameters) throws Exception
    {
        String activitiXml = buildActivitiXml(implementation, fieldExtensionList);
        createJobAndCheckStepStatusSuccess(activitiXml, parameters);
    }

    private String buildActivitiXml(String implementation, List<FieldExtension> fieldExtensionList) throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_ADD_EMR_STEPS_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("addStepServiceTask");

        serviceTask.setImplementation(implementation);
        serviceTask.getFieldExtensions().addAll(fieldExtensionList);

        return getActivitiXmlFromBpmnModel(bpmnModel);
    }

    private void createJobAndCheckStepStatusSuccess(String activitiXml, List<Parameter> parameters) throws Exception
    {
        Job job = createJobForCreateClusterForActivitiXml(activitiXml, parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String addStepServiceTaskStatus =
            (String) variables.get("addStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, addStepServiceTaskStatus);

        String addStepId = (String) variables.get("addStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + BaseAddEmrStep.VARIABLE_EMR_STEP_ID);
        assertNotNull(addStepId);
    }

    private void testActivitiAddEmrStepFailure(String implementation, List<FieldExtension> fieldExtensionList, List<Parameter> parameters) throws Exception
    {
        String activitiXml = buildActivitiXml(implementation, fieldExtensionList);
        createJobAndCheckStepStatusFailure(activitiXml, parameters);
    }

    private void createJobAndCheckStepStatusFailure(String activitiXml, List<Parameter> parameters) throws Exception
    {
        Job job = createJobForCreateClusterForActivitiXml(activitiXml, parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String addStepServiceTaskStatus =
            (String) variables.get("addStepServiceTask" + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + ActivitiRuntimeHelper.VARIABLE_STATUS);
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, addStepServiceTaskStatus);
    }

    private List<Parameter> getCommonParameters(String stepName)
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        parameter = new Parameter("stepName", stepName);
        parameters.add(parameter);

        parameter = new Parameter("continueOnError", "true");
        parameters.add(parameter);

        return parameters;
    }

    private List<FieldExtension> getScriptStepsFieldExtension()
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("scriptLocation");
        exceptionField.setExpression("${scriptLocation}");
        fieldExtensionList.add(exceptionField);

        exceptionField = new FieldExtension();
        exceptionField.setFieldName("scriptArguments");
        exceptionField.setExpression("${scriptArguments}");
        fieldExtensionList.add(exceptionField);

        return fieldExtensionList;
    }
}