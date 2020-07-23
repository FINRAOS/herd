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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.impl.jobexecutor.TimerExecuteNestedActivityJobHandler;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.service.activiti.task.BaseJavaDelegate;
import org.finra.herd.service.activiti.task.HerdActivitiServiceTaskTest;
import org.finra.herd.service.activiti.task.MockJavaDelegate;

/**
 * Tests the ActivitiDelegateTest class.
 */
public class ActivitiDelegateTest extends HerdActivitiServiceTaskTest
{
    /**
     * This method tests the timer execution in a workflow.
     */
    @Test
    public void testTimerJob() throws Exception
    {
        // Create and start the workflow.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_HERD_TIMER_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));
        assertNotNull(job);

        // This workflow would normally automatically start a timer which would eventually complete the workflow, however, since
        // this test method is running within our own transaction, the timer would never go off since it is run in a different thread which is outside of
        // our transaction which didn't commit yet. As a result, we need to manually run the timer job to simulate what would happen if the timer
        // went off by itself.
        org.activiti.engine.runtime.Job timer = activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            activitiManagementService.executeJob(timer.getId());
        }
    }

    /**
     * This is an alternative way of running the timer test above which doesn't go through the herd job infrastructure, but rather ensures that Activiti can run
     * the timer job successfully. This is accomplished by invoking Activiti API's directly and not running within our own transaction.
     *
     * @throws Exception
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testTimerJobLowLevelActiviti() throws Exception
    {
        try
        {// Read workflow XML from classpath and deploy it.
            activitiRepositoryService.createDeployment().addClasspathResource(ACTIVITI_XML_HERD_TIMER).deploy();

            // Set workflow variables for no other reason than to make sure it doesn't cause any problems.
            Map<String, Object> variables = new HashMap<>();
            variables.put("key1", "value1");

            // Execute the workflow.
            ProcessInstance processInstance = activitiRuntimeService.startProcessInstanceByKey("testNamespace.testHerdWorkflow", variables);

            // Wait a reasonable amount of time for the process to finish.
            waitUntilAllProcessCompleted();

            // Get the history for the process and ensure the workflow completed.
            HistoricProcessInstance historicProcessInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(processInstance.getProcessInstanceId()).singleResult();

            Assert.assertNotNull(historicProcessInstance);
            Assert.assertNotNull(historicProcessInstance.getEndTime());
        }
        finally
        {
            deleteActivitiDeployments();
        }
    }

    /**
     * This method tests the scenario when an error that is not workflow related is throws while workflow is executing an Async type task like Timer. This error
     * is logged as ERROR.
     */
    @Test(expected = ActivitiException.class)
    public void testActivitiReportableError() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_HERD_TIMER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("exceptionToThrow");
        exceptionField.setExpression("${exceptionToThrow}");
        serviceTask.getFieldExtensions().add(exceptionField);

        jobDefinitionServiceTestHelper.createJobDefinitionForActivitiXml(getActivitiXmlFromBpmnModel(bpmnModel));

        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("exceptionToThrow", MockJavaDelegate.EXCEPTION_BPMN_ERROR);
        parameters.add(parameter);

        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters));
        org.activiti.engine.runtime.Job timer = activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            executeWithoutLogging(Arrays.asList(ActivitiRuntimeHelper.class, TimerExecuteNestedActivityJobHandler.class), () -> {
                activitiManagementService.executeJob(timer.getId());
            });
        }
    }

    /**
     * This method tests the scenario when an workflow related error is throws while workflow is executing an Async type task like Timer. This error is logged
     * as WARN.
     */
    @Test(expected = ActivitiException.class)
    public void testActivitiUnReportableError() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_HERD_TIMER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        serviceTask.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_EXPRESSION);
        serviceTask.setImplementation("${BeanNotAvailable}");

        jobDefinitionServiceTestHelper.createJobDefinitionForActivitiXml(getActivitiXmlFromBpmnModel(bpmnModel));
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, null));
        org.activiti.engine.runtime.Job timer = activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            executeWithoutLogging(TimerExecuteNestedActivityJobHandler.class, () -> {
                activitiManagementService.executeJob(timer.getId());
            });
        }
    }

    /**
     * This method tests the scenario where a java delegate is not populated again with spring beans.
     */
    @Test
    public void testDelegateSpringBeansNotPopulatedAgain() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_HERD_WORKFLOW);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");
        serviceTask.setImplementation(MockJavaDelegate.class.getCanonicalName());
        serviceTask.getFieldExtensions().clear();

        // Define the job definition
        jobDefinitionServiceTestHelper.createJobDefinitionForActivitiXml(getActivitiXmlFromBpmnModel(bpmnModel));

        // Executing the job twice so that the same JavaDelegate object is used and spring beans are not wired again.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));
    }

    /**
     * This method tests the scenario where a RuntimeException occurs but not thrown by java delegate.
     */
    @Test
    public void testDelegateRuntimeError() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_HERD_WORKFLOW);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        serviceTask.setImplementation(MockJavaDelegate.class.getCanonicalName());

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("exceptionToThrow");
        exceptionField.setExpression("${exceptionToThrow}");
        serviceTask.getFieldExtensions().clear();
        serviceTask.getFieldExtensions().add(exceptionField);

        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("exceptionToThrow", MockJavaDelegate.EXCEPTION_RUNTIME);
        parameters.add(parameter);

        executeWithoutLogging(Arrays.asList(ActivitiRuntimeHelper.class, BaseJavaDelegate.class), () -> {
            jobServiceTestHelper.createJobFromActivitiXml(getActivitiXmlFromBpmnModel(bpmnModel), parameters);
        });
    }

    /**
     * This method tests the scenario where a process definition is not defined.
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateProcessCommandProcessNotDefined() throws Exception
    {
        // Create the job with Activiti.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, "test_process_not_defined"));
    }

    /**
     * This method tests the scenario where a process definition is suspended in Activiti.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateProcessCommandProcessSuspended() throws Exception
    {
        // Define the job definition
        JobDefinition jobDefinition = jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_HERD_WORKFLOW);

        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(jobDefinition.getNamespace(), jobDefinition.getJobName());

        // Suspend the job definition with activiti api
        activitiRepositoryService.suspendProcessDefinitionById(jobDefinitionEntity.getActivitiId());

        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));
    }

    /**
     * This method tests when wrong class name is used in service task, the process instance is created.
     */
    @Test
    public void testDelegateWrongClass() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_HERD_WORKFLOW);
        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");
        String wrongClass = "org.finra.herd.service.activiti.task.ClassDoesNotExist";

        serviceTask.setImplementation(wrongClass);
        serviceTask.getFieldExtensions().clear();

        // Run a job with Activiti XML that will start cluster.
        try
        {
            jobServiceTestHelper.createJobFromActivitiXml(getActivitiXmlFromBpmnModel(bpmnModel), null);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ActivitiException.class, e.getClass());
            assertEquals(String.format("couldn't instantiate class %s",wrongClass), e.getMessage());
        }
    }
}
