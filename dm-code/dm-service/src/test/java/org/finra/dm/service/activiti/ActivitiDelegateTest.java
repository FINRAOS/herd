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
package org.finra.dm.service.activiti;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.impl.jobexecutor.FailedJobListener;
import org.activiti.engine.impl.jobexecutor.TimerExecuteNestedActivityJobHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.JobDefinition;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.service.activiti.task.MockJavaDelegate;

/**
 * Tests the ActivitiDelegateTest class.
 */
public class ActivitiDelegateTest extends AbstractServiceTest
{
    /**
     *  This method tests the timer execution in a workflow.
     */
    @Test
    public void testTimerJob() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_DM_TIMER_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME), false);
        org.activiti.engine.runtime.Job timer =
                activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            activitiManagementService.executeJob(timer.getId());
        }
        assertNotNull(job);
    }

    /**
     *  This method tests the scenario when an error that is not workflow related is throws while workflow is executing an Async 
     *  type task like Timer. This error is logged as ERROR.
     */
    @Test(expected = ActivitiException.class)
    public void testActivitiReportableError() throws Exception
    {
        turnOffBaseJavaDelegateLogging();
        Logger.getLogger(TimerExecuteNestedActivityJobHandler.class).setLevel(Level.OFF);
        Logger.getLogger(FailedJobListener.class).setLevel(Level.OFF);
        Logger.getLogger(TransactionSynchronizationUtils.class).setLevel(Level.OFF);

        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_TIMER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("exceptionToThrow");
        exceptionField.setExpression("${exceptionToThrow}");
        serviceTask.getFieldExtensions().add(exceptionField);

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));

        createJobDefinitionForActivitiXml(activitiXml);

        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("exceptionToThrow", MockJavaDelegate.EXCEPTION_BPMN_ERROR);
        parameters.add(parameter);

        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters), false);
        org.activiti.engine.runtime.Job timer =
                activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            activitiManagementService.executeJob(timer.getId());
        }
    }

    /**
     *  This method tests the scenario when an workflow related error is throws while workflow is executing an Async type task like Timer.
     *  This error is logged as WARN.
     */
    @Test(expected = ActivitiException.class)
    public void testActivitiUnReportableError() throws Exception
    {
        turnOffBaseJavaDelegateLogging();
        Logger.getLogger(TimerExecuteNestedActivityJobHandler.class).setLevel(Level.OFF);
        Logger.getLogger(FailedJobListener.class).setLevel(Level.OFF);
        Logger.getLogger(TransactionSynchronizationUtils.class).setLevel(Level.OFF);

        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_TIMER_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        serviceTask.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_EXPRESSION);
        serviceTask.setImplementation("${BeanNotAvailable}");

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));

        createJobDefinitionForActivitiXml(activitiXml);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, null), false);
        org.activiti.engine.runtime.Job timer =
                activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            activitiManagementService.executeJob(timer.getId());
        }
    }

    /**
     * This method tests the scenario where a java delegate is not populated again with spring beans.
     */
    @Test
    public void testDelegateSpringBeansNotPopulatedAgain() throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_WORKFLOW);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");
        serviceTask.setImplementation(MockJavaDelegate.class.getCanonicalName());
        serviceTask.getFieldExtensions().clear();

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));

        // Define the job definition
        createJobDefinitionForActivitiXml(activitiXml);

        // Executing the job twice so that the same JavaDelegate object is used and spring beans are not wired again.
        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME), false);
        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME), false);
    }

    /**
     * This method tests the scenario where a bpmn error is thrown by a java delegate.
     */
    @Test
    public void testDelegateBpmnError() throws Exception
    {
        turnOffCreateAndStartProcessInstanceCmdLogging();

        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_WORKFLOW);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        serviceTask.setImplementation(MockJavaDelegate.class.getCanonicalName());

        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName("exceptionToThrow");
        exceptionField.setExpression("${exceptionToThrow}");
        serviceTask.getFieldExtensions().clear();
        serviceTask.getFieldExtensions().add(exceptionField);

        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("exceptionToThrow", MockJavaDelegate.EXCEPTION_BPMN_ERROR);
        parameters.add(parameter);

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));
        createJobFromActivitiXml(activitiXml, parameters);
    }

    /**
     * This method tests the scenario where a RuntimeException occurs but not thrown by java delegate.
     */
    @Test
    public void testDelegateRuntimeError() throws Exception
    {
        turnOffBaseJavaDelegateLogging();

        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_WORKFLOW);

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

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));
        createJobFromActivitiXml(activitiXml, parameters);
    }

    /**
     * This method tests the scenario where a process definition is not defined.
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateProcessCommandProcessNotDefined() throws Exception
    {
        // Create the job with Activiti.
        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, "test_process_not_defined"), false);
    }

    /**
     * This method tests the scenario where a process definition is suspended in Activiti.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateProcessCommandProcessSuspended() throws Exception
    {
        // Define the job definition
        JobDefinition jobDefinition = createJobDefinition(ACTIVITI_XML_DM_WORKFLOW);

        JobDefinitionEntity jobDefinitionEntity = dmDao.getJobDefinitionByAltKey(jobDefinition.getNamespace(), jobDefinition.getJobName());

        // Suspend the job definition with activiti api
        activitiRepositoryService.suspendProcessDefinitionById(jobDefinitionEntity.getActivitiId());

        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME), false);
    }

    /**
     * This method tests when wrong class name is used in service task, the process instance is created.
     */
    @Test
    public void testDelegateWrongClass() throws Exception
    {
        turnOffCreateAndStartProcessInstanceCmdLogging();

        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_DM_WORKFLOW);
        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement("servicetask1");

        serviceTask.setImplementation("ClassDoesNotExist");
        serviceTask.getFieldExtensions().clear();

        String activitiXml = new String(new BpmnXMLConverter().convertToXML(bpmnModel));
        // Run a job with Activiti XML that will start cluster.
        Job job = createJobFromActivitiXml(activitiXml, null);
        assertNotNull(job);
    }
}