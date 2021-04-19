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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ScriptTask;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.history.HistoricVariableInstance;
import org.activiti.engine.impl.cmd.ExecuteAsyncJobCmd;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.CommandConfig;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.activiti.engine.impl.persistence.entity.JobEntityManager;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.Test;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.service.activiti.task.HerdActivitiServiceTaskTest;

public class HerdCommandInvokerTest extends HerdActivitiServiceTaskTest
{
    /**
     * Tests the error handling behavior for asynchronous tasks. This test proves 2 behaviors: when a asynchronous task is executed, and an error occurs, any
     * variables written by the tasks are persisted, and the error message and stack trace is recorded in the JobEntity which belongs to the execution. Writing
     * the exception in JobEntity ensures that we are able to retrieve the exception message in our GetJob API.
     * <p/>
     * This test must run outside of a transaction since the workflow will be executing asynchronously.
     *
     * @throws Exception
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testAssertAsynchronousTaskErrorWrittenToJobEntity() throws Exception
    {
        // Create a workflow
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId("test");
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            ScriptTask element = new ScriptTask();
            element.setId("script1");
            element.setScriptFormat("js");
            element.setAsynchronous(true);
            element.setScript("execution.setVariable('foo', 'bar');");
            process.addFlowElement(element);
        }
        {
            ScriptTask element = new ScriptTask();
            element.setId("script2");
            element.setScriptFormat("js");
            element.setScript("throw new Error()");
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }

        process.addFlowElement(new SequenceFlow("start", "script1"));
        process.addFlowElement(new SequenceFlow("script1", "script2"));
        process.addFlowElement(new SequenceFlow("script2", "end"));

        bpmnModel.addProcess(process);

        // Deploy the workflow
        activitiRepositoryService.createDeployment().addBpmnModel("bpmn20.xml", bpmnModel).deploy();

        // Start the process instance
        ProcessInstance processInstance = activitiRuntimeService.startProcessInstanceByKey("test");

        try
        {
            /*
             * Wait for process instance to run.
             * Note that we cannot use the convenience method waitUntilAllProcessCompleted() since the workflow will be in an error state and therefore stay
             * active.
             */
            long startTime = System.currentTimeMillis();
            HistoricVariableInstance persistedVariable = null;
            while (persistedVariable == null)
            {
                persistedVariable = activitiHistoryService.createHistoricVariableInstanceQuery().variableName("foo").singleResult();
                Thread.sleep(10);

                if (System.currentTimeMillis() - startTime > 10000)
                {
                    fail("workflow did not reach the desired state within a reasonable time");
                }
            }

            // Make assertions

            // Assert variable is persisted
            assertEquals("bar", persistedVariable.getValue());

            // Assert exception message is logged
            assertEquals("ActivitiException: Problem evaluating script: Error in <eval> at line number 1 at column number 0",
                activitiManagementService.createJobQuery().executionId(processInstance.getProcessInstanceId()).singleResult().getExceptionMessage());
        }
        finally
        {
            deleteActivitiDeployments();
        }
    }

    @Test
    public void testExecuteWithExceptionAndGetJobEntityWithNoSuchFieldException()
    {
        // Mock dependencies.
        CommandConfig config = mock(CommandConfig.class);
        JobEntity jobEntity = mock(JobEntity.class);
        ExecuteAsyncJobCmd command = new ExecuteAsyncJobCmd(jobEntity);
        doThrow(NoSuchFieldException.class).when(jobEntity).getId();

        // Try to call the method under test.
        try
        {
            herdCommandInvoker.execute(config, command);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(NoSuchFieldException.class.getName(), e.getMessage());
        }
    }

    @Test
    public void testExecuteWithExceptionAndGetJobEntityWithSecurityException()
    {
        // Mock dependencies.
        CommandConfig config = mock(CommandConfig.class);
        JobEntity jobEntity = mock(JobEntity.class);
        ExecuteAsyncJobCmd command = new ExecuteAsyncJobCmd(jobEntity);
        doThrow(SecurityException.class).when(jobEntity).getId();

        // Try to call the method under test.
        try
        {
            herdCommandInvoker.execute(config, command);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(SecurityException.class.getName(), e.getMessage());
        }
    }

    @Test
    public void testExecuteWithExceptionAndGetCreateTransactionException()
    {
        // Mock dependencies.
        CommandConfig config = mock(CommandConfig.class);
        JobEntity job = mock(JobEntity.class);
        JobEntityManager jobEntityManager = mock(JobEntityManager.class);
        CommandContext commandContext = mock(CommandContext.class);
        //Save the command context to use later
        CommandContext commandContextSaved = Context.getCommandContext();
        Context.setCommandContext(commandContext);
        String jobId = "testId100";
        when(job.getId()).thenReturn(jobId);
        when(job.getProcessInstanceId()).thenReturn("testProcessId100");
        when(job.getRetries()).thenReturn(3);
        ExecuteAsyncJobCmd command = new ExecuteAsyncJobCmd(job);
        doThrow(CannotCreateTransactionException.class).when(job).execute(any());
        when(commandContext.getJobEntityManager()).thenReturn(jobEntityManager);
        when(jobEntityManager.findJobById(jobId)).thenReturn(job);

        try
        {
            herdCommandInvoker.execute(config, command);
            fail();
        }
        catch (CannotCreateTransactionException e)
        {
           //Get expected exception
        }
        finally
        {
            Context.setCommandContext(commandContextSaved);
        }

    }
}
