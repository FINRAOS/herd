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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.activiti.engine.RuntimeService;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ExecutionQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the ActivitiRuntimeHelper class.
 */
public class ActivitiRuntimeHelperTest extends AbstractServiceTest
{
    @InjectMocks
    private ActivitiRuntimeHelper activitiRuntimeHelper;

    @Mock
    private RuntimeService runtimeService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testBuildTaskWorkflowVariableName()
    {
        assertEquals(ACTIVITI_ID + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + VARIABLE_NAME,
            activitiRuntimeHelper.buildTaskWorkflowVariableName(ACTIVITI_ID, VARIABLE_NAME));
    }

    @Test
    public void testSetTaskErrorInWorkflow()
    {
        // Create variables required for testing.
        final String currentActivityId = ACTIVITI_ID;
        final String executionId = ACTIVITI_ID_2;
        final String statusTaskWorkflowVariableName =
            activitiRuntimeHelper.buildTaskWorkflowVariableName(currentActivityId, ActivitiRuntimeHelper.VARIABLE_STATUS);
        final String errorMessageTaskWorkflowVariableName =
            activitiRuntimeHelper.buildTaskWorkflowVariableName(currentActivityId, ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE);

        // Create an exception.
        Exception exception = new Exception(ERROR_CODE);

        // Mock dependencies.
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getCurrentActivityId()).thenReturn(currentActivityId);
        when(execution.getId()).thenReturn(executionId);

        // Call the method under test.
        activitiRuntimeHelper.setTaskErrorInWorkflow(execution, ERROR_MESSAGE, exception);

        // Verify the calls.
        verify(execution, times(3)).getCurrentActivityId();
        verify(execution, times(3)).getId();
        verify(runtimeService, times(2)).getVariables(executionId);
        verify(runtimeService).setVariable(executionId, statusTaskWorkflowVariableName, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        verify(runtimeService).setVariable(executionId, errorMessageTaskWorkflowVariableName, ERROR_MESSAGE);
        verifyNoMoreInteractions(execution, runtimeService);
    }

    @Test
    public void testSetTaskErrorInWorkflowNoException()
    {
        // Create variables required for testing.
        final String currentActivityId = ACTIVITI_ID;
        final String executionId = ACTIVITI_ID_2;
        final String statusTaskWorkflowVariableName =
            activitiRuntimeHelper.buildTaskWorkflowVariableName(currentActivityId, ActivitiRuntimeHelper.VARIABLE_STATUS);
        final String errorMessageTaskWorkflowVariableName =
            activitiRuntimeHelper.buildTaskWorkflowVariableName(currentActivityId, ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE);

        // Mock dependencies.
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getCurrentActivityId()).thenReturn(currentActivityId);
        when(execution.getId()).thenReturn(executionId);

        // Call the method under test.
        activitiRuntimeHelper.setTaskErrorInWorkflow(execution, ERROR_MESSAGE, null);

        // Verify the calls.
        verify(execution, times(2)).getCurrentActivityId();
        verify(execution, times(2)).getId();
        verify(runtimeService, times(2)).getVariables(executionId);
        verify(runtimeService).setVariable(executionId, statusTaskWorkflowVariableName, ActivitiRuntimeHelper.TASK_STATUS_ERROR);
        verify(runtimeService).setVariable(executionId, errorMessageTaskWorkflowVariableName, ERROR_MESSAGE);
        verifyNoMoreInteractions(execution, runtimeService);
    }

    @Test
    public void testSetTaskSuccessInWorkflow()
    {
        // Create variables required for testing.
        final String currentActivityId = ACTIVITI_ID;
        final String executionId = ACTIVITI_ID_2;
        final String statusTaskWorkflowVariableName =
            activitiRuntimeHelper.buildTaskWorkflowVariableName(currentActivityId, ActivitiRuntimeHelper.VARIABLE_STATUS);

        // Mock dependencies.
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getCurrentActivityId()).thenReturn(currentActivityId);
        when(execution.getId()).thenReturn(executionId);

        // Call the method under test.
        activitiRuntimeHelper.setTaskSuccessInWorkflow(execution);

        // Verify the calls.
        verify(execution).getCurrentActivityId();
        verify(execution).getId();
        verify(runtimeService).getVariables(executionId);
        verify(runtimeService).setVariable(executionId, statusTaskWorkflowVariableName, ActivitiRuntimeHelper.TASK_STATUS_SUCCESS);
        verifyNoMoreInteractions(execution, runtimeService);
    }

    @Test
    public void testSignal()
    {
        // Create variables required for testing.
        final String processInstanceId = ACTIVITI_ID;
        final String signalTaskId = ACTIVITI_ID_2;
        final String executionId = ACTIVITI_ID_3;

        // Mock dependencies.
        ExecutionQuery executionQuery = mock(ExecutionQuery.class);
        Execution execution = mock(Execution.class);
        when(runtimeService.createExecutionQuery()).thenReturn(executionQuery);
        when(executionQuery.processInstanceId(processInstanceId)).thenReturn(executionQuery);
        when(executionQuery.activityId(signalTaskId)).thenReturn(executionQuery);
        when(executionQuery.singleResult()).thenReturn(execution);
        when(execution.getId()).thenReturn(executionId);

        // Call the method under test.
        activitiRuntimeHelper.signal(processInstanceId, signalTaskId);

        // Verify the calls.
        verify(runtimeService).createExecutionQuery();
        verify(executionQuery).processInstanceId(processInstanceId);
        verify(executionQuery).activityId(signalTaskId);
        verify(executionQuery).singleResult();
        verify(execution).getId();
        verify(runtimeService).signal(executionId);
        verifyNoMoreInteractions(executionQuery, execution, runtimeService);
    }
}
