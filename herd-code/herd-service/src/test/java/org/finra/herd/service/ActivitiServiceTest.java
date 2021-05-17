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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricActivityInstanceQuery;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricProcessInstanceQuery;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.repository.ProcessDefinitionQuery;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ExecutionQuery;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.JobQuery;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.runtime.ProcessInstanceQuery;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.impl.ActivitiServiceImpl;

public class ActivitiServiceTest
{
    @InjectMocks
    private ActivitiServiceImpl activitiService;

    @Mock
    private HistoryService activitiHistoryService;

    @Mock
    private ManagementService activitiManagementService;

    @Mock
    private RepositoryService activitiRepositoryService;

    @Mock
    private RuntimeService activitiRuntimeService;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void testGetProcessDefinitionById()
    {
        String processDefinitionId = "processDefinitionId";
        ProcessDefinitionQuery processDefinitionQuery = mock(ProcessDefinitionQuery.class);
        when(activitiRepositoryService.createProcessDefinitionQuery()).thenReturn(processDefinitionQuery);
        when(processDefinitionQuery.processDefinitionId(processDefinitionId)).thenReturn(processDefinitionQuery);
        ProcessDefinition expectedProcessDefinition = mock(ProcessDefinition.class);
        when(processDefinitionQuery.singleResult()).thenReturn(expectedProcessDefinition);
        ProcessDefinition actualProcessDefinition = activitiService.getProcessDefinitionById(processDefinitionId);
        assertSame(expectedProcessDefinition, actualProcessDefinition);
        InOrder inOrder = inOrder(processDefinitionQuery);
        inOrder.verify(processDefinitionQuery).processDefinitionId(processDefinitionId);
        inOrder.verify(processDefinitionQuery).singleResult();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testStartProcessInstanceByProcessDefinitionId()
    {
        String processDefinitionId = "processDefinitionId";
        Map<String, Object> variables = new HashMap<>();
        ProcessInstance expectedProcessInstance = mock(ProcessInstance.class);
        when(configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)).thenReturn("DEV");
        when(activitiRuntimeService.startProcessInstanceById(processDefinitionId, variables)).thenReturn(expectedProcessInstance);
        ProcessInstance actualProcessInstance = activitiService.startProcessInstanceByProcessDefinitionId(processDefinitionId, variables);
        assertSame(expectedProcessInstance, actualProcessInstance);
        verify(activitiRuntimeService).startProcessInstanceById(processDefinitionId, variables);
        verifyNoMoreInteractions(activitiRuntimeService);
    }

    @Test
    public void testGetProcessInstanceById()
    {
        String processInstanceId = "processInstanceId";
        ProcessInstanceQuery processInstanceQuery = mock(ProcessInstanceQuery.class);
        when(activitiRuntimeService.createProcessInstanceQuery()).thenReturn(processInstanceQuery);
        when(processInstanceQuery.processInstanceId(processInstanceId)).thenReturn(processInstanceQuery);
        when(processInstanceQuery.includeProcessVariables()).thenReturn(processInstanceQuery);
        ProcessInstance expectedProcessInstance = mock(ProcessInstance.class);
        when(processInstanceQuery.singleResult()).thenReturn(expectedProcessInstance);
        ProcessInstance actualProcessInstance = activitiService.getProcessInstanceById(processInstanceId);
        assertSame(expectedProcessInstance, actualProcessInstance);
        InOrder inOrder = inOrder(processInstanceQuery);
        inOrder.verify(processInstanceQuery).processInstanceId(processInstanceId);
        inOrder.verify(processInstanceQuery).includeProcessVariables();
        inOrder.verify(processInstanceQuery).singleResult();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstanceByProcessInstanceId()
    {
        String processInstanceId = "processInstanceId";
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processInstanceId(processInstanceId)).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.includeProcessVariables()).thenReturn(historicProcessInstanceQuery);
        HistoricProcessInstance expectedHistoricProcessInstance = mock(HistoricProcessInstance.class);
        when(historicProcessInstanceQuery.singleResult()).thenReturn(expectedHistoricProcessInstance);
        HistoricProcessInstance actualHistoricProcessInstance = activitiService.getHistoricProcessInstanceByProcessInstanceId(processInstanceId);
        assertSame(expectedHistoricProcessInstance, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processInstanceId(processInstanceId);
        inOrder.verify(historicProcessInstanceQuery).includeProcessVariables();
        inOrder.verify(historicProcessInstanceQuery).singleResult();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricActivityInstancesByProcessInstanceId()
    {
        String processInstanceId = "processInstanceId";
        HistoricActivityInstanceQuery historicActivityInstanceQuery = mock(HistoricActivityInstanceQuery.class);
        when(activitiHistoryService.createHistoricActivityInstanceQuery()).thenReturn(historicActivityInstanceQuery);
        when(historicActivityInstanceQuery.processInstanceId(processInstanceId)).thenReturn(historicActivityInstanceQuery);
        when(historicActivityInstanceQuery.orderByHistoricActivityInstanceStartTime()).thenReturn(historicActivityInstanceQuery);
        when(historicActivityInstanceQuery.asc()).thenReturn(historicActivityInstanceQuery);
        when(historicActivityInstanceQuery.orderByHistoricActivityInstanceEndTime()).thenReturn(historicActivityInstanceQuery);
        when(historicActivityInstanceQuery.asc()).thenReturn(historicActivityInstanceQuery);
        List<HistoricActivityInstance> expectedHistoricActivityInstances = new ArrayList<>();
        when(historicActivityInstanceQuery.list()).thenReturn(expectedHistoricActivityInstances);
        List<HistoricActivityInstance> actualHistoricActivityInstances = activitiService.getHistoricActivityInstancesByProcessInstanceId(processInstanceId);
        assertSame(expectedHistoricActivityInstances, actualHistoricActivityInstances);
        InOrder inOrder = inOrder(historicActivityInstanceQuery);
        inOrder.verify(historicActivityInstanceQuery).processInstanceId(processInstanceId);
        inOrder.verify(historicActivityInstanceQuery).orderByHistoricActivityInstanceStartTime();
        inOrder.verify(historicActivityInstanceQuery).asc();
        inOrder.verify(historicActivityInstanceQuery).orderByHistoricActivityInstanceEndTime();
        inOrder.verify(historicActivityInstanceQuery).asc();
        inOrder.verify(historicActivityInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetJobsWithExceptionByProcessInstanceId()
    {
        String processInstanceId = "processInstanceId";
        JobQuery jobQuery = mock(JobQuery.class);
        when(activitiManagementService.createJobQuery()).thenReturn(jobQuery);
        when(jobQuery.withException()).thenReturn(jobQuery);
        when(jobQuery.processInstanceId(processInstanceId)).thenReturn(jobQuery);
        List<Job> expectedJobs = new ArrayList<>();
        when(jobQuery.list()).thenReturn(expectedJobs);
        List<Job> actualJobs = activitiService.getJobsWithExceptionByProcessInstanceId(processInstanceId);
        assertSame(expectedJobs, actualJobs);
        InOrder inOrder = inOrder(jobQuery);
        inOrder.verify(jobQuery).withException();
        inOrder.verify(jobQuery).processInstanceId(processInstanceId);
        inOrder.verify(jobQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetJobExceptionStacktrace()
    {
        String jobId = "jobId";
        String expectedResult = "expectedResult";
        when(activitiManagementService.getJobExceptionStacktrace(jobId)).thenReturn(expectedResult);
        String actualResult = activitiService.getJobExceptionStacktrace(jobId);
        assertSame(expectedResult, actualResult);
        verify(activitiManagementService).getJobExceptionStacktrace(jobId);
        verifyNoMoreInteractions(activitiManagementService);
    }

    @Test
    public void testGetProcessDefinitionsByIds()
    {
        Set<String> processDefinitionIds = new HashSet<>();
        ProcessDefinitionQuery processDefinitionQuery = mock(ProcessDefinitionQuery.class);
        when(activitiRepositoryService.createProcessDefinitionQuery()).thenReturn(processDefinitionQuery);
        when(processDefinitionQuery.processDefinitionIds(processDefinitionIds)).thenReturn(processDefinitionQuery);
        List<ProcessDefinition> expectedProcessDefinitions = new ArrayList<>();
        when(processDefinitionQuery.list()).thenReturn(expectedProcessDefinitions);
        List<ProcessDefinition> actualProcessDefinitions = activitiService.getProcessDefinitionsByIds(processDefinitionIds);
        assertSame(expectedProcessDefinitions, actualProcessDefinitions);
        InOrder inOrder = inOrder(processDefinitionQuery);
        inOrder.verify(processDefinitionQuery).processDefinitionIds(processDefinitionIds);
        inOrder.verify(processDefinitionQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetJobsWithExceptionCountByProcessInstanceId()
    {
        String processInstanceId = "processInstanceId";
        JobQuery jobQuery = mock(JobQuery.class);
        when(activitiManagementService.createJobQuery()).thenReturn(jobQuery);
        when(jobQuery.withException()).thenReturn(jobQuery);
        when(jobQuery.processInstanceId(processInstanceId)).thenReturn(jobQuery);
        long expectedResult = 1234l;
        when(jobQuery.count()).thenReturn(expectedResult);
        long actualResult = activitiService.getJobsWithExceptionCountByProcessInstanceId(processInstanceId);
        assertEquals(expectedResult, actualResult);
        InOrder inOrder = inOrder(jobQuery);
        inOrder.verify(jobQuery).withException();
        inOrder.verify(jobQuery).processInstanceId(processInstanceId);
        inOrder.verify(jobQuery).count();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstancesByStatusAndProcessDefinitionKeys()
    {
        JobStatusEnum jobStatus = JobStatusEnum.RUNNING;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = new DateTime();
        DateTime endTime = new DateTime();
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.startedAfter(startTime.toDate())).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.finishedBefore(endTime.toDate())).thenReturn(historicProcessInstanceQuery);
        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);
        List<HistoricProcessInstance> actualHistoricProcessInstance =
            activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertSame(expectedHistoricProcessInstances, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).unfinished();
        inOrder.verify(historicProcessInstanceQuery).startedAfter(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).finishedBefore(endTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstancesByStatusAndProcessDefinitionKeysWhenStatusCompleted()
    {
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = new DateTime();
        DateTime endTime = new DateTime();
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.startedAfter(startTime.toDate())).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.finishedBefore(endTime.toDate())).thenReturn(historicProcessInstanceQuery);
        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);
        List<HistoricProcessInstance> actualHistoricProcessInstance =
            activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertSame(expectedHistoricProcessInstances, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).finished();
        inOrder.verify(historicProcessInstanceQuery).startedAfter(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).finishedBefore(endTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstancesByStatusAndProcessDefinitionKeysWhenStatusNotSpecified()
    {
        JobStatusEnum jobStatus = null;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = new DateTime();
        DateTime endTime = new DateTime();
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.startedAfter(startTime.toDate())).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.finishedBefore(endTime.toDate())).thenReturn(historicProcessInstanceQuery);
        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);
        List<HistoricProcessInstance> actualHistoricProcessInstance =
            activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertSame(expectedHistoricProcessInstances, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).startedAfter(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).finishedBefore(endTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstancesByStatusAndProcessDefinitionKeysWhenStartTimeNotSpecified()
    {
        JobStatusEnum jobStatus = JobStatusEnum.RUNNING;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = null;
        DateTime endTime = new DateTime();
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.finishedBefore(endTime.toDate())).thenReturn(historicProcessInstanceQuery);
        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);
        List<HistoricProcessInstance> actualHistoricProcessInstance =
            activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertSame(expectedHistoricProcessInstances, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).unfinished();
        inOrder.verify(historicProcessInstanceQuery).finishedBefore(endTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGetHistoricProcessInstancesByStatusAndProcessDefinitionKeysWhenEndTimeNotSpecified()
    {
        JobStatusEnum jobStatus = JobStatusEnum.RUNNING;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = new DateTime();
        DateTime endTime = null;
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.startedAfter(startTime.toDate())).thenReturn(historicProcessInstanceQuery);
        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);
        List<HistoricProcessInstance> actualHistoricProcessInstance =
            activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertSame(expectedHistoricProcessInstances, actualHistoricProcessInstance);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).unfinished();
        inOrder.verify(historicProcessInstanceQuery).startedAfter(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys()
    {
        JobStatusEnum jobStatus = JobStatusEnum.RUNNING;
        Collection<String> processDefinitionKeys = new ArrayList<>();
        DateTime startTime = new DateTime();
        DateTime endTime = new DateTime();
        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys))).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.unfinished()).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.startedAfter(startTime.toDate())).thenReturn(historicProcessInstanceQuery);
        when(historicProcessInstanceQuery.finishedBefore(endTime.toDate())).thenReturn(historicProcessInstanceQuery);
        long expectedResult = 1234l;
        when(historicProcessInstanceQuery.count()).thenReturn(expectedResult);
        long actualResult =
            activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
        assertEquals(expectedResult, actualResult);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));
        inOrder.verify(historicProcessInstanceQuery).unfinished();
        inOrder.verify(historicProcessInstanceQuery).startedAfter(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).finishedBefore(endTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).count();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void getExecutionByProcessInstanceIdAndActivitiId()
    {
        String processInstanceId = "processInstanceId";
        String activitiId = "activitiId";
        ExecutionQuery executionQuery = mock(ExecutionQuery.class);
        when(activitiRuntimeService.createExecutionQuery()).thenReturn(executionQuery);
        when(executionQuery.processInstanceId(processInstanceId)).thenReturn(executionQuery);
        when(executionQuery.activityId(activitiId)).thenReturn(executionQuery);
        Execution expectedExecution = mock(Execution.class);
        when(executionQuery.singleResult()).thenReturn(expectedExecution);
        Execution actualExecution = activitiService.getExecutionByProcessInstanceIdAndActivitiId(processInstanceId, activitiId);
        assertSame(expectedExecution, actualExecution);
        InOrder inOrder = inOrder(executionQuery);
        inOrder.verify(executionQuery).processInstanceId(processInstanceId);
        inOrder.verify(executionQuery).activityId(activitiId);
        inOrder.verify(executionQuery).singleResult();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testSignal()
    {
        String executionId = "executionId";
        Map<String, Object> processVariables = new HashMap<>();
        activitiService.signal(executionId, processVariables);
        verify(activitiRuntimeService).signal(executionId, processVariables);
        verifyNoMoreInteractions(activitiRuntimeService);
    }

    @Test
    public void getProcessModel()
    {
        String processDefinitionId = "processDefinitionId";
        String expectedResult = "expectedResult";
        InputStream inputStream = new ByteArrayInputStream(expectedResult.getBytes());
        when(activitiRepositoryService.getProcessModel(processDefinitionId)).thenReturn(inputStream);
        String actualResult = activitiService.getProcessModel(processDefinitionId);
        assertEquals(expectedResult, actualResult);
        verify(activitiRepositoryService).getProcessModel(processDefinitionId);
        verifyNoMoreInteractions(activitiRepositoryService);
    }

    @Test
    public void getProcessModelWhenIOExceptionThrown() throws Exception
    {
        String processDefinitionId = "processDefinitionId";
        InputStream inputStream = mock(InputStream.class);
        when(inputStream.read()).thenThrow(new IOException());
        when(activitiRepositoryService.getProcessModel(processDefinitionId)).thenReturn(inputStream);
        try
        {
            activitiService.getProcessModel(processDefinitionId);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            assertSame(IOException.class, illegalArgumentException.getCause().getClass());
        }
    }

    @Test
    public void testDeleteProcessInstance()
    {
        String processInstanceId = "processInstanceId";
        String deleteReason = "deleteReason";
        activitiService.deleteProcessInstance(processInstanceId, deleteReason);
        verify(activitiRuntimeService).deleteProcessInstance(processInstanceId, deleteReason);
    }

    @Test
    public void testGetUnfinishedHistoricProcessInstancesByStartBeforeTime()
    {
        // Setup mock objects and expected behavior.
        DateTime startTime = new DateTime();

        HistoricProcessInstanceQuery historicProcessInstanceQuery = mock(HistoricProcessInstanceQuery.class);
        when(activitiHistoryService.createHistoricProcessInstanceQuery()).thenReturn(historicProcessInstanceQuery);

        List<HistoricProcessInstance> expectedHistoricProcessInstances = new ArrayList<>();
        HistoricProcessInstance historicProcessInstance1 = mock(HistoricProcessInstance.class);
        HistoricProcessInstance historicProcessInstance2 = mock(HistoricProcessInstance.class);
        expectedHistoricProcessInstances.add(historicProcessInstance1);
        expectedHistoricProcessInstances.add(historicProcessInstance2);
        when(historicProcessInstanceQuery.list()).thenReturn(expectedHistoricProcessInstances);

        // Call method being tested.
        List<HistoricProcessInstance> actualHistoricProcessInstances =
            activitiService.getUnfinishedHistoricProcessInstancesByStartBeforeTime(startTime);

        // Validate results.
        assertEquals(expectedHistoricProcessInstances, actualHistoricProcessInstances);
        InOrder inOrder = inOrder(historicProcessInstanceQuery);
        inOrder.verify(historicProcessInstanceQuery).unfinished();
        inOrder.verify(historicProcessInstanceQuery).startedBefore(startTime.toDate());
        inOrder.verify(historicProcessInstanceQuery).orderByProcessInstanceStartTime();
        inOrder.verify(historicProcessInstanceQuery).asc();
        inOrder.verify(historicProcessInstanceQuery).list();
        inOrder.verifyNoMoreInteractions();
    }
}
