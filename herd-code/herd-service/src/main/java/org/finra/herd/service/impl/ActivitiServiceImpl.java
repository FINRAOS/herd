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
package org.finra.herd.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricProcessInstanceQuery;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.service.ActivitiService;

/**
 * Service implementation to Activiti operations.
 */
@Service
public class ActivitiServiceImpl implements ActivitiService
{
    @Autowired
    private HistoryService activitiHistoryService;

    @Autowired
    private ManagementService activitiManagementService;

    @Autowired
    private RepositoryService activitiRepositoryService;

    @Autowired
    private RuntimeService activitiRuntimeService;

    @Override
    public ProcessDefinition getProcessDefinitionById(String processDefinitionId)
    {
        return activitiRepositoryService.createProcessDefinitionQuery().processDefinitionId(processDefinitionId).singleResult();
    }

    @Override
    public ProcessInstance startProcessInstanceByProcessDefinitionId(String processDefinitionId, Map<String, Object> variables)
    {
        return activitiRuntimeService.startProcessInstanceById(processDefinitionId, variables);
    }

    @Override
    public ProcessInstance getProcessInstanceById(String processInstanceId)
    {
        return activitiRuntimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).includeProcessVariables().singleResult();
    }

    @Override
    public HistoricProcessInstance getHistoricProcessInstanceByProcessInstanceId(String processInstanceId)
    {
        return activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(processInstanceId).includeProcessVariables().singleResult();
    }

    @Override
    public List<HistoricActivityInstance> getHistoricActivityInstancesByProcessInstanceId(String processInstanceId)
    {
        return activitiHistoryService.createHistoricActivityInstanceQuery().processInstanceId(processInstanceId).orderByHistoricActivityInstanceStartTime()
            .asc().orderByHistoricActivityInstanceEndTime().asc().list();
    }

    @Override
    public List<Job> getJobsWithExceptionByProcessInstanceId(String processInstanceId)
    {
        return activitiManagementService.createJobQuery().withException().processInstanceId(processInstanceId).list();
    }

    @Override
    public String getJobExceptionStacktrace(String jobId)
    {
        return activitiManagementService.getJobExceptionStacktrace(jobId);
    }

    @Override
    public List<ProcessDefinition> getProcessDefinitionsByIds(Set<String> processDefinitionIds)
    {
        return activitiRepositoryService.createProcessDefinitionQuery().processDefinitionIds(processDefinitionIds).list();
    }

    @Override
    public long getJobsWithExceptionCountByProcessInstanceId(String processInstanceId)
    {
        return activitiManagementService.createJobQuery().withException().processInstanceId(processInstanceId).count();
    }

    @Override
    public List<HistoricProcessInstance> getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(JobStatusEnum jobStatus,
        Collection<String> processDefinitionKeys, DateTime startTime, DateTime endTime)
    {
        return createHistoricProcessInstanceQuery(processDefinitionKeys, jobStatus, startTime, endTime).list();
    }

    @Override
    public long getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(JobStatusEnum jobStatus, Collection<String> processDefinitionKeys,
        DateTime startTime, DateTime endTime)
    {
        return createHistoricProcessInstanceQuery(processDefinitionKeys, jobStatus, startTime, endTime).count();
    }

    @Override
    public Execution getExecutionByProcessInstanceIdAndActivitiId(String processInstanceId, String activitiId)
    {
        return activitiRuntimeService.createExecutionQuery().processInstanceId(processInstanceId).activityId(activitiId).singleResult();
    }

    @Override
    public void signal(String executionId, Map<String, Object> processVariables)
    {
        activitiRuntimeService.signal(executionId, processVariables);
    }

    @Override
    public String getProcessModel(String processDefinitionId)
    {
        try
        {
            return IOUtils.toString(activitiRepositoryService.getProcessModel(processDefinitionId));
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteProcessInstance(String processInstanceId, String deleteReason)
    {
        activitiRuntimeService.deleteProcessInstance(processInstanceId, deleteReason);
    }

    /**
     * Creates a HistoricProcessInstanceQuery in the given process definitions, optionally filtering by the given job status, start and end times.
     * 
     * @param processDefinitionKeys Collection of process definition keys
     * @param jobStatus The job status. Optional.
     * @param startTime The start time. Optional.
     * @param endTime The end time. Optional.
     * 
     * @return A HistoricProcessInstanceQuery
     */
    private HistoricProcessInstanceQuery createHistoricProcessInstanceQuery(Collection<String> processDefinitionKeys, JobStatusEnum jobStatus,
        DateTime startTime, DateTime endTime)
    {
        HistoricProcessInstanceQuery query =
            activitiHistoryService.createHistoricProcessInstanceQuery().processDefinitionKeyIn(new ArrayList<>(processDefinitionKeys));

        if (JobStatusEnum.RUNNING.equals(jobStatus))
        {
            // If the filter is for "running", use the "unfinished" query filter.
            query.unfinished();
        }
        else if (JobStatusEnum.COMPLETED.equals(jobStatus))
        {
            // If the filter is for "completed" processes, use the "finished" query filter.
            query.finished();
        }

        if (startTime != null)
        {
            query.startedAfter(startTime.toDate());
        }

        if (endTime != null)
        {
            query.finishedBefore(endTime.toDate());
        }
        return query;
    }
}
