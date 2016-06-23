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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.joda.time.DateTime;

import org.finra.herd.model.api.xml.JobStatusEnum;

/**
 * Service interface to Activiti operations. The purpose of this interface is to provide abstraction on top of direct Activiti API (ex. to avoid chaining query
 * builder methods directly in the business logic service layer).
 */
public interface ActivitiService
{
    /**
     * Gets a process definition by its ID.
     * 
     * @param processDefinitionId The process definition ID
     * @return The process definition
     */
    ProcessDefinition getProcessDefinitionById(String processDefinitionId);

    /**
     * Starts a process instance of a given process definition with the given variables.
     * 
     * @param processDefinitionId The process definition ID
     * @param variables The variables
     * @return The process instance
     */
    ProcessInstance startProcessInstanceByProcessDefinitionId(String processDefinitionId, Map<String, Object> variables);

    /**
     * Gets a process instance by its ID.
     * 
     * @param processInstanceId The process instance ID
     * @return The process instance
     */
    ProcessInstance getProcessInstanceById(String processInstanceId);

    /**
     * Gets a historic process instance by its process instance ID.
     * 
     * @param processInstanceId The process instance ID
     * @return The historic process instance
     */
    HistoricProcessInstance getHistoricProcessInstanceByProcessInstanceId(String processInstanceId);

    /**
     * Gets all historic activity instances by their process instance ID sorted by start time and end time.
     * 
     * @param processInstanceId The process instance ID
     * @return List of historic activity instances
     */
    List<HistoricActivityInstance> getHistoricActivityInstancesByProcessInstanceId(String processInstanceId);

    /**
     * Gets all jobs with exceptions by process instance ID.
     * 
     * @param processInstanceId The process instance ID
     * @return List of jobs
     */
    List<Job> getJobsWithExceptionByProcessInstanceId(String processInstanceId);

    /**
     * Gets the stacktrace of a job.
     * 
     * @param jobId The job ID
     * @return The stacktrace
     */
    String getJobExceptionStacktrace(String jobId);

    /**
     * Gets all process definitions by their IDs
     * 
     * @param processDefinitionIds The process definition IDs
     * @return List of process definitions
     */
    List<ProcessDefinition> getProcessDefinitionsByIds(Set<String> processDefinitionIds);

    /**
     * Gets the count of jobs with exceptions by their process instance ID.
     * 
     * @param processInstanceId The process instance ID
     * @return The count
     */
    long getJobsWithExceptionCountByProcessInstanceId(String processInstanceId);

    /**
     * Gets all historic process instances by their status and process definition keys.
     * 
     * @param jobStatus The job status. Optional.
     * @param processDefinitionKeys Collection of process definition keys
     * @param startTime an optional job start time
     * @param endTime an optional job end time
     * @return List of historic process instances
     */
    List<HistoricProcessInstance> getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(JobStatusEnum jobStatus, Collection<String> processDefinitionKeys,
        DateTime startTime, DateTime endTime);

    /**
     * Gets the count of historic process instances by their status and process definition keys.
     * 
     * @param jobStatus The job status. Optional.
     * @param processDefinitionKeys Collection of process definition keys
     * @param startTime an optional job start time
     * @param endTime an optional job end time
     * @return The count
     */
    long getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(JobStatusEnum jobStatus, Collection<String> processDefinitionKeys, DateTime startTime,
        DateTime endTime);

    /**
     * Gets an execution by its process instance ID and activiti ID.
     * 
     * @param processInstanceId The process instance ID
     * @param activitiId The activiti ID
     * @return The execution
     */
    Execution getExecutionByProcessInstanceIdAndActivitiId(String processInstanceId, String activitiId);

    /**
     * Signals an execution at a waiting state to continue with the given variables.
     * 
     * @param executionId The execution ID
     * @param processVariables The process variables
     */
    void signal(String executionId, Map<String, Object> processVariables);

    /**
     * Gets the process model by the given process definition ID.
     * 
     * @param processDefinitionId The process definition ID
     * @return The process model
     */
    String getProcessModel(String processDefinitionId);

    /**
     * Deletes a process instance by the given process instance ID and supplies the given delete reason.
     * 
     * @param processInstanceId The process instance ID
     * @param deleteReason The delete reason
     */
    void deleteProcessInstance(String processInstanceId, String deleteReason);
}
