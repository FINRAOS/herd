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
package org.finra.dm.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.JobQuery;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.dm.core.DmDateUtils;
import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.dao.helper.JavaPropertiesHelper;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.JobDefinitionParameterEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.JobCreateRequest;
import org.finra.dm.model.api.xml.JobSignalRequest;
import org.finra.dm.model.api.xml.JobStatusEnum;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.model.api.xml.S3PropertiesLocation;
import org.finra.dm.model.api.xml.WorkflowError;
import org.finra.dm.model.api.xml.WorkflowStep;
import org.finra.dm.service.JobService;
import org.finra.dm.service.activiti.ActivitiProcessInstanceCreator;
import org.finra.dm.service.activiti.ProcessInstanceHolder;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.service.helper.S3PropertiesLocationHelper;

/**
 * The job service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class JobServiceImpl implements JobService
{
    private static final Logger LOGGER = Logger.getLogger(JobServiceImpl.class);

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private ActivitiProcessInstanceCreator activitiProcessInstanceCreator;

    @Autowired
    private RuntimeService activitiRuntimeService;

    @Autowired
    private HistoryService activitiHistoryService;

    @Autowired
    private RepositoryService activitiRepositoryService;

    @Autowired
    private ManagementService activitiManagementService;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    private S3PropertiesLocationHelper s3PropertiesLocationHelper;

    @Override
    public Job createAndStartJob(JobCreateRequest request, boolean isAsync) throws Exception
    {
        // Perform the validation.
        validateJobCreateRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(request.getNamespace());

        // Get the job definition and ensure it exists.
        JobDefinitionEntity jobDefinitionEntity = dmDao.getJobDefinitionByAltKey(request.getNamespace(), request.getJobName());
        if (jobDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(
                "Job definition with name \"" + request.getJobName() + "\" doesn't exist for namespace \"" + request.getNamespace() + "\".");
        }

        // Build the parameters map
        Map<String, Object> mergedParameters = getParameters(jobDefinitionEntity, request);

        // Create a process instance holder to check for a handle to the process instance once it is created.
        ProcessInstanceHolder processInstanceHolder = new ProcessInstanceHolder();
        ProcessInstance processInstance = null;

        if (isAsync)
        {
            // Create and start the job asynchronously.
            Future<Void> future =
                activitiProcessInstanceCreator.createAndStartProcessInstanceAsync(jobDefinitionEntity.getActivitiId(), mergedParameters, processInstanceHolder);

            // Keep looping until a process instance was created (although not necessarily started) or until the the job has been created and started via
            // the async method.
            while ((!future.isDone()) && (processInstance == null))
            {
                // Try to get the process instance from the holder. It should be available once it is created, but before it is started.
                processInstance = processInstanceHolder.getProcessInstance();

                // If we don't have a process instance yet, sleep for a short time to give the async method time to move forward.
                if (processInstance == null)
                {
                    Thread.sleep(100);
                }
            }

            // Try to get the process instance from the holder one last time in case the job future is done and we didn't get it after the sleep above.
            processInstance = processInstanceHolder.getProcessInstance();

            // Cause an exception to be thrown assuming an exception caused the future to complete, but the process instance to be null.
            try
            {
                future.get();
            }
            catch (ExecutionException e)
            {
                /*
                 * Throwing a illegal argument exception here since we have no idea what the cause of this exception is.
                 * We can try adding custom handling for each type of known types of exception, but that would become quickly unmaintainable.
                 * For now we will assume the user can do something about it.
                 */
                throw new IllegalArgumentException("Error executing job. See cause for details.", e);
            }
            
            // If we don't have a process instance, this should mean that we weren't able to create the job and calling get should thrown the exception
            // to the caller as to why we couldn't.
            if (processInstance == null)
            {
                // If we get here, that means the future completed, but nobody populated the process instance which shouldn't happen. Just throw
                // and exception so we're aware of the problem.
                throw new IllegalStateException(
                    "Unable to create process instance for unknown reason for job definition \"" + jobDefinitionEntity.getName() + "\" and Activiti Id \"" +
                        jobDefinitionEntity.getActivitiId() + "\".");
            }
        }
        else
        {
            activitiProcessInstanceCreator.createAndStartProcessInstanceSync(jobDefinitionEntity.getActivitiId(), mergedParameters, processInstanceHolder);
            processInstance = processInstanceHolder.getProcessInstance();
        }

        // If we get here, we have a newly created process instance. Log to know it was created successfully.
        LOGGER.info("Created process instance with Id: " + processInstance.getProcessInstanceId() +
            " for process definition Id: " + jobDefinitionEntity.getActivitiId() + " with merged parameters: " + mergedParameters);

        // Create and return the job object.
        return createJobFromRequest(namespaceEntity.getCode(), jobDefinitionEntity.getName(), mergedParameters, processInstance.getProcessInstanceId());
    }

    @Override
    public Job getJob(String jobId, boolean verbose) throws Exception
    {
        String jobIdLocal = jobId;

        // Validate and trim required elements
        Assert.hasText(jobIdLocal, "A job Id must be specified.");
        jobIdLocal = jobIdLocal.trim();

        // Get process instance from Activiti runtime.
        ProcessInstance processInstance =
            activitiRuntimeService.createProcessInstanceQuery().processInstanceId(jobIdLocal).includeProcessVariables().singleResult();

        HistoricProcessInstance historicProcessInstance = null;
        List<HistoricActivityInstance> historicActivitiTasks = null;

        Job job = new Job();
        job.setId(jobIdLocal);

        // Get historic process instance if process not found or need verbose response
        if (processInstance == null || verbose)
        {
            historicProcessInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(jobIdLocal).includeProcessVariables().singleResult();

            // Get completed historic tasks if verbose
            if (historicProcessInstance != null && verbose)
            {
                historicActivitiTasks =
                    activitiHistoryService.createHistoricActivityInstanceQuery().processInstanceId(jobIdLocal).orderByHistoricActivityInstanceStartTime().asc()
                        .orderByHistoricActivityInstanceEndTime().asc().list();
            }
        }

        if (processInstance == null && historicProcessInstance == null)
        {
            // Process not found
            throw new ObjectNotFoundException(String.format("Job with Id: \"%s\" doesn't exist.", jobIdLocal));
        }
        else if (processInstance != null)
        {
            // Check if there are errors.
            JobQuery jobQuery = activitiManagementService.createJobQuery().withException().processInstanceId(processInstance.getProcessInstanceId());
            List<org.activiti.engine.runtime.Job> erroredJobs = jobQuery.list();

            if (CollectionUtils.isEmpty(erroredJobs))
            {
                // Process running without errors.
                job.setStatus(JobStatusEnum.RUNNING);
            }
            else
            {
                // Errors found.
                List<WorkflowError> workflowErrors = new ArrayList<>();

                for (org.activiti.engine.runtime.Job erroredJob : erroredJobs)
                {
                    WorkflowError workflowError = new WorkflowError();
                    workflowError.setErrorMessage(erroredJob.getExceptionMessage());
                    workflowError.setRetriesLeft(erroredJob.getRetries());
                    workflowError.setErrorStackTrace(activitiManagementService.getJobExceptionStacktrace(erroredJob.getId()));
                    workflowErrors.add(workflowError);
                }
                job.setStatus(JobStatusEnum.ERROR);
                job.setWorkflowErrors(workflowErrors);
            }

            if (processInstance.getActivityId() != null)
            {
                // Set current workflow step.
                WorkflowStep currentStep = new WorkflowStep();
                currentStep.setId(processInstance.getActivityId());
                job.setCurrentWorkflowStep(currentStep);
            }

            // Set the workflow variables.
            populateWorkflowParameters(job, processInstance.getProcessVariables());

            if (verbose && historicProcessInstance != null)
            {
                // Set completed steps
                populateCompletedActivitiSteps(job, historicActivitiTasks);
            }
            if (verbose)
            {
                // Set activiti workflow xml
                populateActivitiXml(job, processInstance.getProcessDefinitionId());
            }
        }
        else
        {
            // Process completed
            job.setStatus(JobStatusEnum.COMPLETED);

            // Set the workflow variables.
            populateWorkflowParameters(job, historicProcessInstance.getProcessVariables());

            if (verbose)
            {
                // Set completed steps
                populateCompletedActivitiSteps(job, historicActivitiTasks);

                // Set activiti workflow xml
                populateActivitiXml(job, historicProcessInstance.getProcessDefinitionId());
            }
        }

        return job;
    }

    @Override
    public Job signalJob(JobSignalRequest request) throws Exception
    {
        // Perform the validation.
        validateJobSignalRequest(request);

        Execution execution =
            activitiRuntimeService.createExecutionQuery().processInstanceId(request.getId()).activityId(request.getReceiveTaskId()).singleResult();

        if (execution == null)
        {
            throw new ObjectNotFoundException(
                String.format("No job found for matching job id: \"%s\" and receive task id: \"%s\".", request.getId(), request.getReceiveTaskId()));
        }

        // Retrieve the job before signaling.
        Job job = getJob(request.getId(), false);

        // Build the parameters map
        Map<String, Object> signalParameters = getParameters(request);

        // Signal the workflow.
        activitiRuntimeService.signal(request.getId(), signalParameters);

        // Build the parameters map merged with job and signal parameters.
        Map<String, Object> mergedParameters = new HashMap<>();

        for (Parameter jobParam : job.getParameters())
        {
            mergedParameters.put(jobParam.getName(), jobParam.getValue());
        }

        mergedParameters.putAll(signalParameters);

        // Update the parameters in job
        populateWorkflowParameters(job, mergedParameters);

        return job;
    }

    /**
     * Populates the job Object with workflow variables.
     *
     * @param job, the Job object
     * @param variables, the workflow variables
     */
    private void populateWorkflowParameters(Job job, Map<String, Object> variables)
    {
        List<Parameter> parameters = new ArrayList<>();
        for (Entry<String, Object> paramEntry : variables.entrySet())
        {
            Parameter parameter = new Parameter(paramEntry.getKey(), paramEntry.getValue() == null ? null : paramEntry.getValue().toString());
            dmHelper.maskPassword(parameter);
            parameters.add(parameter);
        }
        job.setParameters(parameters);
    }

    /**
     * Populates the job Object with completed workflow steps.
     *
     * @param job, the Job object
     * @param historicActivitiTasks, the completed activiti steps
     */
    private void populateCompletedActivitiSteps(Job job, List<HistoricActivityInstance> historicActivitiTasks)
    {
        // Set completed steps
        List<WorkflowStep> completedWorkflowSteps = new ArrayList<>();
        for (HistoricActivityInstance historicActivityInstance : historicActivitiTasks)
        {
            completedWorkflowSteps.add(new WorkflowStep(historicActivityInstance.getActivityId(), historicActivityInstance.getActivityName(),
                DmDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getStartTime()),
                DmDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getEndTime())));
        }

        job.setCompletedWorkflowSteps(completedWorkflowSteps);
    }

    /**
     * Populates the job Object with workflow xml.
     *
     * @param job, the Job object
     * @param processDefinitionId, the process definition Id.
     *
     * @throws IOException in case of errors.
     */
    private void populateActivitiXml(Job job, String processDefinitionId) throws IOException
    {
        job.setActivitiJobXml(IOUtils.toString(activitiRepositoryService.getProcessModel(processDefinitionId)));
    }

    /**
     * Validates the job create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateJobCreateRequest(JobCreateRequest request)
    {
        // Validate required elements
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        Assert.hasText(request.getJobName(), "A job name must be specified.");

        // Validate that parameter names are there and not duplicate
        dmHelper.validateParameters(request.getParameters());

        if (request.getS3PropertiesLocation() != null)
        {
            s3PropertiesLocationHelper.validate(request.getS3PropertiesLocation());
        }

        // Remove leading and trailing spaces.
        request.setNamespace(request.getNamespace().trim());
        request.setJobName(request.getJobName().trim());
    }

    /**
     * Creates a new job object from request.
     *
     * @param namespaceCd, the namespace Code
     * @param jobName, the job name
     * @param mergedParameters parameters that were submitted with the job
     * @param processInstanceId process instance ID of the submitted job
     *
     * @return the created job object
     */
    private Job createJobFromRequest(String namespaceCd, String jobName, Map<String, Object> mergedParameters, String processInstanceId)
    {
        // Create the job.
        Job job = new Job();
        job.setId(processInstanceId);
        job.setNamespace(namespaceCd);
        job.setJobName(jobName);

        // Populate parameters from process instance.
        if (!mergedParameters.isEmpty())
        {
            List<Parameter> jobParameters = new ArrayList<>();
            job.setParameters(jobParameters);
            for (Map.Entry<String, Object> entry : mergedParameters.entrySet())
            {
                Parameter parameter = new Parameter(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                dmHelper.maskPassword(parameter);
                jobParameters.add(parameter);
            }
        }

        return job;
    }

    /**
     * Validates the job signal request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateJobSignalRequest(JobSignalRequest request)
    {
        // Validate required elements
        Assert.hasText(request.getId(), "A job id must be specified.");
        Assert.hasText(request.getReceiveTaskId(), "A receive task id must be specified.");

        // Validate that parameter names are there and not duplicate
        dmHelper.validateParameters(request.getParameters());

        if (request.getS3PropertiesLocation() != null)
        {
            s3PropertiesLocationHelper.validate(request.getS3PropertiesLocation());
        }

        // Remove leading and trailing spaces.
        request.setId(request.getId().trim());
        request.setReceiveTaskId(request.getReceiveTaskId().trim());
    }

    /**
     * Gets a Java properties from the given S3 location, and puts the key-value pairs into the given parameters.
     * If either bucket name or object key is null, this method does nothing.
     * 
     * @param s3BucketName S3 bucket name
     * @param s3ObjectKey S3 object key
     * @param parameters parameters to merge
     */
    private void putParametersFromS3(String s3BucketName, String s3ObjectKey, Map<String, Object> parameters)
    {
        if (s3BucketName != null && s3ObjectKey != null)
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = dmDaoHelper.getS3FileTransferRequestParamsDto();
            Properties properties = s3Dao.getProperties(s3BucketName, s3ObjectKey, s3FileTransferRequestParamsDto);
            parameters.putAll(javaPropertiesHelper.toMap(properties));
        }
    }

    /**
     * Gets the parameters from the given {@link JobDefinitionEntity} and {@link JobCreateRequest} and their respective S3 locations provided. If there are any
     * parameter conflicts, this method will automatically merge them by a predefined precedence, from least to greatest:
     * <ol>
     * <li>Job Definition S3 location</li>
     * <li>Job Definition parameters</li>
     * <li>Job Create Request S3 location</li>
     * <li>Job Create Request parameters</li>
     * </ol>
     * 
     * @param jobDefinitionEntity {@link JobDefinitionEntity}
     * @param jobCreateRequest {@link JobCreateRequest}
     * @return merged parameters
     */
    private Map<String, Object> getParameters(JobDefinitionEntity jobDefinitionEntity, JobCreateRequest jobCreateRequest)
    {
        Map<String, Object> mergedParameters = new HashMap<>();

        // Get parameters from job definition S3 location
        putParametersFromS3(jobDefinitionEntity.getS3BucketName(), jobDefinitionEntity.getS3ObjectKey(), mergedParameters);

        // Get parameters from job definition parameters
        for (JobDefinitionParameterEntity definitionParam : jobDefinitionEntity.getParameters())
        {
            mergedParameters.put(definitionParam.getName(), definitionParam.getValue());
        }

        // Get parameters from job create request S3 location
        S3PropertiesLocation s3PropertiesLocation = jobCreateRequest.getS3PropertiesLocation();
        if (s3PropertiesLocation != null)
        {
            putParametersFromS3(s3PropertiesLocation.getBucketName(), s3PropertiesLocation.getKey(), mergedParameters);
        }

        // Get parameters from job create request parameters
        mergedParameters.putAll(toMap(jobCreateRequest.getParameters()));

        return mergedParameters;
    }

    /**
     * Gets the parameters from the given {@link JobSignalRequest} and the S3 location provided.
     * 
     * @param jobSignalRequest {@link JobSignalRequest}
     * @return parameters
     */
    private Map<String, Object> getParameters(JobSignalRequest jobSignalRequest)
    {
        Map<String, Object> signalParameters = new HashMap<>();

        // Get parameters from S3
        S3PropertiesLocation s3PropertiesLocation = jobSignalRequest.getS3PropertiesLocation();
        if (s3PropertiesLocation != null)
        {
            putParametersFromS3(s3PropertiesLocation.getBucketName(), s3PropertiesLocation.getKey(), signalParameters);
        }

        // Get parameters from request
        signalParameters.putAll(toMap(jobSignalRequest.getParameters()));

        return signalParameters;
    }

    /**
     * Converts the given list of {@link Parameter} into a map.
     * 
     * @param parameters list of {@link Parameter}
     * @return map of key-values
     */
    private Map<String, Object> toMap(List<Parameter> parameters)
    {
        Map<String, Object> map = new HashMap<>();
        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                map.put(parameter.getName(), parameter.getValue());
            }
        }
        return map;
    }
}
