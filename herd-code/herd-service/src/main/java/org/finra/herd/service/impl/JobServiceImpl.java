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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.api.xml.WorkflowError;
import org.finra.herd.model.api.xml.WorkflowStep;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.JobDefinitionParameterEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.ActivitiService;
import org.finra.herd.service.JobService;
import org.finra.herd.service.helper.JobDefinitionHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceSecurityHelper;
import org.finra.herd.service.helper.ParameterHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;
import org.finra.herd.service.helper.StorageHelper;

/**
 * The job service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class JobServiceImpl implements JobService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    private ActivitiService activitiService;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    @Autowired
    private JobDefinitionHelper jobDefinitionHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceSecurityHelper namespaceSecurityHelper;

    @Autowired
    private ParameterHelper parameterHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private S3PropertiesLocationHelper s3PropertiesLocationHelper;

    @Autowired
    private StorageHelper storageHelper;

    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    public Job createAndStartJob(JobCreateRequest request) throws Exception
    {
        // Perform the validation.
        validateJobCreateRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespace());

        // Get the job definition and ensure it exists.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(request.getNamespace(), request.getJobName());
        if (jobDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(
                "Job definition with name \"" + request.getJobName() + "\" doesn't exist for namespace \"" + request.getNamespace() + "\".");
        }

        // Build the parameters map
        Map<String, Object> mergedParameters = getParameters(jobDefinitionEntity, request);

        // Create a process instance holder to check for a handle to the process instance once it is created.
        String processDefinitionId = jobDefinitionEntity.getActivitiId();
        ProcessDefinition processDefinition = activitiService.getProcessDefinitionById(processDefinitionId);
        Assert.notNull(processDefinition, "No process definition found for Id: " + processDefinitionId);
        Assert.isTrue(!processDefinition.isSuspended(),
            "Cannot start process instance for process definition Id: " + processDefinitionId + " because it is suspended.");
        ProcessInstance processInstance = activitiService.startProcessInstanceByProcessDefinitionId(processDefinitionId, mergedParameters);

        // If we get here, we have a newly created process instance. Log to know it was created successfully.
        LOGGER.info("Created process instance with Id: " + processInstance.getProcessInstanceId() +
            " for process definition Id: " + processDefinitionId + " with merged parameters: " + mergedParameters);

        // Create and return the job object.
        return createJobFromRequest(namespaceEntity.getCode(), jobDefinitionEntity.getName(), mergedParameters, processInstance.getProcessInstanceId());
    }

    @Override
    public Job deleteJob(String jobId, JobDeleteRequest jobDeleteRequest) throws Exception
    {
        Assert.hasText(jobId, "jobId must be specified");
        Assert.notNull(jobDeleteRequest, "jobDeleteRequest must be specified");
        Assert.hasText(jobDeleteRequest.getDeleteReason(), "deleteReason must be specified");

        boolean jobFound = false;
        ProcessInstance processInstance = activitiService.getProcessInstanceById(jobId);
        if (processInstance != null)
        {
            checkPermissions(processInstance.getProcessDefinitionKey(), new NamespacePermissionEnum[] {NamespacePermissionEnum.EXECUTE});

            activitiService.deleteProcessInstance(jobId, jobDeleteRequest.getDeleteReason());
            jobFound = true;
        }

        if (!jobFound)
        {
            throw new ObjectNotFoundException("Job with ID \"" + jobId + "\" does not exist or is already completed.");
        }

        return getJob(jobId, false, false);
    }

    @Override
    public Job getJob(String jobId, boolean verbose) throws Exception
    {
        return getJob(jobId, verbose, true);
    }

    @Override
    public JobSummaries getJobs(String namespace, String jobName, JobStatusEnum jobStatus, DateTime startTime, DateTime endTime) throws Exception
    {
        // Trim the parameters.
        String namespaceTrimmed = namespace == null ? null : namespace.trim();
        String jobNameTrimmed = jobName == null ? null : jobName.trim();

        // Construct the list of job summaries to return.
        JobSummaries jobSummaries = new JobSummaries();

        /*
         * Get the namespaces which the current user is authorized to READ.
         * If a specific namespace was requested, and the current user is authorized to read the namespace, include ONLY the requested namespace.
         * If a specific namespace was requested, but the current user is not authorized to read the namespace, clear all namespaces.
         * Otherwise, include all authorized namespaces.
         *
         * This ensures that only authorized namespaces are queried from the database and that
         * an unauthorized user cannot determine if he specified an existing namespace or not.
         */
        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        if (StringUtils.isNotBlank(namespaceTrimmed))
        {
            NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespaceTrimmed);
            if (namespaceEntity != null && authorizedNamespaces.contains(namespaceEntity.getCode()))
            {
                authorizedNamespaces.clear();
                authorizedNamespaces.add(namespaceEntity.getCode());
            }
            else
            {
                authorizedNamespaces.clear();
            }
        }

        // Continue the processing only when the list of authorized namespaces is not empty.
        if (!authorizedNamespaces.isEmpty())
        {
            // Build a set of Activiti process definition keys for each job definition that was filtered.
            Set<String> processDefinitionIds = new HashSet<>();
            for (JobDefinitionEntity jobDefinitionEntity : jobDefinitionDao.getJobDefinitionsByFilter(authorizedNamespaces, jobNameTrimmed))
            {
                processDefinitionIds.add(jobDefinitionEntity.getActivitiId());
            }

            // Continue the processing only when the list of filtered job definitions is not empty.
            if (!processDefinitionIds.isEmpty())
            {
                /*
                 * Keep a mapping of definition ID to its key.
                 * Activiti API does not provide an easy way to get the key without doing extra queries for each definition.
                 */
                Map<String, String> processDefinitionIdToKeyMap = new HashMap<>();
                for (ProcessDefinition processDefinition : activitiService.getProcessDefinitionsByIds(processDefinitionIds))
                {
                    processDefinitionIdToKeyMap.put(processDefinition.getId(), processDefinition.getKey());
                }

                List<HistoricProcessInstance> historicProcessInstances =
                    getHistoricProcessInstances(processDefinitionIdToKeyMap.values(), jobStatus, startTime, endTime);

                // Compile the Regex pattern.
                Pattern pattern = getNamespaceAndJobNameRegexPattern();

                // Loop over the process instances and build a list of job summaries to return.
                for (HistoricProcessInstance historicProcessInstance : historicProcessInstances)
                {
                    if (processDefinitionIdToKeyMap.containsKey(historicProcessInstance.getProcessDefinitionId()))
                    {
                        // Create a new job summary.
                        JobSummary jobSummary = new JobSummary();
                        jobSummary.setId(historicProcessInstance.getId());

                        // Set the namespace and job name on the job summary.
                        setNamespaceAndJobNameInJobSummary(processDefinitionIdToKeyMap.get(historicProcessInstance.getProcessDefinitionId()), pattern,
                            jobSummary);

                        // Set the start time always since all jobs will have a start time.
                        jobSummary.setStartTime(HerdDateUtils.getXMLGregorianCalendarValue(historicProcessInstance.getStartTime()));

                        if (historicProcessInstance.getEndTime() == null)
                        {
                            // Since there is no end time, the job is still running.
                            jobSummary.setStatus(JobStatusEnum.RUNNING);

                            // If the end time is null, then determine the status based on the presence of any exceptions.
                            jobSummary.setTotalExceptions(activitiService.getJobsWithExceptionCountByProcessInstanceId(historicProcessInstance.getId()));
                        }
                        else
                        {
                            // If the end time is set, then the job has finished so set the end time and the status to completed.
                            jobSummary.setEndTime(HerdDateUtils.getXMLGregorianCalendarValue(historicProcessInstance.getEndTime()));
                            jobSummary.setStatus(JobStatusEnum.COMPLETED);
                        }

                        // Add the new summary to the list.
                        jobSummaries.getJobSummaries().add(jobSummary);
                    }
                }
            }
        }

        // Return the list of job summaries.
        return jobSummaries;
    }

    @Override
    public Job signalJob(JobSignalRequest request) throws Exception
    {
        // Perform the validation.
        validateJobSignalRequest(request);

        Execution execution = activitiService.getExecutionByProcessInstanceIdAndActivitiId(request.getId(), request.getReceiveTaskId());

        if (execution == null)
        {
            throw new ObjectNotFoundException(
                String.format("No job found for matching job id: \"%s\" and receive task id: \"%s\".", request.getId(), request.getReceiveTaskId()));
        }

        String processDefinitionKey = activitiService.getProcessInstanceById(execution.getProcessInstanceId()).getProcessDefinitionKey();

        checkPermissions(processDefinitionKey, new NamespacePermissionEnum[] {NamespacePermissionEnum.EXECUTE});

        // Retrieve the job before signaling.
        Job job = getJob(request.getId(), false, false);

        // Build the parameters map
        Map<String, Object> signalParameters = getParameters(request);

        // Signal the workflow.
        activitiService.signal(execution.getId(), signalParameters);

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
     * Checks the namespace permissions for the current user for the given process definition key.
     *
     * @param processDefinitionKey The process definition key
     * @param permissions The list of permissions the current user must have
     */
    private void checkPermissions(String processDefinitionKey, NamespacePermissionEnum[] permissions)
    {
        Matcher matcher = getNamespaceAndJobNameRegexPattern().matcher(processDefinitionKey);
        if (matcher.find())
        {
            String namespace = matcher.group("namespace");
            namespaceSecurityHelper.checkPermission(namespace, permissions);
        }
        else
        {
            /*
             * Under normal operation, this exception would never be thrown.
             * The only possibility is if someone creates and executes a workflow without using our enforced definition key format. The only way to accomplish
             * this is directly operating against the database.
             * The check is here for robustness, but should not be expected to be covered as part of tests.
             */
            throw new IllegalArgumentException("Process definition key \"" + processDefinitionKey + "\" does not match the expected pattern");
        }
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
                jobDefinitionHelper.maskPassword(parameter);
                jobParameters.add(parameter);
            }
        }

        return job;
    }

    /**
     * Gets a list of historic process instances by the given process definition keys, job status, start time, and/or end time. If the given list of process
     * definition keys is empty or null, the result will not be filtered by the process definition keys. If the given job status is null, the result will not be
     * filtered by the job status.
     *
     * @param processDefinitionKeys Optional. The set of process definition keys
     * @param jobStatus an optional job status
     * @param startTime an optional job start time
     * @param endTime an optional job end time
     *
     * @return A list of historic process instances
     */
    private List<HistoricProcessInstance> getHistoricProcessInstances(Collection<String> processDefinitionKeys, JobStatusEnum jobStatus, DateTime startTime,
        DateTime endTime)
    {
        /*
         * Validates the result count before actually retrieving the result set, so that it would fail-fast and minimize the impact of the query.
         */
        long processInstanceCount =
            activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);

        int jobsMaxQueryResults = herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.JOBS_QUERY_MAX_RESULTS);
        Assert.isTrue(processInstanceCount <= jobsMaxQueryResults,
            "Too many jobs found for the specified filter parameters. The maximum number of results allowed is " + jobsMaxQueryResults
                + " and the number of results returned was " + processInstanceCount + ".");

        return activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(jobStatus, processDefinitionKeys, startTime, endTime);
    }

    /**
     * Gets a job by the given jobId, and displays verbose information if verbose flag is true. Does namespace permission check if namespace permissions flag is
     * true.
     *
     * @param jobId the job ID
     * @param verbose specifies if verbose mode is enabled or not
     * @param checkNamespacePermissions specifies whether to check namespace permissions or not
     *
     * @return the job information
     * @throws Exception if any problems were encountered
     */
    private Job getJob(String jobId, boolean verbose, boolean checkNamespacePermissions) throws Exception
    {
        String jobIdLocal = jobId;

        // Validate and trim required elements
        Assert.hasText(jobIdLocal, "A job Id must be specified.");
        jobIdLocal = jobIdLocal.trim();

        // Get process instance from Activiti runtime.
        ProcessInstance processInstance = activitiService.getProcessInstanceById(jobIdLocal);

        HistoricProcessInstance historicProcessInstance = null;
        List<HistoricActivityInstance> historicActivitiTasks = null;

        Job job = new Job();
        job.setId(jobIdLocal);

        // Get historic process instance if process not found or need verbose response
        if (processInstance == null || verbose)
        {
            historicProcessInstance = activitiService.getHistoricProcessInstanceByProcessInstanceId(jobIdLocal);

            // Get completed historic tasks if verbose
            if (historicProcessInstance != null && verbose)
            {
                historicActivitiTasks = activitiService.getHistoricActivityInstancesByProcessInstanceId(jobIdLocal);
            }
        }

        /*
         * Check permissions against namespace of the job.
         * Cannot be done through the annotation because the namespace is not part of the request.
         * TODO refactor this so it gets namespace from JobDefinitionEntity instead of parsing process definition key.
         */
        if (checkNamespacePermissions)
        {
            String processDefinitionKey = null;
            if (processInstance != null)
            {
                processDefinitionKey = processInstance.getProcessDefinitionKey();
            }
            else if (historicProcessInstance != null)
            {
                processDefinitionKey = activitiService.getProcessDefinitionById(historicProcessInstance.getProcessDefinitionId()).getKey();
            }
            if (processDefinitionKey != null)
            {
                checkPermissions(processDefinitionKey, new NamespacePermissionEnum[] {NamespacePermissionEnum.READ});
            }
        }

        if (processInstance == null && historicProcessInstance == null)
        {
            // Process not found
            throw new ObjectNotFoundException(String.format("Job with Id: \"%s\" doesn't exist.", jobIdLocal));
        }
        else if (processInstance != null)
        {
            // Process is still running.
            job.setStatus(JobStatusEnum.RUNNING);

            // Check if there are errors.
            List<org.activiti.engine.runtime.Job> erroredJobs = activitiService.getJobsWithExceptionByProcessInstanceId(processInstance.getProcessInstanceId());

            if (!CollectionUtils.isEmpty(erroredJobs))
            {
                // Errors found.
                List<WorkflowError> workflowErrors = new ArrayList<>();

                for (org.activiti.engine.runtime.Job erroredJob : erroredJobs)
                {
                    WorkflowError workflowError = new WorkflowError();
                    workflowError.setErrorMessage(erroredJob.getExceptionMessage());
                    workflowError.setRetriesLeft(erroredJob.getRetries());
                    workflowError.setErrorStackTrace(activitiService.getJobExceptionStacktrace(erroredJob.getId()));
                    workflowErrors.add(workflowError);
                }
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

            job.setDeleteReason(historicProcessInstance.getDeleteReason());

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

    /**
     * Gets the Regex pattern to match the namespace and job name from a process definition key.
     *
     * @return the Regex pattern.
     */
    private Pattern getNamespaceAndJobNameRegexPattern()
    {
        // Get the job definition template (e.g. ~namespace~.~jobName~) and Regex escape all periods (e.g. ~namespace~\\.~jobName~).
        String jobDefinitionTemplate = jobDefinitionHelper.getActivitiJobDefinitionTemplate().replaceAll("\\.", "\\\\.");

        // Change the tokens to named capture groups (e.g. (?<namespace>.*)\\.(?<jobName>.*)).
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(jobDefinitionHelper.getNamespaceToken(), "(?<namespace>.*)");
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(jobDefinitionHelper.getJobNameToken(), "(?<jobName>.*)");

        // Compile the Regex pattern.
        return Pattern.compile(jobDefinitionTemplate);
    }

    /**
     * Gets the parameters from the given {@link JobDefinitionEntity} and {@link JobCreateRequest} and their respective S3 locations provided. If there are any
     * parameter conflicts, this method will automatically merge them by a predefined precedence, from least to greatest: <ol> <li>Job Definition S3
     * location</li> <li>Job Definition parameters</li> <li>Job Create Request S3 location</li> <li>Job Create Request parameters</li> </ol>
     *
     * @param jobDefinitionEntity {@link JobDefinitionEntity}
     * @param jobCreateRequest {@link JobCreateRequest}
     *
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
     *
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
     * Populates the job Object with workflow xml.
     *
     * @param job, the Job object
     * @param processDefinitionId, the process definition Id.
     *
     * @throws IOException in case of errors.
     */
    private void populateActivitiXml(Job job, String processDefinitionId) throws IOException
    {
        job.setActivitiJobXml(activitiService.getProcessModel(processDefinitionId));
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
                HerdDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getStartTime()),
                HerdDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getEndTime())));
        }

        job.setCompletedWorkflowSteps(completedWorkflowSteps);
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
            jobDefinitionHelper.maskPassword(parameter);
            parameters.add(parameter);
        }
        job.setParameters(parameters);
    }

    /**
     * Gets a Java properties from the given S3 location, and puts the key-value pairs into the given parameters. If either bucket name or object key is null,
     * this method does nothing.
     *
     * @param s3BucketName S3 bucket name
     * @param s3ObjectKey S3 object key
     * @param parameters parameters to merge
     */
    private void putParametersFromS3(String s3BucketName, String s3ObjectKey, Map<String, Object> parameters)
    {
        if (s3BucketName != null && s3ObjectKey != null)
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            Properties properties = s3Dao.getProperties(s3BucketName, s3ObjectKey, s3FileTransferRequestParamsDto);
            parameters.putAll(javaPropertiesHelper.toMap(properties));
        }
    }

    /**
     * Sets the namespace and jobName on the job summary given the specified process definition Id and Regex pattern. TODO remove this method in preference to
     * use a job definition entity to retrieve namespace and job name when we refactor this class. We should no longer assume the job definition's namespace and
     * job name from Activiti generaeted process definition keys.
     *
     * @param processDefinitionKey the process definition key.
     * @param regexPattern the Regex pattern
     * @param jobSummary the job summary to update.
     */
    private void setNamespaceAndJobNameInJobSummary(String processDefinitionKey, Pattern regexPattern, JobSummary jobSummary)
    {
        // Default the result namespace to nothing and the job name to the full process definition Id (of the form "Namespace.JobName:1234:1234567").
        String resultNamespace = null;

        // Update the default result job name with what's left in case a namespace and jobName couldn't individually be found (perhaps with
        // test data).
        String resultJobName = processDefinitionKey;

        if (processDefinitionKey != null)
        {
            // See if what's left matches our Regex for the namespace and jobName. If so, extract out the parts.
            Matcher matcher = regexPattern.matcher(processDefinitionKey);
            if (matcher.find())
            {
                resultNamespace = matcher.group("namespace");
                resultJobName = matcher.group("jobName");
            }
        }

        // Set the namespace and jobName in the job summary.
        jobSummary.setNamespace(resultNamespace);
        jobSummary.setJobName(resultJobName);
    }

    /**
     * Converts the given list of {@link Parameter} into a map.
     *
     * @param parameters list of {@link Parameter}
     *
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
        parameterHelper.validateParameters(request.getParameters());

        if (request.getS3PropertiesLocation() != null)
        {
            s3PropertiesLocationHelper.validate(request.getS3PropertiesLocation());
        }

        // Remove leading and trailing spaces.
        request.setNamespace(request.getNamespace().trim());
        request.setJobName(request.getJobName().trim());
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
        parameterHelper.validateParameters(request.getParameters());

        if (request.getS3PropertiesLocation() != null)
        {
            s3PropertiesLocationHelper.validate(request.getS3PropertiesLocation());
        }

        // Remove leading and trailing spaces.
        request.setId(request.getId().trim());
        request.setReceiveTaskId(request.getReceiveTaskId().trim());
    }
}
