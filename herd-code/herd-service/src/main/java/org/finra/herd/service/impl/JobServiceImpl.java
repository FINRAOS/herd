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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.activiti.engine.ActivitiObjectNotFoundException;
import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricActivityInstance;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricProcessInstanceQuery;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.JobQuery;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.api.xml.WorkflowError;
import org.finra.herd.model.api.xml.WorkflowStep;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.JobDefinitionParameterEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.JobService;
import org.finra.herd.service.activiti.ActivitiProcessInstanceCreator;
import org.finra.herd.service.activiti.ProcessInstanceHolder;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;
import org.finra.herd.service.helper.StorageDaoHelper;

/**
 * The job service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class JobServiceImpl implements JobService
{
    private static final Logger LOGGER = Logger.getLogger(JobServiceImpl.class);

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

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

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Override
    public Job createAndStartJob(JobCreateRequest request, boolean isAsync) throws Exception
    {
        // Perform the validation.
        validateJobCreateRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = herdDaoHelper.getNamespaceEntity(request.getNamespace());

        // Get the job definition and ensure it exists.
        JobDefinitionEntity jobDefinitionEntity = herdDao.getJobDefinitionByAltKey(request.getNamespace(), request.getJobName());
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

            // If we don't have a process instance, this should mean that we weren't able to create the job and calling get should thrown the exception
            // to the caller as to why we couldn't.
            if (processInstance == null)
            {
                // Cause an exception to be thrown assuming an exception caused the future to complete, but the process instance to be null.
                future.get();

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
            // Process is still running.
            job.setStatus(JobStatusEnum.RUNNING);

            // Check if there are errors.
            JobQuery jobQuery = activitiManagementService.createJobQuery().withException().processInstanceId(processInstance.getProcessInstanceId());
            List<org.activiti.engine.runtime.Job> erroredJobs = jobQuery.list();

            if (!CollectionUtils.isEmpty(erroredJobs))
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

    @Override
    public JobSummaries getJobs(String namespace, String jobName, JobStatusEnum jobStatus) throws Exception
    {
        // Trim the parameters.
        String namespaceTrimmed = namespace == null ? null : namespace.trim();
        String jobNameTrimmed = jobName == null ? null : jobName.trim();

        // Construct the list of job summaries to return.
        JobSummaries jobSummaries = new JobSummaries();

        // Create a query for all process instances from the history service. This will contain running and already completed processes instances.
        HistoricProcessInstanceQuery query = activitiHistoryService.createHistoricProcessInstanceQuery();

        // If a namespace or job name filter was specified, get a list of all job definitions for those parameters.
        if (StringUtils.isNotBlank(namespaceTrimmed) || StringUtils.isNotBlank(jobNameTrimmed))
        {
            // TODO: This check is temporary due to Activiti's inability to filter based on a list of processDefinitionKeys (see below TODO).
            // This should be removed once that fix is in place.
            if (((StringUtils.isNotBlank(namespaceTrimmed)) && (StringUtils.isBlank(jobNameTrimmed))) ||
                ((StringUtils.isBlank(namespaceTrimmed)) && (StringUtils.isNotBlank(jobNameTrimmed))))
            {
                throw new IllegalArgumentException(
                    "Namespace and job name must either both be specified or not specified. Namespace: \"" + namespaceTrimmed + "\", jobName: \"" +
                        jobNameTrimmed + "\".");
            }

            // Build a set of Activiti process definition keys for each job definition that was filtered.
            Set<String> processDefinitionKeys = new HashSet<>();
            List<JobDefinitionEntity> jobDefinitionEntities = herdDao.getJobDefinitionsByFilter(namespaceTrimmed, jobNameTrimmed);
            for (JobDefinitionEntity jobDefinitionEntity : jobDefinitionEntities)
            {
                processDefinitionKeys.add(herdHelper.buildActivitiIdString(jobDefinitionEntity.getNamespace().getCode(), jobDefinitionEntity.getName()));
            }

            // Make sure the filter parameters point to at least one valid job definition.
            Assert.isTrue(processDefinitionKeys.size() > 0, "No job definitions found for specified namespace and jobName filter parameters.");

            // Add the list of process definition keys to the query.
            // TODO: Activiti 5.16.3 only has a method to filter on 1 process definition key whereas the latest version of Activiti (currently 5.18.0)
            // has a method to filter on a list of process definition keys. We will use only the first one for now, but should change this to include
            // the full list once we upgrade.
            query.processDefinitionKey(processDefinitionKeys.iterator().next());
        }

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

        // Make sure the number of results is within the system configured limit.
        long processInstanceCount = query.count();
        int jobsMaxQueryResults = herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.JOBS_QUERY_MAX_RESULTS);
        Assert.isTrue(processInstanceCount <= jobsMaxQueryResults,
            "Too many jobs found for the specified filter parameters. The maximum number of results allowed is " + jobsMaxQueryResults +
                " and the number of results returned was " + processInstanceCount + ".");

        // Run the query to get the list of process instances.
        List<HistoricProcessInstance> historicProcessInstances = query.list();

        // Compile the Regex pattern.
        Pattern pattern = getNamespaceAndJobNameRegexPattern();

        // Loop over the process instances and build a list of job summaries to return.
        for (HistoricProcessInstance historicProcessInstance : historicProcessInstances)
        {
            // Create a new job summary.
            JobSummary jobSummary = new JobSummary();
            jobSummary.setId(historicProcessInstance.getId());

            // Set the namespace and job name on the job summary.
            setNamespaceAndJobNameInJobSummary(historicProcessInstance.getProcessDefinitionId(), pattern, jobSummary);

            // Set the start time always since all jobs will have a start time.
            jobSummary.setStartTime(HerdDateUtils.getXMLGregorianCalendarValue(historicProcessInstance.getStartTime()));

            if (historicProcessInstance.getEndTime() == null)
            {
                // Since there is no end time, the job is still running.
                jobSummary.setStatus(JobStatusEnum.RUNNING);

                // If the end time is null, then determine the status based on the presence of any exceptions.
                JobQuery jobQuery = activitiManagementService.createJobQuery().withException().processInstanceId(historicProcessInstance.getId());
                jobSummary.setTotalExceptions(jobQuery.count());
            }
            else
            {
                // If the end time is set, then the job has finished so set the end time and the status to completed.
                jobSummary.setEndTime(HerdDateUtils.getXMLGregorianCalendarValue(historicProcessInstance.getEndTime()));
                jobSummary.setStatus(JobStatusEnum.COMPLETED);
            }

            // If the status filter was specified and it doesn't match the actual status, loop so we don't add the job summary to the list.
            if ((jobStatus != null) && (!jobStatus.equals(jobSummary.getStatus())))
            {
                continue;
            }

            // Add the new summary to the list.
            jobSummaries.getJobSummaries().add(jobSummary);
        }

        // Return the list of job summaries.
        return jobSummaries;
    }

    /**
     * Gets the Regex pattern to match the namespace and job name.
     *
     * @return the Regex pattern.
     */
    private Pattern getNamespaceAndJobNameRegexPattern()
    {
        // Get the job definition template (e.g. ~namespace~.~jobName~) and Regex escape all periods (e.g. ~namespace~\\.~jobName~).
        String jobDefinitionTemplate = herdHelper.getActivitiJobDefinitionTemplate().replaceAll("\\.", "\\\\.");

        // Change the tokens to named capture groups (e.g. (?<namespace>.*)\\.(?<jobName>.*)).
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(herdHelper.getNamespaceToken(), "(?<namespace>.*)");
        jobDefinitionTemplate = jobDefinitionTemplate.replaceAll(herdHelper.getJobNameToken(), "(?<jobName>.*)");

        // Compile the Regex pattern.
        return Pattern.compile(jobDefinitionTemplate);
    }

    /**
     * Sets the namespace and jobName on the job summary given the specified process definition Id and Regex pattern.
     *
     * @param processDefinitionId the process definition Id.
     * @param regexPattern the Regex pattern
     * @param jobSummary the job summary to update.
     */
    private void setNamespaceAndJobNameInJobSummary(String processDefinitionId, Pattern regexPattern, JobSummary jobSummary)
    {
        // Default the result namespace to nothing and the job name to the full process definition Id (of the form "Namespace.JobName:1234:1234567").
        String resultNamespace = null;
        String resultJobName = processDefinitionId;

        // Get the returned process definition Id.
        String namespaceAndJobName = processDefinitionId;

        // Find the last index of the ":" character which is the last Activiti unique Id separator.
        int lastIndex = namespaceAndJobName.lastIndexOf(':');
        if (lastIndex == -1)
        {
            // Log a warning if it isn't present.
            LOGGER.warn("Last \":\" character of job definition id not found: \"" + namespaceAndJobName + "\".");
        }
        else
        {
            // Find the second to last index of the ":" character which is the second to last Activiti unique Id separator.
            int secondToLastIndex = namespaceAndJobName.lastIndexOf(':', lastIndex - 1);
            if (secondToLastIndex == -1)
            {
                // Log a warning if it isn't present.
                LOGGER.warn("Second to last \":\" character of job definition id not found: \"" + namespaceAndJobName + "\".");
            }
            else
            {
                if (secondToLastIndex == 0)
                {
                    // Log a warning if the second to last ":" character is at the starting position of the String (i.e. a namespace and jobName aren't
                    // present).
                    LOGGER.warn("Namespace and job name are missing from job definition id: \"" + namespaceAndJobName + "\".");
                }
                else
                {
                    // Truncate the Activiti unique Id's. This should leave just the namespace and jobName (e.g. Namespace.JobName).
                    namespaceAndJobName = namespaceAndJobName.substring(0, secondToLastIndex);

                    // Update the default result job name with what's left in case a namespace and jobName couldn't individually be found (perhaps with
                    // test data).
                    resultJobName = namespaceAndJobName;

                    // See if what's left matches our Regex for the namespace and jobName. If so, extract out the parts.
                    Matcher matcher = regexPattern.matcher(namespaceAndJobName);
                    if (matcher.find())
                    {
                        resultNamespace = matcher.group("namespace");
                        resultJobName = matcher.group("jobName");
                    }
                }
            }
        }

        // Set the namespace and jobName in the job summary.
        jobSummary.setNamespace(resultNamespace);
        jobSummary.setJobName(resultJobName);
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
            herdHelper.maskPassword(parameter);
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
                HerdDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getStartTime()),
                HerdDateUtils.getXMLGregorianCalendarValue(historicActivityInstance.getEndTime())));
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
        herdHelper.validateParameters(request.getParameters());

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
                herdHelper.maskPassword(parameter);
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
        herdHelper.validateParameters(request.getParameters());

        if (request.getS3PropertiesLocation() != null)
        {
            s3PropertiesLocationHelper.validate(request.getS3PropertiesLocation());
        }

        // Remove leading and trailing spaces.
        request.setId(request.getId().trim());
        request.setReceiveTaskId(request.getReceiveTaskId().trim());
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
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageDaoHelper.getS3FileTransferRequestParamsDto();
            Properties properties = s3Dao.getProperties(s3BucketName, s3ObjectKey, s3FileTransferRequestParamsDto);
            parameters.putAll(javaPropertiesHelper.toMap(properties));
        }
    }

    /**
     * Gets the parameters from the given {@link JobDefinitionEntity} and {@link JobCreateRequest} and their respective S3 locations provided. If there are any
     * parameter conflicts, this method will automatically merge them by a predefined precedence, from least to greatest:
     * <ol>
     * <li>Job Definition S3
     * location</li>
     * <li>Job Definition parameters</li>
     * <li>Job Create Request S3 location</li>
     * <li>Job Create Request parameters</li>
     * </ol>
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
     * Calls org.activiti.engine.RuntimeService.deleteProcessInstance(String, String) on the specified jobId.
     */
    @Override
    public Job deleteJob(String jobId, JobDeleteRequest jobDeleteRequest) throws Exception
    {
        Assert.hasText(jobId, "jobId must be specified");
        Assert.notNull(jobDeleteRequest, "jobDeleteRequest must be specified");
        Assert.hasText(jobDeleteRequest.getDeleteReason(), "deleteReason must be specified");

        try
        {
            activitiRuntimeService.deleteProcessInstance(jobId, jobDeleteRequest.getDeleteReason());
        }
        catch (ActivitiObjectNotFoundException activitiObjectNotFoundException)
        {
            throw new ObjectNotFoundException("Job with ID \"" + jobId + "\" does not exist or is already completed.", activitiObjectNotFoundException);
        }

        return getJob(jobId, false);
    }
}
