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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.Activity;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.validation.ValidationError;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.JobDefinitionParameterEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.JobDefinitionService;
import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.JobDefinitionDaoHelper;
import org.finra.herd.service.helper.JobDefinitionHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;

/**
 * The job definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class JobDefinitionServiceImpl implements JobDefinitionService
{
    // A deployable Activiti XML resource should have ".bpmn20.xml" extension as per Activiti user guide.
    private static final String ACTIVITI_DEPLOY_XML_SUFFIX = "bpmn20.xml";

    @Autowired
    private ActivitiHelper activitiHelper;

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    @Autowired
    private JobDefinitionDaoHelper jobDefinitionDaoHelper;

    @Autowired
    private JobDefinitionHelper jobDefinitionHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private RepositoryService activitiRepositoryService;

    @Autowired
    private S3PropertiesLocationHelper s3PropertiesLocationHelper;

    /**
     * Creates a new business object definition.
     *
     * @param request the business object definition create request.
     * @param enforceAsync True to enforce first task is async, false to ignore
     *
     * @return the created business object definition.
     */
    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public JobDefinition createJobDefinition(JobDefinitionCreateRequest request, boolean enforceAsync) throws Exception
    {
        // Perform the validation.
        validateJobDefinitionCreateRequest(request);

        if (enforceAsync)
        {
            assertFirstTaskIsAsync(activitiHelper.constructBpmnModelFromXmlAndValidate(request.getActivitiJobXml()));
        }

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespace());

        // Ensure a job definition with the specified name doesn't already exist.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(request.getNamespace(), request.getJobName());
        if (jobDefinitionEntity != null)
        {
            throw new AlreadyExistsException(
                "Unable to create job definition with name \"" + request.getJobName() + "\" because it already exists for namespace \"" +
                    request.getNamespace() + "\".");
        }

        // Create the new process definition.
        ProcessDefinition processDefinition = createProcessDefinition(request.getNamespace(), request.getJobName(), request.getActivitiJobXml());

        // Create a job definition entity from the request information.
        jobDefinitionEntity =
            createOrUpdateJobDefinitionEntity(null, namespaceEntity, request.getJobName(), request.getDescription(), processDefinition.getId(),
                request.getParameters(), request.getS3PropertiesLocation());

        // Persist the new entity.
        jobDefinitionEntity = jobDefinitionDao.saveAndRefresh(jobDefinitionEntity);

        // Create and return the job definition from the persisted entity.
        return createJobDefinitionFromEntity(jobDefinitionEntity);
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public JobDefinition getJobDefinition(String namespace, String jobName) throws Exception
    {
        // Validate the job definition alternate key.
        String namespaceLocal = alternateKeyHelper.validateStringParameter("namespace", namespace);
        String jobNameLocal = alternateKeyHelper.validateStringParameter("job name", jobName);

        // Retrieve and ensure that a job definition exists.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoHelper.getJobDefinitionEntity(namespaceLocal, jobNameLocal);

        // Create and return the job definition object from the persisted entity.
        return createJobDefinitionFromEntity(jobDefinitionEntity);
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public JobDefinition updateJobDefinition(String namespace, String jobName, JobDefinitionUpdateRequest request, boolean enforceAsync) throws Exception
    {
        // Validate the job definition alternate key.
        String namespaceLocal = alternateKeyHelper.validateStringParameter("namespace", namespace);
        String jobNameLocal = alternateKeyHelper.validateStringParameter("job name", jobName);

        // Validate and trim the Activiti job XML.
        Assert.hasText(request.getActivitiJobXml(), "An Activiti job XML must be specified.");
        request.setActivitiJobXml(request.getActivitiJobXml().trim());

        // Perform the job definition validation.
        validateJobDefinition(namespaceLocal, jobNameLocal, request.getActivitiJobXml(), request.getParameters(), request.getS3PropertiesLocation());

        if (enforceAsync)
        {
            assertFirstTaskIsAsync(activitiHelper.constructBpmnModelFromXmlAndValidate(request.getActivitiJobXml()));
        }

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceLocal);

        // Retrieve and ensure that a job definition exists.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoHelper.getJobDefinitionEntity(namespaceLocal, jobNameLocal);

        // Create the new process definition.
        ProcessDefinition processDefinition = createProcessDefinition(namespaceLocal, jobNameLocal, request.getActivitiJobXml());

        // Create a job definition entity from the request information.
        jobDefinitionEntity =
            createOrUpdateJobDefinitionEntity(jobDefinitionEntity, namespaceEntity, jobNameLocal, request.getDescription(), processDefinition.getId(),
                request.getParameters(), request.getS3PropertiesLocation());

        // Persist the entity.
        jobDefinitionEntity = jobDefinitionDao.saveAndRefresh(jobDefinitionEntity);

        // Create and return the job definition object from the persisted entity.
        return createJobDefinitionFromEntity(jobDefinitionEntity);
    }

    /**
     * Validates the job definition create request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateJobDefinitionCreateRequest(JobDefinitionCreateRequest request)
    {
        // Validate the job definition alternate key.
        request.setNamespace(alternateKeyHelper.validateStringParameter("namespace", request.getNamespace()));
        request.setJobName(alternateKeyHelper.validateStringParameter("job name", request.getJobName()));

        // Validate and trim the Activiti job XML.
        Assert.hasText(request.getActivitiJobXml(), "An Activiti job XML must be specified.");
        request.setActivitiJobXml(request.getActivitiJobXml().trim());

        // Perform the job definition validation.
        validateJobDefinition(request.getNamespace(), request.getJobName(), request.getActivitiJobXml(), request.getParameters(),
            request.getS3PropertiesLocation());
    }

    /**
     * Validates the job definition create request. This method also trims request parameters.
     *
     * @param namespace the namespace.
     * @param jobName the job name.
     * @param activitiJobXml the Activiti Job XML
     * @param parameters the list parameters.
     * @param s3PropertiesLocation {@link S3PropertiesLocation}
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateJobDefinition(String namespace, String jobName, String activitiJobXml, List<Parameter> parameters,
        S3PropertiesLocation s3PropertiesLocation)
    {
        // Ensure the Activiti XML doesn't contain a CDATA wrapper.
        Assert.isTrue(!activitiJobXml.contains("<![CDATA["), "Activiti XML can not contain a CDATA section.");

        // Ensure the Activiti XML doesn't contain a shell type task defined in it.
        Assert.isTrue(!activitiJobXml.contains("activiti:type=\"shell\""), "Activiti XML can not contain activti shell type service tasks.");

        // Convert Activiti XML into BpmnModel and validate.
        BpmnModel bpmnModel;
        try
        {
            bpmnModel = activitiHelper.constructBpmnModelFromXmlAndValidate(activitiJobXml);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException("Error processing Activiti XML: " + ex.getMessage(), ex);
        }

        // Validate that namespaceCd.jobName matched whats in Activiti job XML Id.
        String idInsideActivitiXml = bpmnModel.getProcesses().get(0).getId();
        Assert.hasText(idInsideActivitiXml, "ID inside Activiti job XML must be specified.");
        Assert.isTrue(idInsideActivitiXml.equalsIgnoreCase(jobDefinitionHelper.buildActivitiIdString(namespace, jobName)),
            "Namespace \"" + namespace + "\" and Job Name \"" + jobName + "\" does not match the ID specified within Activiti XML \"" +
                idInsideActivitiXml +
                "\".");

        // Validate that Activiti job XML is valid.
        List<ValidationError> activitiModelErrors = activitiRepositoryService.validateProcess(bpmnModel);
        StringBuilder validationErrors = new StringBuilder();
        for (ValidationError validationError : activitiModelErrors)
        {
            validationErrors.append('\n').append(validationError.getDefaultDescription());
        }
        Assert.isTrue(activitiModelErrors.isEmpty(), "Activiti XML is not valid, Errors: " + validationErrors);

        // Validate that parameter names are there and not duplicate.
        Map<String, String> parameterNameMap = new HashMap<>();
        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                Assert.hasText(parameter.getName(), "A parameter name must be specified.");
                parameter.setName(parameter.getName().trim());

                // Ensure the parameter key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String validationMapKey = parameter.getName().toLowerCase();
                Assert.isTrue(!parameterNameMap.containsKey(validationMapKey), "Duplicate parameter name found: " + parameter.getName());
                parameterNameMap.put(validationMapKey, validationMapKey);
            }
        }

        if (s3PropertiesLocation != null)
        {
            s3PropertiesLocationHelper.validate(s3PropertiesLocation);
        }
    }

    /**
     * Deploys the Activiti XML into Activiti given the specified namespace and job name. If an existing process definition with the specified namespace and job
     * name already exists, a new process definition with an incremented version will be created by Activiti.
     *
     * @param namespace the namespace.
     * @param jobName the job name.
     * @param activitiJobXml the Activiti job XML.
     *
     * @return the newly created process definition.
     */
    private ProcessDefinition createProcessDefinition(String namespace, String jobName, String activitiJobXml)
    {
        // Deploy Activiti XML using Activiti API.
        String activitiIdString = jobDefinitionHelper.buildActivitiIdString(namespace, jobName);
        Deployment deployment =
            activitiRepositoryService.createDeployment().name(activitiIdString).addString(activitiIdString + ACTIVITI_DEPLOY_XML_SUFFIX, activitiJobXml)
                .deploy();

        // Read the created process definition.
        return activitiRepositoryService.createProcessDefinitionQuery().deploymentId(deployment.getId()).list().get(0);
    }

    /**
     * Asserts that the first asyncable task in the given model is indeed asynchronous. Only asserts when the configuration is set to true.
     *
     * @param bpmnModel The BPMN model
     */
    private void assertFirstTaskIsAsync(BpmnModel bpmnModel)
    {
        if (Boolean.TRUE.equals(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ASSERT_ASYNC, Boolean.class)))
        {
            Process process = bpmnModel.getMainProcess();
            for (StartEvent startEvent : process.findFlowElementsOfType(StartEvent.class))
            {
                for (SequenceFlow sequenceFlow : startEvent.getOutgoingFlows())
                {
                    String targetRef = sequenceFlow.getTargetRef();
                    FlowElement targetFlowElement = process.getFlowElement(targetRef);
                    if (targetFlowElement instanceof Activity)
                    {
                        Assert.isTrue(((Activity) targetFlowElement).isAsynchronous(), "Element with id \"" + targetRef +
                            "\" must be set to activiti:async=true. All tasks which start the workflow must be asynchronous to prevent certain undesired " +
                            "transactional behavior, such as records of workflow not being saved on errors. Please refer to Activiti and herd documentations " +
                            "for details.");
                    }
                }
            }
        }
    }

    /**
     * Creates a new job definition entity from the request information.
     *
     * @param jobDefinitionEntity an optional existing job definition entity to update. If null, then a new one will be created.
     * @param namespaceEntity the namespace entity.
     * @param jobName the job name.
     * @param description the job definition description.
     * @param activitiId the Activiti Id.
     * @param parameters the job definition parameters.
     *
     * @return the newly created or existing updated job definition entity.
     */
    private JobDefinitionEntity createOrUpdateJobDefinitionEntity(JobDefinitionEntity jobDefinitionEntity, NamespaceEntity namespaceEntity, String jobName,
        String description, String activitiId, List<Parameter> parameters, S3PropertiesLocation s3PropertiesLocation)
    {
        JobDefinitionEntity jobDefinitionEntityLocal = jobDefinitionEntity;

        // If a job definition entity doesn't yet exist, create a new one.
        if (jobDefinitionEntityLocal == null)
        {
            jobDefinitionEntityLocal = new JobDefinitionEntity();
        }

        // Create a new entity.
        jobDefinitionEntityLocal.setName(jobName);
        jobDefinitionEntityLocal.setNamespace(namespaceEntity);
        jobDefinitionEntityLocal.setDescription(description);
        jobDefinitionEntityLocal.setActivitiId(activitiId);

        // Set or clear S3 properties location
        String bucketName = null;
        String key = null;
        if (s3PropertiesLocation != null)
        {
            bucketName = s3PropertiesLocation.getBucketName();
            key = s3PropertiesLocation.getKey();
        }
        jobDefinitionEntityLocal.setS3BucketName(bucketName);
        jobDefinitionEntityLocal.setS3ObjectKey(key);

        // Create the parameters.
        List<JobDefinitionParameterEntity> parameterEntities = new ArrayList<>();

        // As per generated JobDefinitionCreateRequest class, getParameters() never returns null.
        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                JobDefinitionParameterEntity parameterEntity = new JobDefinitionParameterEntity();
                parameterEntities.add(parameterEntity);
                parameterEntity.setName(parameter.getName());
                parameterEntity.setValue(parameter.getValue());
            }
        }

        // Set the new list of parameters on the entity.
        jobDefinitionEntityLocal.setParameters(parameterEntities);

        return jobDefinitionEntityLocal;
    }

    /**
     * Creates the job definition from the persisted entity.
     *
     * @param jobDefinitionEntity the newly persisted job definition entity.
     *
     * @return the job definition.
     */
    private JobDefinition createJobDefinitionFromEntity(JobDefinitionEntity jobDefinitionEntity) throws IOException
    {
        // Create the business object definition information.
        JobDefinition jobDefinition = new JobDefinition();
        jobDefinition.setId(jobDefinitionEntity.getId());
        jobDefinition.setNamespace(jobDefinitionEntity.getNamespace().getCode());
        jobDefinition.setJobName(jobDefinitionEntity.getName());
        jobDefinition.setDescription(jobDefinitionEntity.getDescription());

        String s3BucketName = jobDefinitionEntity.getS3BucketName();
        String s3ObjectKey = jobDefinitionEntity.getS3ObjectKey();
        if (s3BucketName != null && s3ObjectKey != null)
        {
            S3PropertiesLocation s3PropertiesLocation = new S3PropertiesLocation();
            s3PropertiesLocation.setBucketName(s3BucketName);
            s3PropertiesLocation.setKey(s3ObjectKey);
            jobDefinition.setS3PropertiesLocation(s3PropertiesLocation);
        }

        // Retrieve Activiti XML from activiti.
        ProcessDefinition processDefinition =
            activitiRepositoryService.createProcessDefinitionQuery().processDefinitionId(jobDefinitionEntity.getActivitiId()).singleResult();

        InputStream xmlStream = activitiRepositoryService.getResourceAsStream(processDefinition.getDeploymentId(), processDefinition.getResourceName());

        jobDefinition.setActivitiJobXml(IOUtils.toString(xmlStream));

        // Add in the parameters.
        List<Parameter> parameters = new ArrayList<>();
        jobDefinition.setParameters(parameters);

        for (JobDefinitionParameterEntity parameterEntity : jobDefinitionEntity.getParameters())
        {
            Parameter parameter = new Parameter(parameterEntity.getName(), parameterEntity.getValue());
            jobDefinitionHelper.maskPassword(parameter);
            parameters.add(parameter);
        }

        // Populate the "last updated by" user ID.
        jobDefinition.setLastUpdatedByUserId(jobDefinitionEntity.getUpdatedBy());

        return jobDefinition;
    }
}
