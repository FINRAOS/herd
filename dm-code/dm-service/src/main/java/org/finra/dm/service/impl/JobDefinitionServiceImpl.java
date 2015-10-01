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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.BpmnModel;
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

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.JobDefinitionParameterEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.api.xml.JobDefinition;
import org.finra.dm.model.api.xml.JobDefinitionCreateRequest;
import org.finra.dm.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.model.api.xml.S3PropertiesLocation;
import org.finra.dm.service.JobDefinitionService;
import org.finra.dm.service.activiti.ActivitiHelper;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.service.helper.S3PropertiesLocationHelper;

/**
 * The job definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class JobDefinitionServiceImpl implements JobDefinitionService
{
    // A deployable Activiti XML resource should have ".bpmn20.xml" extension as per Activiti user guide.
    private static final String ACTIVITI_DEPLOY_XML_SUFFIX = "bpmn20.xml";

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private RepositoryService activitiRepositoryService;

    @Autowired
    private ActivitiHelper activitiHelper;

    @Autowired
    private S3PropertiesLocationHelper s3PropertiesLocationHelper;

    /**
     * Creates a new business object definition.
     *
     * @param request the business object definition create request.
     *
     * @return the created business object definition.
     */
    @Override
    public JobDefinition createJobDefinition(JobDefinitionCreateRequest request) throws Exception
    {
        // Perform the validation.
        validateJobDefinitionCreateRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(request.getNamespace());

        // Ensure a job definition with the specified name doesn't already exist.
        JobDefinitionEntity jobDefinitionEntity = dmDao.getJobDefinitionByAltKey(request.getNamespace(), request.getJobName());
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
        jobDefinitionEntity = dmDao.saveAndRefresh(jobDefinitionEntity);

        // Create and return the job definition from the persisted entity.
        return createJobDefinitionFromEntity(jobDefinitionEntity);
    }

    @Override
    public JobDefinition getJobDefinition(String namespace, String jobName) throws Exception
    {
        String namespaceLocal = namespace;
        String jobNameLocal = jobName;

        // Perform validation.
        Assert.hasText(namespaceLocal, "A namespace must be specified.");
        Assert.hasText(jobNameLocal, "A job name must be specified.");

        // Trim keys.
        namespaceLocal = namespaceLocal.trim();
        jobNameLocal = jobNameLocal.trim();

        // Retrieve and ensure that a job definition exists.
        JobDefinitionEntity jobDefinitionEntity = dmDaoHelper.getJobDefinitionEntity(namespaceLocal, jobNameLocal);

        // Create and return the job definition object from the persisted entity.
        return createJobDefinitionFromEntity(jobDefinitionEntity);
    }

    @Override
    public JobDefinition updateJobDefinition(String namespace, String jobName, JobDefinitionUpdateRequest request) throws Exception
    {
        String namespaceLocal = namespace;
        String jobNameLocal = jobName;

        // Perform the validation.
        validateJobDefinition(namespaceLocal, jobNameLocal, request.getActivitiJobXml(), request.getParameters(), request.getS3PropertiesLocation());

        // Trim data.
        namespaceLocal = namespaceLocal.trim();
        jobNameLocal = jobNameLocal.trim();
        request.setActivitiJobXml(request.getActivitiJobXml().trim());

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(namespaceLocal);

        // Retrieve and ensure that a job definition exists.
        JobDefinitionEntity jobDefinitionEntity = dmDaoHelper.getJobDefinitionEntity(namespaceLocal, jobNameLocal);

        // Create the new process definition.
        ProcessDefinition processDefinition = createProcessDefinition(namespaceLocal, jobNameLocal, request.getActivitiJobXml());

        // Create a job definition entity from the request information.
        jobDefinitionEntity =
            createOrUpdateJobDefinitionEntity(jobDefinitionEntity, namespaceEntity, jobNameLocal, request.getDescription(), processDefinition.getId(),
                request.getParameters(), request.getS3PropertiesLocation());

        // Persist the entity.
        jobDefinitionEntity = dmDao.saveAndRefresh(jobDefinitionEntity);

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
        // Perform the validation.
        validateJobDefinition(request.getNamespace(), request.getJobName(), request.getActivitiJobXml(), request.getParameters(), request
            .getS3PropertiesLocation());

        // Remove leading and trailing spaces.
        request.setNamespace(request.getNamespace().trim());
        request.setJobName(request.getJobName().trim());
        request.setActivitiJobXml(request.getActivitiJobXml().trim());
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
        String namespaceLocal = namespace;
        String jobNameLocal = jobName;
        String activitiJobXmlLocal = activitiJobXml;

        // Validate required elements.
        Assert.hasText(namespaceLocal, "A namespace must be specified.");
        Assert.hasText(jobNameLocal, "A job name must be specified.");
        Assert.hasText(activitiJobXmlLocal, "An Activiti job XML must be specified.");

        // Remove leading and trailing spaces for other validation below.
        // Note that this doesn't alter the strings after they get returned so callers will need to trim the fields also before persisting to the database.
        namespaceLocal = namespaceLocal.trim();
        jobNameLocal = jobNameLocal.trim();
        activitiJobXmlLocal = activitiJobXmlLocal.trim();

        // Ensure the Activiti XML doesn't contain a CDATA wrapper.
        Assert.isTrue(!activitiJobXmlLocal.contains("<![CDATA["), "Activiti XML can not contain a CDATA section.");

        // Convert Activiti XML into BpmnModel and validate.
        BpmnModel bpmnModel;
        try
        {
            bpmnModel = activitiHelper.constructBpmnModelFromXmlAndValidate(activitiJobXmlLocal);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException("Error processing Activiti XML: " + ex.getMessage(), ex);
        }

        // Validate that namespaceCd.jobName matched whats in Activiti job XML Id.
        String idInsideActivitiXml = bpmnModel.getProcesses().get(0).getId();
        Assert.hasText(idInsideActivitiXml, "ID inside Activiti job XML must be specified.");
        Assert.isTrue(idInsideActivitiXml.equalsIgnoreCase(dmHelper.buildActivitiIdString(namespaceLocal, jobNameLocal)),
            "Namespace \"" + namespaceLocal + "\" and Job Name \"" + jobNameLocal + "\" does not match the ID specified within Activiti XML \"" +
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
        String activitiIdString = dmHelper.buildActivitiIdString(namespace, jobName);
        Deployment deployment =
            activitiRepositoryService.createDeployment().name(activitiIdString).addString(activitiIdString + ACTIVITI_DEPLOY_XML_SUFFIX, activitiJobXml)
                .deploy();

        // Read the created process definition.
        return activitiRepositoryService.createProcessDefinitionQuery().deploymentId(deployment.getId()).list().get(0);
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
            dmHelper.maskPassword(parameter);
            parameters.add(parameter);
        }

        return jobDefinition;
    }
}
