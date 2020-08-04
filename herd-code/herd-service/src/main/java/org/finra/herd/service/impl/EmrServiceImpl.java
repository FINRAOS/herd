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

import java.util.Date;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrStep;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.StatusChangeReason;
import org.finra.herd.model.api.xml.StatusTimeline;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterCreateDto;
import org.finra.herd.model.dto.EmrClusterPreCreateDto;
import org.finra.herd.model.dto.EmrParamsDto;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.service.EmrService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AwsServiceHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;
import org.finra.herd.service.helper.NamespaceDaoHelper;

/**
 * The EMR service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class EmrServiceImpl implements EmrService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrServiceImpl.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AwsServiceHelper awsServiceHelper;

    @Autowired
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Autowired
    private EmrDao emrDao;

    @Autowired
    private EmrHelper emrHelper;

    @Autowired
    private EmrHelperServiceImpl emrHelperServiceImpl;

    @Autowired
    private EmrStepHelperFactory emrStepHelperFactory;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#emrClusterAlternateKeyDto?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrCluster getCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose, String accountId,
        Boolean retrieveInstanceFleets) throws Exception
    {
        return getClusterImpl(emrClusterAlternateKeyDto, emrClusterId, emrStepId, verbose, accountId, retrieveInstanceFleets);
    }

    /**
     * Gets details of an existing EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param emrClusterId the cluster id of the cluster to get details
     * @param emrStepId the step id of the step to get details
     * @param verbose parameter for whether to return detailed information
     * @param accountId the optional AWS account that EMR cluster is running in
     * @param retrieveInstanceFleets parameter for whether to retrieve instance fleets
     *
     * @return the EMR Cluster object with details.
     */
    protected EmrCluster getClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose,
        String accountId, Boolean retrieveInstanceFleets)
    {
        EmrParamsDto emrParamsDto = emrHelper.getEmrParamsDtoByAccountId(accountId);

        // Perform the request validation.
        validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName()));

        EmrCluster emrCluster = createEmrClusterFromRequest(null, emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), accountId, null, null, null, null);
        String clusterName = emrHelper.buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        try
        {
            // Get Cluster status if clusterId is specified
            if (StringUtils.isNotBlank(emrClusterId))
            {
                Cluster cluster = emrDao.getEmrClusterById(emrClusterId.trim(), emrParamsDto);

                // Validate that, Cluster exists
                Assert.notNull(cluster, "An EMR cluster must exists with the cluster ID \"" + emrClusterId + "\".");

                // Validate that, Cluster name match as specified
                Assert.isTrue(clusterName.equalsIgnoreCase(cluster.getName()),
                    "Cluster name of specified cluster id \"" + emrClusterId + "\" must match the name specified.");
                emrCluster.setId(cluster.getId());
                setEmrClusterStatus(emrCluster, cluster.getStatus());
            }
            else
            {
                ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByNameAndAccountId(clusterName, accountId, emrParamsDto);

                // Validate that, Cluster exists with the name
                Assert.notNull(clusterSummary, "An EMR cluster must exists with the name \"" + clusterName + "\".");

                emrCluster.setId(clusterSummary.getId());
                setEmrClusterStatus(emrCluster, clusterSummary.getStatus());
            }

            // Get active step details
            if (emrHelper.isActiveEmrState(emrCluster.getStatus()))
            {
                StepSummary stepSummary = emrDao.getClusterActiveStep(emrCluster.getId(), emrParamsDto);
                if (stepSummary != null)
                {
                    EmrStep activeStep;

                    // If verbose get active step details
                    if (verbose)
                    {
                        activeStep = buildEmrStepFromAwsStep(stepSummary, true);
                    }
                    else
                    {
                        activeStep = buildEmrStepFromAwsStepSummary(stepSummary);
                    }
                    emrCluster.setActiveStep(activeStep);
                }
            }

            // Get requested step details
            if (StringUtils.isNotBlank(emrStepId))
            {
                Step step = emrDao.getClusterStep(emrCluster.getId(), emrStepId.trim(), emrParamsDto);

                emrCluster.setStep(buildEmrStepFromAwsStep(step, verbose));
            }

            // Get instance fleet if true
            if (BooleanUtils.isTrue(retrieveInstanceFleets))
            {
                ListInstanceFleetsResult listInstanceFleetsResult = emrDao.getListInstanceFleetsResult(emrCluster.getId(), emrParamsDto);
                emrCluster.setInstanceFleets(emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));
            }
        }
        catch (AmazonServiceException ex)
        {
            awsServiceHelper.handleAmazonException(ex, "An Amazon exception occurred while getting EMR cluster details with name \"" + clusterName + "\".");
        }

        return emrCluster;
    }

    /**
     * Builds EmrStep object from the EMR step. Fills in details if verbose=true.
     *
     * @param stepSummary The step summary
     * @param verbose The verbose flag
     *
     * @return EmrStep
     */
    private EmrStep buildEmrStepFromAwsStep(StepSummary stepSummary, boolean verbose)
    {
        EmrStep emrStep = new EmrStep();
        emrStep.setId(stepSummary.getId());
        emrStep.setStepName(stepSummary.getName());
        emrStep.setStatus(stepSummary.getStatus().getState());
        if (verbose)
        {
            emrStep.setJarLocation(stepSummary.getConfig().getJar());
            emrStep.setMainClass(stepSummary.getConfig().getMainClass());
            emrStep.setScriptArguments(stepSummary.getConfig().getArgs());
            emrStep.setContinueOnError(stepSummary.getActionOnFailure());
        }
        return emrStep;
    }

    /**
     * Builds EmrStep object from the EMR step. Fills in details if verbose=true.
     *
     * @param step The step
     * @param verbose The verbose flag
     *
     * @return EmrStep
     */
    private EmrStep buildEmrStepFromAwsStep(Step step, boolean verbose)
    {
        EmrStep emrStep = new EmrStep();
        emrStep.setId(step.getId());
        emrStep.setStepName(step.getName());
        emrStep.setStatus(step.getStatus().getState());
        if (verbose)
        {
            emrStep.setJarLocation(step.getConfig().getJar());
            emrStep.setMainClass(step.getConfig().getMainClass());
            emrStep.setScriptArguments(step.getConfig().getArgs());
            emrStep.setContinueOnError(step.getActionOnFailure());
        }
        return emrStep;
    }

    /**
     * Builds EmrStep object from the EMR StepSummary. Fills in details if verbose=true.
     */
    private EmrStep buildEmrStepFromAwsStepSummary(StepSummary stepSummary)
    {
        EmrStep emrStep = new EmrStep();
        emrStep.setId(stepSummary.getId());
        emrStep.setStepName(stepSummary.getName());
        emrStep.setStatus(stepSummary.getStatus().getState());

        return emrStep;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public EmrCluster createCluster(EmrClusterCreateRequest request) throws Exception
    {
        return createClusterImpl(request);
    }

    /**
     * <p> Creates a new EMR cluster based on the given request if the cluster with the given name does not already exist. If the cluster already exist,
     * returns the information about the existing cluster. </p> <p> The request must contain: </p> <ul> <li>A namespace and definition name which refer to an
     * existing EMR cluster definition.</li> <li>A valid cluster name to create.</li> </ul> <p> The request may optionally contain: </p> <ul> <li>A "dry run"
     * flag, which when set to {@code true}, no calls to AWS will occur, but validations and override will. Defaults to {@code false}.</li> <li>An override
     * parameter, which when set, overrides the given parameters in the cluster definition before creating the cluster. Defaults to no override. </li> </ul> <p>
     * A successful response will contain: </p> <ul> <li>The ID of the cluster that was created, or if the cluster already exists, the ID of the cluster that
     * exists. This field will be {@code null} when dry run flag is {@code true}.</li> <li>The status of the cluster that was created or already exists. The
     * status will normally be "Starting" on successful creations. This field will be {@code null} when dry run flag is {@code true}</li> <li>The namespace,
     * definition name, and cluster name of the cluster.</li> <li>The dry run flag, if given in the request.</li> <li>An indicator whether the cluster was
     * created or not. If the cluster already exists, the cluster will not be created and this flag will be set to {@code false}.</li> <li>The definition which
     * was used to create the cluster. If any overrides were given, this definition's values will be the values after the override. This field will be {@code
     * null} if the cluster was not created.</li> </ul> <p> Notes: </p> <ul> <li>At any point of the execution, if there are validation errors, the method will
     * immediately throw an exception.</li> <li>Even if the validations pass, AWS may still reject the request, which will cause this method to throw an
     * exception.</li>
     * <li>Dry runs do not make any calls to AWS, therefore AWS may still reject the creation request even when a dry run succeeds.</li> </ul>
     *
     * @param request - {@link EmrClusterCreateRequest} The EMR cluster create request
     *
     * @return {@link EmrCluster} the created EMR cluster object
     *
     * @throws Exception when the original EMR cluster definition XML is malformed
     */
    protected EmrCluster createClusterImpl(EmrClusterCreateRequest request) throws Exception
    {
        // Extract EMR cluster alternate key from the create request.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getEmrClusterAlternateKey(request);

        // Perform the request validation.
        validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Execute EMR cluster pre-creation steps, so we can close the transaction when calling AWS specific steps below.
        EmrClusterPreCreateDto emrClusterPreCreateDto = emrHelperServiceImpl.emrPreCreateClusterSteps(emrClusterAlternateKeyDto, request);

        // Get EMR cluster definition and other information from the EMR cluster pre-creation DTO.
        EmrClusterDefinition emrClusterDefinition = emrClusterPreCreateDto.getEmrClusterDefinition();

        // Get AWS account ID for the trusting account, if it is specified.
        String accountId = emrClusterDefinition.getAccountId();

        // Execute AWS specific steps related to EMR cluster creation.
        EmrClusterCreateDto emrClusterCreateDto = emrHelperServiceImpl
            .emrCreateClusterAwsSpecificSteps(request, emrClusterDefinition, emrClusterAlternateKeyDto, emrClusterPreCreateDto.getEmrParamsDto());

        if (emrClusterCreateDto.isEmrClusterCreated())
        {
            emrHelperServiceImpl.logEmrClusterCreation(emrClusterAlternateKeyDto, emrClusterDefinition, emrClusterCreateDto.getClusterId());
        }

        if (BooleanUtils.isTrue(emrClusterCreateDto.isEmrClusterAlreadyExists()))
        {
            // Do not include cluster definition in response
            emrClusterDefinition = null;
        }

        return createEmrClusterFromRequest(emrClusterCreateDto.getClusterId(), emrClusterAlternateKeyDto.getNamespace(),
            emrClusterAlternateKeyDto.getEmrClusterDefinitionName(), emrClusterAlternateKeyDto.getEmrClusterName(), accountId,
            emrClusterCreateDto.getEmrClusterStatus(), emrClusterCreateDto.isEmrClusterCreated(), request.isDryRun(), emrClusterDefinition);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#emrClusterAlternateKeyDto?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrCluster terminateCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId,
        String accountId) throws Exception
    {
        return terminateClusterImpl(emrClusterAlternateKeyDto, overrideTerminationProtection, emrClusterId, accountId);
    }

    /**
     * Terminates the EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param overrideTerminationProtection parameter for whether to override termination protection
     * @param emrClusterId The EMR cluster ID
     * @param accountId The account Id
     *
     * @return the terminated EMR cluster object
     */
    protected EmrCluster terminateClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId,
        String accountId)
    {
        EmrParamsDto emrParamsDto = emrHelper.getEmrParamsDtoByAccountId(accountId);

        // Perform the request validation.
        validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName()));

        String clusterId = null;
        String clusterName = emrHelper.buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        try
        {
            clusterId = emrHelper.getActiveEmrClusterId(emrClusterId, clusterName, accountId);
            emrDao.terminateEmrCluster(clusterId, overrideTerminationProtection, emrParamsDto);
        }
        catch (AmazonServiceException ex)
        {
            awsServiceHelper.handleAmazonException(ex, "An Amazon exception occurred while terminating EMR cluster with name \"" + clusterName + "\".");
        }

        return createEmrClusterFromRequest(clusterId, emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), accountId, emrDao.getEmrClusterStatusById(clusterId, emrParamsDto), null, null, null);
    }

    /**
     * Creates a EMR cluster alternate key from the relative values in the EMR Cluster Create Request.
     *
     * @param emrClusterCreateRequest the EMR cluster create request
     *
     * @return the EMR cluster alternate key
     */
    private EmrClusterAlternateKeyDto getEmrClusterAlternateKey(EmrClusterCreateRequest emrClusterCreateRequest)
    {
        return EmrClusterAlternateKeyDto.builder().withNamespace(emrClusterCreateRequest.getNamespace())
            .withEmrClusterDefinitionName(emrClusterCreateRequest.getEmrClusterDefinitionName()).withEmrClusterName(emrClusterCreateRequest.getEmrClusterName())
            .build();
    }

    /**
     * Creates a new EMR cluster object from request.
     *
     * @param clusterId the cluster Id.
     * @param namespaceCd the namespace Code
     * @param clusterDefinitionName the cluster definition
     * @param clusterName the cluster name
     * @param accountId the optional AWS account that EMR cluster is running in
     * @param clusterStatus the cluster status
     * @param emrClusterCreated whether EMR cluster was created.
     * @param dryRun The dry run flag.
     * @param emrClusterDefinition the EMR cluster definition.
     *
     * @return the created EMR cluster object
     */
    private EmrCluster createEmrClusterFromRequest(String clusterId, String namespaceCd, String clusterDefinitionName, String clusterName, String accountId,
        String clusterStatus, Boolean emrClusterCreated, Boolean dryRun, EmrClusterDefinition emrClusterDefinition)
    {
        // Create the EMR cluster.
        EmrCluster emrCluster = new EmrCluster();
        emrCluster.setId(clusterId);
        emrCluster.setNamespace(namespaceCd);
        emrCluster.setEmrClusterDefinitionName(clusterDefinitionName);
        emrCluster.setEmrClusterName(clusterName);
        emrCluster.setAccountId(accountId);
        emrCluster.setStatus(clusterStatus);
        emrCluster.setDryRun(dryRun);
        emrCluster.setEmrClusterCreated(emrClusterCreated);
        emrCluster.setEmrClusterDefinition(emrClusterDefinition);
        return emrCluster;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Object addStepToCluster(Object request) throws Exception
    {
        return addStepToClusterImpl(request);
    }

    /**
     * Adds step to an existing EMR Cluster.
     *
     * @param request the EMR steps add request
     *
     * @return the EMR step add object with added steps
     *
     * @throws Exception if there were any errors while adding a step to the cluster.
     */
    protected Object addStepToClusterImpl(Object request) throws Exception
    {
        EmrStepHelper stepHelper = emrStepHelperFactory.getStepHelper(request.getClass().getName());

        // Perform the request validation.
        validateAddStepToClusterRequest(request, stepHelper);

        // Perform the step specific validation
        stepHelper.validateAddStepRequest(request);

        //get accountId and awsParamDto
        String accountId = stepHelper.getRequestAccountId(request);
        EmrParamsDto emrParamsDto = emrHelper.getEmrParamsDtoByAccountId(accountId);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(stepHelper.getRequestNamespace(request), stepHelper.getRequestEmrClusterDefinitionName(request)));

        // Update the namespace and cluster definition name in request from database.
        stepHelper.setRequestNamespace(request, emrClusterDefinitionEntity.getNamespace().getCode());
        stepHelper.setRequestEmrClusterDefinitionName(request, emrClusterDefinitionEntity.getName());

        String clusterName = emrHelper.buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            stepHelper.getRequestEmrClusterName(request));
        Object emrStep = stepHelper.buildResponseFromRequest(request, emrParamsDto.getTrustingAccountStagingBucketName());

        try
        {
            String clusterId =
                emrHelper.getActiveEmrClusterId(stepHelper.getRequestEmrClusterId(request), clusterName, stepHelper.getRequestAccountId(request));
            stepHelper.setRequestEmrClusterId(request, clusterId);
            String stepId = emrDao.addEmrStep(clusterId, stepHelper.getEmrStepConfig(emrStep), emrParamsDto);
            stepHelper.setStepId(emrStep, stepId);
        }
        catch (AmazonServiceException ex)
        {
            awsServiceHelper.handleAmazonException(ex,
                "An Amazon exception occurred while adding EMR step \"" + stepHelper.getRequestStepName(request) + "\" to cluster with name \"" + clusterName +
                    "\".");
        }

        return emrStep;
    }

    /**
     * Validates the add steps to EMR cluster create request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateAddStepToClusterRequest(Object request, EmrStepHelper stepHelper) throws IllegalArgumentException
    {
        String namespace = stepHelper.getRequestNamespace(request);
        String clusterDefinitionName = stepHelper.getRequestEmrClusterDefinitionName(request);
        String clusterName = stepHelper.getRequestEmrClusterName(request);

        // Validate required elements
        Assert.hasText(namespace, "A namespace must be specified.");
        Assert.hasText(clusterDefinitionName, "An EMR cluster definition name must be specified.");
        Assert.hasText(clusterName, "An EMR cluster name must be specified.");

        // Remove leading and trailing spaces.
        stepHelper.setRequestNamespace(request, namespace.trim());
        stepHelper.setRequestEmrClusterDefinitionName(request, clusterDefinitionName.trim());
        stepHelper.setRequestEmrClusterName(request, clusterName.trim());
    }

    /**
     * Validates the add groups to EMR cluster master create request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateAddSecurityGroupsToClusterMasterRequest(EmrMasterSecurityGroupAddRequest request) throws IllegalArgumentException
    {
        // Validate required elements
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        Assert.hasText(request.getEmrClusterDefinitionName(), "An EMR cluster definition name must be specified.");
        Assert.hasText(request.getEmrClusterName(), "An EMR cluster name must be specified.");

        Assert.notEmpty(request.getSecurityGroupIds(), "At least one security group must be specified.");
        for (String securityGroup : request.getSecurityGroupIds())
        {
            Assert.hasText(securityGroup, "A security group value must be specified.");
        }

        // Remove leading and trailing spaces.
        request.setNamespace(request.getNamespace().trim());
        request.setEmrClusterDefinitionName(request.getEmrClusterDefinitionName().trim());
        request.setEmrClusterName(request.getEmrClusterName().trim());
        for (int i = 0; i < request.getSecurityGroupIds().size(); i++)
        {
            String element = request.getSecurityGroupIds().get(i);
            request.getSecurityGroupIds().set(i, element.trim());
        }
    }

    /**
     * Creates a new EMR master group object from request.
     *
     * @param namespaceCd, the namespace Code
     * @param clusterDefinitionName, the cluster definition name
     * @param clusterName, the cluster name
     * @param groupIds, the List of groupId
     *
     * @return the created EMR master group object
     */
    private EmrMasterSecurityGroup createEmrClusterMasterGroupFromRequest(String namespaceCd, String clusterDefinitionName, String clusterName,
        List<String> groupIds)
    {
        // Create the EMR cluster.
        EmrMasterSecurityGroup emrMasterSecurityGroup = new EmrMasterSecurityGroup();
        emrMasterSecurityGroup.setNamespace(namespaceCd);
        emrMasterSecurityGroup.setEmrClusterDefinitionName(clusterDefinitionName);
        emrMasterSecurityGroup.setEmrClusterName(clusterName);
        emrMasterSecurityGroup.setSecurityGroupIds(groupIds);

        return emrMasterSecurityGroup;
    }

    /**
     * Builds the {@link XMLGregorianCalendar} for the given {@link Date}
     *
     * @param date date
     *
     * @return XMLGregorianCalendar
     */
    private XMLGregorianCalendar toXmlGregorianCalendar(Date date)
    {
        XMLGregorianCalendar result = null;
        if (date != null)
        {
            result = HerdDateUtils.getXMLGregorianCalendarValue(date);
        }
        return result;
    }

    /**
     * Validates the EMR cluster create request. This method also trims request parameters.
     *
     * @param key the ERM cluster alternate key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateEmrClusterKey(EmrClusterAlternateKeyDto key) throws IllegalArgumentException
    {
        Assert.notNull(key, "An EMR cluster key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setEmrClusterDefinitionName(alternateKeyHelper.validateStringParameter("An", "EMR cluster definition name", key.getEmrClusterDefinitionName()));
        key.setEmrClusterName(alternateKeyHelper.validateStringParameter("An", "EMR cluster name", key.getEmrClusterName()));
    }

    /**
     * Updates EMR cluster model object with the specified EMR cluster status information.
     *
     * @param emrCluster the EMR cluster
     * @param clusterStatus the EMR cluster status information
     */
    private void setEmrClusterStatus(EmrCluster emrCluster, ClusterStatus clusterStatus)
    {
        // Log cluster status information.
        LOGGER.info("emrClusterId=\"{}\" emrClusterStatus={}", emrCluster.getId(), jsonHelper.objectToJson(clusterStatus));

        // Update the EMR cluster with the status information.
        emrCluster.setStatus(clusterStatus.getState());
        emrCluster
            .setStatusChangeReason(new StatusChangeReason(clusterStatus.getStateChangeReason().getCode(), clusterStatus.getStateChangeReason().getMessage()));
        emrCluster.setStatusTimeline(new StatusTimeline(toXmlGregorianCalendar(clusterStatus.getTimeline().getCreationDateTime()),
            toXmlGregorianCalendar(clusterStatus.getTimeline().getReadyDateTime()), toXmlGregorianCalendar(clusterStatus.getTimeline().getEndDateTime())));
    }
}
