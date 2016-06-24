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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.OozieDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.EmrPricingHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.dao.impl.OozieDaoImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrStep;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.OozieWorkflowAction;
import org.finra.herd.model.api.xml.OozieWorkflowJob;
import org.finra.herd.model.api.xml.RunOozieWorkflowRequest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.EmrService;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceIamRoleAuthorizationHelper;
import org.finra.herd.service.helper.ParameterHelper;

/**
 * The EMR service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class EmrServiceImpl implements EmrService
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Autowired
    private EmrClusterDefinitionHelper emrClusterDefinitionHelper;

    @Autowired
    private EmrDao emrDao;

    @Autowired
    private EmrHelper emrHelper;

    @Autowired
    private EmrPricingHelper emrPricingHelper;

    @Autowired
    private EmrStepHelperFactory emrStepHelperFactory;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceIamRoleAuthorizationHelper namespaceIamRoleAuthorizationHelper;

    @Autowired
    private OozieDao oozieDao;

    @Autowired
    private ParameterHelper parameterHelper;

    @Autowired
    private XmlHelper xmlHelper;

    /**
     * Gets details of an existing EMR Cluster. Creates its own transaction.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param emrClusterId the cluster id of the cluster to get details
     * @param emrStepId the step id of the step to get details
     * @param verbose parameter for whether to return detailed information
     * @param retrieveOozieJobs parameter for whether to retrieve oozie job information
     *
     * @return the EMR Cluster object with details.
     * @throws Exception
     */
    @NamespacePermission(fields = "#emrClusterAlternateKeyDto?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrCluster getCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose,
        boolean retrieveOozieJobs) throws Exception
    {
        return getClusterImpl(emrClusterAlternateKeyDto, emrClusterId, emrStepId, verbose, retrieveOozieJobs);
    }

    /**
     * Gets details of an existing EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param emrClusterId the cluster id of the cluster to get details
     * @param emrStepId the step id of the step to get details
     * @param verbose parameter for whether to return detailed information
     * @param retrieveOozieJobs parameter for whether to retrieve oozie job information
     *
     * @return the EMR Cluster object with details.
     * @throws Exception if an error occurred while getting the cluster.
     */
    protected EmrCluster getClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose,
        boolean retrieveOozieJobs) throws Exception
    {
        // Perform the request validation.
        emrHelper.validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(emrClusterAlternateKeyDto.getNamespace());

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper
            .getEmrClusterDefinitionEntity(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName());

        EmrCluster emrCluster =
            createEmrClusterFromRequest(null, namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterAlternateKeyDto.getEmrClusterName(),
                null, null, null, null);
        String clusterName =
            emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterAlternateKeyDto.getEmrClusterName());
        try
        {
            // Get Cluster status if clusterId is specified
            if (StringUtils.isNotBlank(emrClusterId))
            {
                Cluster cluster = emrDao.getEmrClusterById(emrClusterId.trim(), emrHelper.getAwsParamsDto());

                // Validate that, Cluster exists
                Assert.notNull(cluster, "An EMR cluster must exists with the cluster ID \"" + emrClusterId + "\".");

                // Validate that, Cluster name match as specified
                Assert.isTrue(clusterName.equalsIgnoreCase(cluster.getName()),
                    "Cluster name of specified cluster id \"" + emrClusterId + "\" must match the name specified.");
                emrCluster.setId(cluster.getId());
                emrCluster.setStatus(cluster.getStatus().getState());
            }
            else
            {
                ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(clusterName, emrHelper.getAwsParamsDto());

                // Validate that, Cluster exists with the name
                Assert.notNull(clusterSummary, "An EMR cluster must exists with the name \"" + clusterName + "\".");

                emrCluster.setId(clusterSummary.getId());
                emrCluster.setStatus(clusterSummary.getStatus().getState());
            }

            // Get active step details
            if (emrHelper.isActiveEmrState(emrCluster.getStatus()))
            {
                StepSummary stepSummary = emrDao.getClusterActiveStep(emrCluster.getId(), emrHelper.getAwsParamsDto());
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
                Step step = emrDao.getClusterStep(emrCluster.getId(), emrStepId.trim(), emrHelper.getAwsParamsDto());

                emrCluster.setStep(buildEmrStepFromAwsStep(step, verbose));
            }

            // Get oozie job details if requested.
            if (retrieveOozieJobs && (emrCluster.getStatus().equalsIgnoreCase("RUNNING") || emrCluster.getStatus().equalsIgnoreCase("WAITING")))
            {
                emrCluster.setOozieWorkflowJobs(retrieveOozieJobs(emrCluster.getId()));
            }
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while getting EMR cluster details with name \"" + clusterName + "\".");
        }

        return emrCluster;
    }

    /**
     * Retrieves the List of running oozie workflow jobs on the cluster.
     *
     * @param clusterId the cluster Id
     *
     * @return the List of running oozie workflow jobs on the cluster.
     * @throws Exception
     */
    private List<OozieWorkflowJob> retrieveOozieJobs(String clusterId) throws Exception
    {
        // Retrieve cluster's master instance IP
        String masterIpAddress = getEmrClusterMasterIpAddress(clusterId);

        // Number of jobs to be included in the response.
        int jobsToInclude = herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.EMR_OOZIE_JOBS_TO_INCLUDE_IN_CLUSTER_STATUS);

        // List of wrapper jobs that have been found.
        List<WorkflowJob> jobsFound = oozieDao.getRunningEmrOozieJobsByName(masterIpAddress, OozieDaoImpl.HERD_OOZIE_WRAPPER_WORKFLOW_NAME, 1, jobsToInclude);

        // Construct the response
        List<OozieWorkflowJob> oozieWorkflowJobs = new ArrayList<>();

        for (WorkflowJob workflowJob : jobsFound)
        {
            // Get the client Workflow id.
            WorkflowAction clientWorkflowAction = emrHelper.getClientWorkflowAction(workflowJob);

            OozieWorkflowJob resultOozieWorkflowJob = new OozieWorkflowJob();
            resultOozieWorkflowJob.setId(workflowJob.getId());

            // If client workflow is null means that herd wrapper workflow has not started the client workflow yet.
            // Hence return status that it is still in preparation.
            if (clientWorkflowAction == null)
            {
                resultOozieWorkflowJob.setStatus(OozieDaoImpl.OOZIE_WORKFLOW_JOB_STATUS_DM_PREP);
            }
            else
            {
                resultOozieWorkflowJob.setStartTime(toXmlGregorianCalendar(clientWorkflowAction.getStartTime()));
                resultOozieWorkflowJob.setStatus(workflowJob.getStatus().toString());
            }

            oozieWorkflowJobs.add(resultOozieWorkflowJob);
        }

        return oozieWorkflowJobs;
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
     * Creates a new EMR Cluster. Creates its own transaction.
     *
     * @param request the EMR cluster create request
     *
     * @return the created EMR cluster object
     * @throws Exception if there were any errors while creating the cluster.
     */
    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrCluster createCluster(EmrClusterCreateRequest request) throws Exception
    {
        return createClusterImpl(request);
    }

    /**
     * <p> Creates a new EMR cluster based on the given request if the cluster with the given name does not already exist. If the cluster already exist, returns
     * the information about the existing cluster. </p> <p> The request must contain: </p> <ul> <li>A namespace and definition name which refer to an existing
     * EMR cluster definition.</li> <li>A valid cluster name to create.</li> </ul> <p> The request may optionally contain: </p> <ul> <li>A "dry run" flag, which
     * when set to {@code true}, no calls to AWS will occur, but validations and override will. Defaults to {@code false}.</li> <li>An override parameter, which
     * when set, overrides the given parameters in the cluster definition before creating the cluster. Defaults to no override. </li> </ul> <p> A successful
     * response will contain: </p> <ul> <li>The ID of the cluster that was created, or if the cluster already exists, the ID of the cluster that exists. This
     * field will be {@code null} when dry run flag is {@code true}.</li> <li>The status of the cluster that was created or already exists. The status will
     * normally be "Starting" on successful creations. This field will be {@code null} when dry run flag is {@code true}</li> <li>The namespace, definition
     * name, and cluster name of the cluster.</li> <li>The dry run flag, if given in the request.</li> <li>An indicator whether the cluster was created or not.
     * If the cluster already exists, the cluster will not be created and this flag will be set to {@code false}.</li> <li>The definition which was used to
     * create the cluster. If any overrides were given, this definition's values will be the values after the override. This field will be {@code null} if the
     * cluster was not created.</li> </ul> <p> Notes: </p> <ul> <li>At any point of the execution, if there are validation errors, the method will immediately
     * throw an exception.</li> <li>Even if the validations pass, AWS may still reject the request, which will cause this method to throw an exception.</li>
     * <li>Dry runs do not make any calls to AWS, therefore AWS may still reject the creation request even when a dry run succeeds.</li> </ul>
     *
     * @param request - {@link EmrClusterCreateRequest} The EMR cluster create request
     *
     * @return {@link EmrCluster} the created EMR cluster object
     * @throws Exception when the original EMR cluster definition XML is malformed
     */
    protected EmrCluster createClusterImpl(EmrClusterCreateRequest request) throws Exception
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDto();

        // Extract EMR cluster alternate key from the create request.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getEmrClusterAlternateKey(request);

        // Perform the request validation.
        emrHelper.validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(emrClusterAlternateKeyDto.getNamespace());

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper
            .getEmrClusterDefinitionEntity(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName());

        // Replace all S3 managed location variables in xml

        String toReplace = getS3ManagedReplaceString();
        String replacedConfigXml = emrClusterDefinitionEntity.getConfiguration().replaceAll(toReplace, emrHelper.getS3StagingLocation());

        // Unmarshal definition xml into JAXB object.
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, replacedConfigXml);

        // Perform override if override is set
        overrideEmrClusterDefinition(emrClusterDefinition, request.getEmrClusterDefinitionOverride());

        // Perform the EMR cluster definition configuration validation.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);

        namespaceIamRoleAuthorizationHelper.checkPermissions(emrClusterDefinitionEntity.getNamespace(), emrClusterDefinition.getServiceIamRole(),
            emrClusterDefinition.getEc2NodeIamProfileName());

        // Find best price and update definition
        emrPricingHelper.updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition);

        String clusterId = null; // The cluster ID record.
        String emrClusterStatus = null;
        Boolean emrClusterCreated = null; // Was cluster created?

        // If the dryRun flag is null or false. This is the default option if no flag is given.
        if (!Boolean.TRUE.equals(request.isDryRun()))
        {
            /*
             * Create the cluster only if the cluster does not already exist.
             * If the cluster is created, record the newly created cluster ID.
             * If the cluster already exists, record the existing cluster ID.
             * If there is any error while attempting to check for existing cluster or create a new one, handle the exception to throw appropriate exception.
             */
            String clusterName =
                emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterAlternateKeyDto.getEmrClusterName());
            try
            {
                ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(clusterName, awsParamsDto);
                // If cluster does not already exist.
                if (clusterSummary == null)
                {
                    clusterId = emrDao.createEmrCluster(clusterName, emrClusterDefinition, awsParamsDto);
                    emrClusterCreated = true;

                    EmrClusterCreationLogEntity emrClusterCreationLogEntity = new EmrClusterCreationLogEntity();
                    emrClusterCreationLogEntity.setNamespace(emrClusterDefinitionEntity.getNamespace());
                    emrClusterCreationLogEntity.setEmrClusterDefinitionName(emrClusterDefinitionEntity.getName());
                    emrClusterCreationLogEntity.setEmrClusterName(emrClusterAlternateKeyDto.getEmrClusterName());
                    emrClusterCreationLogEntity.setEmrClusterId(clusterId);
                    emrClusterCreationLogEntity.setEmrClusterDefinition(xmlHelper.objectToXml(emrClusterDefinition));
                    herdDao.saveAndRefresh(emrClusterCreationLogEntity);
                }
                // If the cluster already exist.
                else
                {
                    clusterId = clusterSummary.getId();
                    emrClusterCreated = false;
                    emrClusterDefinition = null; // Do not include definition in response
                }
                emrClusterStatus = emrDao.getEmrClusterStatusById(clusterId, awsParamsDto);
            }
            catch (AmazonServiceException ex)
            {
                handleAmazonException(ex, "An Amazon exception occurred while creating EMR cluster with name \"" + clusterName + "\".");
            }
        }
        // If the dryRun flag is true and not null
        else
        {
            emrClusterCreated = false;
        }

        return createEmrClusterFromRequest(clusterId, namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), emrClusterStatus, emrClusterCreated, request.isDryRun(), emrClusterDefinition);
    }

    /**
     * <p> Overrides the properties of {@code emrClusterDefinition} with the properties of {@code emrClusterDefinitionOverride}. </p> <p> If any property in
     * {@code emrClusterDefinitionOverride} is {@code null}, the property will remain unmodified. </p> <p> If any property in {@code
     * emrClusterDefinitionOverride} is not {@code null}, the property will be set. </p> <p> Any list or object type properties will be shallowly overridden.
     * That is, if a list is given in the override, the entire list will be set. Note that this is a shallow copy operation, so any modification to the override
     * list or object will affect the definition. </p> <p> This method does nothing if {@code emrClusterDefinitionOverride} is {@code null}. </p>
     *
     * @param emrClusterDefinition - definition to override
     * @param emrClusterDefinitionOverride - the override value or {@code null}
     */
    @SuppressWarnings("PMD.CyclomaticComplexity") // Method is not complex. It's just very repetitive.
    private void overrideEmrClusterDefinition(EmrClusterDefinition emrClusterDefinition, EmrClusterDefinition emrClusterDefinitionOverride)
    {
        if (emrClusterDefinitionOverride != null)
        {
            if (emrClusterDefinitionOverride.getReleaseLabel() != null)
            {
                emrClusterDefinition.setReleaseLabel(emrClusterDefinitionOverride.getReleaseLabel());
            }
            if (emrClusterDefinitionOverride.getApplications() != null)
            {
                emrClusterDefinition.setApplications(emrClusterDefinitionOverride.getApplications());
            }
            if (emrClusterDefinitionOverride.getConfigurations() != null)
            {
                emrClusterDefinition.setConfigurations(emrClusterDefinitionOverride.getConfigurations());
            }
            if (emrClusterDefinitionOverride.getSshKeyPairName() != null)
            {
                emrClusterDefinition.setSshKeyPairName(emrClusterDefinitionOverride.getSshKeyPairName());
            }
            if (emrClusterDefinitionOverride.getSubnetId() != null)
            {
                emrClusterDefinition.setSubnetId(emrClusterDefinitionOverride.getSubnetId());
            }
            if (emrClusterDefinitionOverride.getLogBucket() != null)
            {
                emrClusterDefinition.setLogBucket(emrClusterDefinitionOverride.getLogBucket());
            }
            if (emrClusterDefinitionOverride.isKeepAlive() != null)
            {
                emrClusterDefinition.setKeepAlive(emrClusterDefinitionOverride.isKeepAlive());
            }
            if (emrClusterDefinitionOverride.isVisibleToAll() != null)
            {
                emrClusterDefinition.setVisibleToAll(emrClusterDefinitionOverride.isVisibleToAll());
            }
            if (emrClusterDefinitionOverride.isTerminationProtection() != null)
            {
                emrClusterDefinition.setTerminationProtection(emrClusterDefinitionOverride.isTerminationProtection());
            }
            if (emrClusterDefinitionOverride.isEncryptionEnabled() != null)
            {
                emrClusterDefinition.setEncryptionEnabled(emrClusterDefinitionOverride.isEncryptionEnabled());
            }
            if (emrClusterDefinitionOverride.getAmiVersion() != null)
            {
                emrClusterDefinition.setAmiVersion(emrClusterDefinitionOverride.getAmiVersion());
            }
            if (emrClusterDefinitionOverride.getHadoopVersion() != null)
            {
                emrClusterDefinition.setHadoopVersion(emrClusterDefinitionOverride.getHadoopVersion());
            }
            if (emrClusterDefinitionOverride.getHiveVersion() != null)
            {
                emrClusterDefinition.setHiveVersion(emrClusterDefinitionOverride.getHiveVersion());
            }
            if (emrClusterDefinitionOverride.getPigVersion() != null)
            {
                emrClusterDefinition.setPigVersion(emrClusterDefinitionOverride.getPigVersion());
            }
            if (emrClusterDefinitionOverride.getEc2NodeIamProfileName() != null)
            {
                emrClusterDefinition.setEc2NodeIamProfileName(emrClusterDefinitionOverride.getEc2NodeIamProfileName());
            }
            if (emrClusterDefinitionOverride.isInstallOozie() != null)
            {
                emrClusterDefinition.setInstallOozie(emrClusterDefinitionOverride.isInstallOozie());
            }
            if (emrClusterDefinitionOverride.getCustomBootstrapActionMaster() != null)
            {
                emrClusterDefinition.setCustomBootstrapActionMaster(emrClusterDefinitionOverride.getCustomBootstrapActionMaster());
            }
            if (emrClusterDefinitionOverride.getCustomBootstrapActionAll() != null)
            {
                emrClusterDefinition.setCustomBootstrapActionAll(emrClusterDefinitionOverride.getCustomBootstrapActionAll());
            }
            if (emrClusterDefinitionOverride.getAdditionalInfo() != null)
            {
                emrClusterDefinition.setAdditionalInfo(emrClusterDefinitionOverride.getAdditionalInfo());
            }
            if (emrClusterDefinitionOverride.getInstanceDefinitions() != null)
            {
                emrClusterDefinition.setInstanceDefinitions(emrClusterDefinitionOverride.getInstanceDefinitions());
            }
            if (emrClusterDefinitionOverride.getNodeTags() != null)
            {
                emrClusterDefinition.setNodeTags(emrClusterDefinitionOverride.getNodeTags());
            }
            if (emrClusterDefinitionOverride.getDaemonConfigurations() != null)
            {
                emrClusterDefinition.setDaemonConfigurations(emrClusterDefinitionOverride.getDaemonConfigurations());
            }
            if (emrClusterDefinitionOverride.getHadoopConfigurations() != null)
            {
                emrClusterDefinition.setHadoopConfigurations(emrClusterDefinitionOverride.getHadoopConfigurations());
            }
            if (emrClusterDefinitionOverride.getServiceIamRole() != null)
            {
                emrClusterDefinition.setServiceIamRole(emrClusterDefinitionOverride.getServiceIamRole());
            }
            if (emrClusterDefinitionOverride.getSupportedProduct() != null)
            {
                emrClusterDefinition.setSupportedProduct(emrClusterDefinitionOverride.getSupportedProduct());
            }
            if (emrClusterDefinitionOverride.getHadoopJarSteps() != null)
            {
                emrClusterDefinition.setHadoopJarSteps(emrClusterDefinitionOverride.getHadoopJarSteps());
            }
            if (emrClusterDefinitionOverride.getAdditionalMasterSecurityGroups() != null)
            {
                emrClusterDefinition.setAdditionalMasterSecurityGroups(emrClusterDefinitionOverride.getAdditionalMasterSecurityGroups());
            }
            if (emrClusterDefinitionOverride.getAdditionalSlaveSecurityGroups() != null)
            {
                emrClusterDefinition.setAdditionalSlaveSecurityGroups(emrClusterDefinitionOverride.getAdditionalSlaveSecurityGroups());
            }
        }
    }

    /**
     * Terminates the EMR Cluster. Creates its own transaction.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param overrideTerminationProtection parameter for whether to override termination protection
     *
     * @return the terminated EMR cluster object
     * @throws Exception if there were any errors while terminating the cluster.
     */
    @NamespacePermission(fields = "#emrClusterAlternateKeyDto?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrCluster terminateCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId)
        throws Exception
    {
        return terminateClusterImpl(emrClusterAlternateKeyDto, overrideTerminationProtection, emrClusterId);
    }

    /**
     * Terminates the EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param overrideTerminationProtection parameter for whether to override termination protection
     * @param emrClusterId The EMR cluster ID
     *
     * @return the terminated EMR cluster object
     * @throws Exception if there were any errors while terminating the cluster.
     */
    protected EmrCluster terminateClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId)
        throws Exception
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDto();

        // Perform the request validation.
        emrHelper.validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(emrClusterAlternateKeyDto.getNamespace());

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper
            .getEmrClusterDefinitionEntity(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName());

        String clusterId = null;
        String clusterName =
            emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterAlternateKeyDto.getEmrClusterName());
        try
        {
            clusterId = emrHelper.getActiveEmrClusterId(emrClusterId, clusterName);
            emrDao.terminateEmrCluster(clusterId, overrideTerminationProtection, awsParamsDto);
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while terminating EMR cluster with name \"" + clusterName + "\".");
        }

        return createEmrClusterFromRequest(clusterId, namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), emrDao.getEmrClusterStatusById(clusterId, awsParamsDto), null, null, null);
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
        return EmrClusterAlternateKeyDto.builder().namespace(emrClusterCreateRequest.getNamespace())
            .emrClusterDefinitionName(emrClusterCreateRequest.getEmrClusterDefinitionName()).emrClusterName(emrClusterCreateRequest.getEmrClusterName())
            .build();
    }

    /**
     * Creates a new EMR cluster object from request.
     *
     * @param clusterId the cluster Id.
     * @param namespaceCd the namespace Code
     * @param clusterDefinitionName the cluster definition
     * @param clusterName the cluster name
     * @param clusterStatus the cluster status
     * @param emrClusterCreated whether EMR cluster was created.
     * @param dryRun The dry run flag.
     * @param emrClusterDefinition the EMR cluster definition.
     *
     * @return the created EMR cluster object
     */
    private EmrCluster createEmrClusterFromRequest(String clusterId, String namespaceCd, String clusterDefinitionName, String clusterName, String clusterStatus,
        Boolean emrClusterCreated, Boolean dryRun, EmrClusterDefinition emrClusterDefinition)
    {
        // Create the EMR cluster.
        EmrCluster emrCluster = new EmrCluster();
        emrCluster.setId(clusterId);
        emrCluster.setNamespace(namespaceCd);
        emrCluster.setEmrClusterDefinitionName(clusterDefinitionName);
        emrCluster.setEmrClusterName(clusterName);
        emrCluster.setStatus(clusterStatus);
        emrCluster.setDryRun(dryRun);
        emrCluster.setEmrClusterCreated(emrClusterCreated);
        emrCluster.setEmrClusterDefinition(emrClusterDefinition);
        return emrCluster;
    }

    /**
     * Adds step to an existing EMR Cluster. Creates its own transaction.
     * <p/>
     * There are five serializable objects supported currently. They are 1: ShellStep - For shell scripts 2: OozieStep - For Oozie workflow xml files 3:
     * HiveStep - For hive scripts 4: HadoopJarStep - For Custom Map Reduce Jar files and 5: PigStep - For Pig scripts.
     *
     * @param request the EMR steps add request
     *
     * @return the EMR steps add object with added steps
     * @throws Exception if there were any errors while adding a step to the cluster.
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
     * @throws Exception if there were any errors while adding a step to the cluster.
     */
    protected Object addStepToClusterImpl(Object request) throws Exception
    {
        EmrStepHelper stepHelper = emrStepHelperFactory.getStepHelper(request.getClass().getName());

        // Perform the request validation.
        validateAddStepToClusterRequest(request, stepHelper);

        // Perform the step specific validation
        stepHelper.validateAddStepRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(stepHelper.getRequestNamespace(request));

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper
            .getEmrClusterDefinitionEntity(stepHelper.getRequestNamespace(request), stepHelper.getRequestEmrClusterDefinitionName(request));

        // Update the namespace and cluster definition name in request from database.  
        stepHelper.setRequestNamespace(request, namespaceEntity.getCode());
        stepHelper.setRequestEmrClusterDefinitionName(request, emrClusterDefinitionEntity.getName());

        String clusterName =
            emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), stepHelper.getRequestEmrClusterName(request));
        Object emrStep = stepHelper.buildResponseFromRequest(request);

        try
        {
            String clusterId = emrHelper.getActiveEmrClusterId(stepHelper.getRequestEmrClusterId(request), clusterName);
            stepHelper.setRequestEmrClusterId(request, clusterId);
            String stepId = emrDao.addEmrStep(clusterId, stepHelper.getEmrStepConfig(emrStep), emrHelper.getAwsParamsDto());
            stepHelper.setStepId(emrStep, stepId);
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex,
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

    private String getS3ManagedReplaceString()
    {
        return configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION);
    }

    /**
     * Adds security groups to the master node of an existing EMR Cluster. Creates its own transaction.
     *
     * @param request the EMR master security group add request
     *
     * @return the added EMR master security groups
     * @throws Exception if there were any errors adding the security groups to the cluster master.
     */
    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrMasterSecurityGroup addSecurityGroupsToClusterMaster(EmrMasterSecurityGroupAddRequest request) throws Exception
    {
        return addSecurityGroupsToClusterMasterImpl(request);
    }

    /**
     * Adds security groups to the master node of an existing EMR Cluster.
     *
     * @param request the EMR master security group add request
     *
     * @return the added EMR master security groups
     * @throws Exception if there were any errors adding the security groups to the cluster master.
     */
    protected EmrMasterSecurityGroup addSecurityGroupsToClusterMasterImpl(EmrMasterSecurityGroupAddRequest request) throws Exception
    {
        // Perform the request validation.
        validateAddSecurityGroupsToClusterMasterRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespace());

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity =
            emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(request.getNamespace(), request.getEmrClusterDefinitionName());

        List<String> groupIds = null;
        String clusterName = emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), request.getEmrClusterName());
        try
        {
            groupIds = emrDao.addEmrMasterSecurityGroups(emrHelper.getActiveEmrClusterId(request.getEmrClusterId(), clusterName), request.getSecurityGroupIds(),
                emrHelper.getAwsParamsDto());
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while adding EMR security groups: " +
                herdStringHelper.buildStringWithDefaultDelimiter(request.getSecurityGroupIds()) +
                " to cluster: " + clusterName);
        }

        return createEmrClusterMasterGroupFromRequest(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), request.getEmrClusterName(), groupIds);
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
     * Runs oozie job on an existing EMR Cluster. Creates its own transaction.
     *
     * @param request the Run oozie workflow request
     *
     * @return the oozie workflow job that was submitted.
     * @throws Exception if there were any errors while submitting the job.
     */
    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.EXECUTE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OozieWorkflowJob runOozieWorkflow(RunOozieWorkflowRequest request) throws Exception
    {
        return runOozieWorkflowImpl(request);
    }

    /**
     * Runs oozie job on an existing EMR Cluster.
     *
     * @param request the Run oozie workflow request
     *
     * @return the oozie workflow job that was submitted.
     * @throws Exception if there were any errors while submitting the job.
     */
    protected OozieWorkflowJob runOozieWorkflowImpl(RunOozieWorkflowRequest request) throws Exception
    {
        // Perform the request validation.
        validateRunOozieWorkflowRequest(request);

        String namespace = request.getNamespace();
        String emrClusterDefinitionName = request.getEmrClusterDefinitionName();
        String emrClusterName = request.getEmrClusterName();
        String emrClusterId = request.getEmrClusterId();

        String clusterId = getRunningOrWaitingEmrCluster(namespace, emrClusterDefinitionName, emrClusterName, emrClusterId);

        String emrClusterPrivateIpAddress = getEmrClusterMasterIpAddress(clusterId);

        String jobId = oozieDao.runOozieWorkflow(emrClusterPrivateIpAddress, request.getWorkflowLocation(), request.getParameters());

        OozieWorkflowJob oozieWorkflowJob = new OozieWorkflowJob();
        oozieWorkflowJob.setId(jobId);
        oozieWorkflowJob.setNamespace(namespace);
        oozieWorkflowJob.setEmrClusterDefinitionName(emrClusterDefinitionName);
        oozieWorkflowJob.setEmrClusterName(emrClusterName);
        return oozieWorkflowJob;
    }

    /**
     * Get the EMR master private IP address.
     *
     * @param emrClusterId the cluster id
     *
     * @return the master node private IP address
     * @throws Exception Exception
     */
    private String getEmrClusterMasterIpAddress(String emrClusterId) throws Exception
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDto();
        return emrDao.getEmrMasterInstance(emrClusterId, awsParamsDto).getPrivateIpAddress();
    }

    /**
     * Get the cluster in RUNNING or WAITING status.
     *
     * @param namespace namespace
     * @param emrClusterDefinitionName emrClusterDefinitionName
     * @param emrClusterName emrClusterName
     * @param emrClusterId The EMR cluster ID
     *
     * @return The actual EMR cluster ID
     */
    private String getRunningOrWaitingEmrCluster(String namespace, String emrClusterDefinitionName, String emrClusterName, String emrClusterId)
    {
        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespace);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity =
            emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(namespace, emrClusterDefinitionName);

        String clusterName = emrHelper.buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterName);

        String actualEmrClusterId = null;
        String emrClusterState = null;
        if (StringUtils.isNotBlank(emrClusterId))
        {
            try
            {
                Cluster cluster = emrDao.getEmrClusterById(emrClusterId, emrHelper.getAwsParamsDto());
                if (cluster != null)
                {
                    actualEmrClusterId = cluster.getId();
                    emrClusterState = cluster.getStatus().getState();
                }
            }
            catch (AmazonServiceException amazonServiceException)
            {
                handleAmazonException(amazonServiceException, "Unable to get EMR cluster.");
            }
        }
        else
        {
            ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(clusterName, emrHelper.getAwsParamsDto());
            if (clusterSummary != null)
            {
                actualEmrClusterId = clusterSummary.getId();
                emrClusterState = clusterSummary.getStatus().getState();
            }
        }

        // We can only run oozie job when the cluster is up (RUNNING or WAITING). Can not submit job otherwise like bootstraping.
        // Make sure that cluster exists and is in RUNNING or WAITING state.
        if (actualEmrClusterId == null || !(emrClusterState.equalsIgnoreCase("RUNNING") || emrClusterState.equalsIgnoreCase("WAITING")))
        {
            throw new ObjectNotFoundException(String.format("Either the cluster \"%s\" does not exist or not in RUNNING or WAITING state.", clusterName));
        }

        return actualEmrClusterId;
    }

    /**
     * Get the oozie workflow. Starts a new transaction.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     * @param emrClusterName the EMR cluster name
     * @param oozieWorkflowJobId the ooxie workflow Id.
     * @param verbose the flag to indicate whether to return verbose information
     * @param emrClusterId The EMR cluster ID
     *
     * @return OozieWorkflowJob OozieWorkflowJob
     * @throws Exception Exception
     */
    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OozieWorkflowJob getEmrOozieWorkflowJob(String namespace, String emrClusterDefinitionName, String emrClusterName, String oozieWorkflowJobId,
        Boolean verbose, String emrClusterId) throws Exception
    {
        return getEmrOozieWorkflowJobImpl(namespace, emrClusterDefinitionName, emrClusterName, oozieWorkflowJobId, verbose, emrClusterId);
    }

    /**
     * Get the oozie workflow.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     * @param emrClusterName the EMR cluster name
     * @param oozieWorkflowJobId the ooxie workflow Id.
     * @param verbose the flag to indicate whether to return verbose information
     * @param emrClusterId The EMR cluster ID
     *
     * @return OozieWorkflowJob OozieWorkflowJob
     * @throws Exception Exception
     */
    protected OozieWorkflowJob getEmrOozieWorkflowJobImpl(String namespace, String emrClusterDefinitionName, String emrClusterName, String oozieWorkflowJobId,
        Boolean verbose, String emrClusterId) throws Exception
    {
        // Validate parameters
        Assert.isTrue(StringUtils.isNotBlank(namespace), "Namespace is required");
        Assert.isTrue(StringUtils.isNotBlank(emrClusterDefinitionName), "EMR cluster definition name is required");
        Assert.isTrue(StringUtils.isNotBlank(emrClusterName), "EMR cluster name is required");
        Assert.isTrue(StringUtils.isNotBlank(oozieWorkflowJobId), "Oozie workflow job ID is required");

        // Trim string parameters
        String namespaceTrimmed = namespace.trim();
        String emrClusterDefinitionNameTrimmed = emrClusterDefinitionName.trim();
        String emrClusterNameTrimmed = emrClusterName.trim();
        String oozieWorkflowJobIdTrimmed = oozieWorkflowJobId.trim();

        // Retrieve cluster's master instance IP
        String clusterId = getRunningOrWaitingEmrCluster(namespaceTrimmed, emrClusterDefinitionNameTrimmed, emrClusterNameTrimmed, emrClusterId);
        String masterIpAddress = getEmrClusterMasterIpAddress(clusterId);

        // Retrieve the wrapper oozie workflow. This workflow is the workflow that herd wraps the client's workflow to help copy client workflow definition from
        // S3 to HDFS.
        WorkflowJob wrapperWorkflowJob = oozieDao.getEmrOozieWorkflow(masterIpAddress, oozieWorkflowJobIdTrimmed);

        // Check to make sure that the workflow job is a herd wrapper workflow.
        Assert.isTrue(wrapperWorkflowJob.getAppName().equals(OozieDaoImpl.HERD_OOZIE_WRAPPER_WORKFLOW_NAME),
            "The oozie workflow with job ID '" + oozieWorkflowJobIdTrimmed +
                "' is not created by herd. Please ensure that the workflow was created through herd.");

        // Retrieve the client workflow's job action by navigating through the actions of the wrapper workflow. The client's workflow job ID is represented as
        // the action's external ID.
        WorkflowAction clientWorkflowAction = emrHelper.getClientWorkflowAction(wrapperWorkflowJob);

        WorkflowJob clientWorkflowJob = null;
        String clientWorkflowStatus = null;
        boolean hasClientWorkflowInfo = false;
        WorkflowAction errorWrapperWorkflowAction = null;
        /*
         * If the client workflow action is not found,  there are three possibilities:
         * 1. DM_PREP: The client workflow has not yet been run.
         * 2. DM_FAILED : herd wrapper workflow failed to run successfully.
         * 3. If client workflow action is found but does not have the external ID, means that it failed to kick off the client workflow. Possible causes:
         *    3.1 workflow.xml is not present in the location provided.
         *    3.2 workflow.xml is not a valid workflow.
         */
        if (clientWorkflowAction == null || clientWorkflowAction.getExternalId() == null)
        {
            // If wrapper workflow is FAILED/KILLED, means wrapper failed without running client workflow
            // else DM_PREP
            if (wrapperWorkflowJob.getStatus().equals(WorkflowJob.Status.KILLED) || wrapperWorkflowJob.getStatus().equals(WorkflowJob.Status.FAILED))
            {
                clientWorkflowStatus = OozieDaoImpl.OOZIE_WORKFLOW_JOB_STATUS_DM_FAILED;
                // Get error information.
                errorWrapperWorkflowAction = emrHelper.getFirstWorkflowActionInError(wrapperWorkflowJob);
            }
            else
            {
                clientWorkflowStatus = OozieDaoImpl.OOZIE_WORKFLOW_JOB_STATUS_DM_PREP;
            }
        }
        else
        {
            // Retrieve the client workflow
            clientWorkflowJob = oozieDao.getEmrOozieWorkflow(masterIpAddress, clientWorkflowAction.getExternalId());
            hasClientWorkflowInfo = true;
        }

        // Construct result
        OozieWorkflowJob resultOozieWorkflowJob = new OozieWorkflowJob();
        resultOozieWorkflowJob.setId(oozieWorkflowJobId);
        resultOozieWorkflowJob.setNamespace(namespace);
        resultOozieWorkflowJob.setEmrClusterDefinitionName(emrClusterDefinitionName);
        resultOozieWorkflowJob.setEmrClusterName(emrClusterName);

        // If client workflow is information is not available that herd wrapper workflow has not started the client workflow yet or failed to start.
        // Hence return status that it is still in preparation.
        if (!hasClientWorkflowInfo)
        {
            resultOozieWorkflowJob.setStatus(clientWorkflowStatus);
            if (errorWrapperWorkflowAction != null)
            {
                resultOozieWorkflowJob.setErrorCode(errorWrapperWorkflowAction.getErrorCode());
                resultOozieWorkflowJob.setErrorMessage(errorWrapperWorkflowAction.getErrorMessage());
            }
        }
        else
        {
            resultOozieWorkflowJob.setStartTime(toXmlGregorianCalendar(clientWorkflowJob.getStartTime()));
            resultOozieWorkflowJob.setEndTime(toXmlGregorianCalendar(clientWorkflowJob.getEndTime()));
            resultOozieWorkflowJob.setStatus(clientWorkflowJob.getStatus().toString());

            // Construct actions in the result if verbose flag is explicitly true, default to false
            if (Boolean.TRUE.equals(verbose))
            {
                List<OozieWorkflowAction> oozieWorkflowActions = new ArrayList<>();
                for (WorkflowAction workflowAction : clientWorkflowJob.getActions())
                {
                    OozieWorkflowAction resultOozieWorkflowAction = new OozieWorkflowAction();
                    resultOozieWorkflowAction.setId(workflowAction.getId());
                    resultOozieWorkflowAction.setName(workflowAction.getName());
                    resultOozieWorkflowAction.setStartTime(toXmlGregorianCalendar(workflowAction.getStartTime()));
                    resultOozieWorkflowAction.setEndTime(toXmlGregorianCalendar(workflowAction.getEndTime()));
                    resultOozieWorkflowAction.setStatus(workflowAction.getStatus().toString());
                    resultOozieWorkflowAction.setErrorCode(workflowAction.getErrorCode());
                    resultOozieWorkflowAction.setErrorMessage(workflowAction.getErrorMessage());
                    oozieWorkflowActions.add(resultOozieWorkflowAction);
                }
                resultOozieWorkflowJob.setWorkflowActions(oozieWorkflowActions);
            }
        }

        return resultOozieWorkflowJob;
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
     * Validates the run oozie workflow request.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateRunOozieWorkflowRequest(RunOozieWorkflowRequest request)
    {
        // Validate required elements
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        Assert.hasText(request.getEmrClusterDefinitionName(), "An EMR cluster definition name must be specified.");
        Assert.hasText(request.getEmrClusterName(), "An EMR cluster name must be specified.");
        Assert.hasText(request.getWorkflowLocation(), "An oozie workflow location must be specified.");

        // Validate that parameter names are there and not duplicate
        parameterHelper.validateParameters(request.getParameters());

        // Remove leading and trailing spaces.
        request.setNamespace(request.getNamespace().trim());
        request.setEmrClusterDefinitionName(request.getEmrClusterDefinitionName().trim());
        request.setEmrClusterName(request.getEmrClusterName().trim());
    }

    /**
     * Handles the AmazonServiceException, throws corresponding exception based on status code in amazon exception.
     */
    private void handleAmazonException(AmazonServiceException ex, String message) throws IllegalArgumentException, ObjectNotFoundException
    {
        if (ex.getStatusCode() == HttpStatus.SC_BAD_REQUEST)
        {
            throw new IllegalArgumentException(message + " Reason: " + ex.getMessage(), ex);
        }
        else if (ex.getStatusCode() == HttpStatus.SC_NOT_FOUND)
        {
            throw new ObjectNotFoundException(message + " Reason: " + ex.getMessage(), ex);
        }
        throw ex;
    }
}