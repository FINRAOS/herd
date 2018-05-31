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
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.EmrPricingHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.ObjectNotFoundException;
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
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.service.EmrService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceIamRoleAuthorizationHelper;

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
    private XmlHelper xmlHelper;

    @Autowired
    private JsonHelper jsonHelper;

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
     * @throws Exception if an error occurred while getting the cluster
     */
    protected EmrCluster getClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose,
        String accountId, Boolean retrieveInstanceFleets) throws Exception
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(accountId);

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
                Cluster cluster = emrDao.getEmrClusterById(emrClusterId.trim(), awsParamsDto);

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
                ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(clusterName, awsParamsDto);

                // Validate that, Cluster exists with the name
                Assert.notNull(clusterSummary, "An EMR cluster must exists with the name \"" + clusterName + "\".");

                emrCluster.setId(clusterSummary.getId());
                setEmrClusterStatus(emrCluster, clusterSummary.getStatus());
            }

            // Get active step details
            if (emrHelper.isActiveEmrState(emrCluster.getStatus()))
            {
                StepSummary stepSummary = emrDao.getClusterActiveStep(emrCluster.getId(), awsParamsDto);
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
                Step step = emrDao.getClusterStep(emrCluster.getId(), emrStepId.trim(), awsParamsDto);

                emrCluster.setStep(buildEmrStepFromAwsStep(step, verbose));
            }

            // Get instance fleet if true
            if (BooleanUtils.isTrue(retrieveInstanceFleets))
            {
                ListInstanceFleetsResult listInstanceFleetsResult = emrDao.getListInstanceFleetsResult(emrCluster.getId(), awsParamsDto);
                emrCluster.setInstanceFleets(emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));
            }
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while getting EMR cluster details with name \"" + clusterName + "\".");
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
        // Extract EMR cluster alternate key from the create request.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getEmrClusterAlternateKey(request);

        // Perform the request validation.
        validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName()));

        // Replace all S3 managed location variables in xml
        String toReplace = getS3ManagedReplaceString();
        String replacedConfigXml = emrClusterDefinitionEntity.getConfiguration().replaceAll(toReplace, emrHelper.getS3StagingLocation());

        // Unmarshal definition xml into JAXB object.
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, replacedConfigXml);

        // Perform override if override is set.
        overrideEmrClusterDefinition(emrClusterDefinition, request.getEmrClusterDefinitionOverride());

        // Perform the EMR cluster definition configuration validation.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);

        // Check permissions.
        namespaceIamRoleAuthorizationHelper.checkPermissions(emrClusterDefinitionEntity.getNamespace(), emrClusterDefinition.getServiceIamRole(),
            emrClusterDefinition.getEc2NodeIamProfileName());

        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(emrClusterDefinition.getAccountId());

        // If instance group definitions are specified, find best price and update definition.
        if (!emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions()))
        {
            emrPricingHelper.updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        }

        String clusterId = null; // The cluster ID record.
        String emrClusterStatus = null;
        String accountId = emrClusterDefinition.getAccountId();
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
            String clusterName = emrHelper.buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
                emrClusterAlternateKeyDto.getEmrClusterName());
            try
            {
                // Try to get an active EMR cluster by its name.
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

        return createEmrClusterFromRequest(clusterId, emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), accountId, emrClusterStatus, emrClusterCreated, request.isDryRun(), emrClusterDefinition);
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
            if (emrClusterDefinitionOverride.getInstanceFleets() != null)
            {
                emrClusterDefinition.setInstanceFleets(emrClusterDefinitionOverride.getInstanceFleets());
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
            if (emrClusterDefinitionOverride.getAccountId() != null)
            {
                emrClusterDefinition.setAccountId(emrClusterDefinitionOverride.getAccountId());
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
            if (emrClusterDefinitionOverride.getSecurityConfiguration() != null)
            {
                emrClusterDefinition.setSecurityConfiguration(emrClusterDefinitionOverride.getSecurityConfiguration());
            }
            if (emrClusterDefinitionOverride.getMasterSecurityGroup() != null)
            {
                emrClusterDefinition.setMasterSecurityGroup(emrClusterDefinitionOverride.getMasterSecurityGroup());
            }
            if (emrClusterDefinitionOverride.getSlaveSecurityGroup() != null)
            {
                emrClusterDefinition.setSlaveSecurityGroup(emrClusterDefinitionOverride.getSlaveSecurityGroup());
            }
            if (emrClusterDefinitionOverride.getScaleDownBehavior() != null)
            {
                emrClusterDefinition.setScaleDownBehavior(emrClusterDefinitionOverride.getScaleDownBehavior());
            }
            if (emrClusterDefinitionOverride.getKerberosAttributes() != null)
            {
                emrClusterDefinition.setKerberosAttributes(emrClusterDefinitionOverride.getKerberosAttributes());
            }
        }
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
     * @throws Exception if there were any errors while terminating the cluster
     */
    protected EmrCluster terminateClusterImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId,
        String accountId) throws Exception
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(accountId);

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
            emrDao.terminateEmrCluster(clusterId, overrideTerminationProtection, awsParamsDto);
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while terminating EMR cluster with name \"" + clusterName + "\".");
        }

        return createEmrClusterFromRequest(clusterId, emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            emrClusterAlternateKeyDto.getEmrClusterName(), accountId, emrDao.getEmrClusterStatusById(clusterId, awsParamsDto), null, null, null);
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
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(accountId);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(stepHelper.getRequestNamespace(request), stepHelper.getRequestEmrClusterDefinitionName(request)));

        // Update the namespace and cluster definition name in request from database.
        stepHelper.setRequestNamespace(request, emrClusterDefinitionEntity.getNamespace().getCode());
        stepHelper.setRequestEmrClusterDefinitionName(request, emrClusterDefinitionEntity.getName());

        String clusterName = emrHelper.buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            stepHelper.getRequestEmrClusterName(request));
        Object emrStep = stepHelper.buildResponseFromRequest(request);

        try
        {
            String clusterId =
                emrHelper.getActiveEmrClusterId(stepHelper.getRequestEmrClusterId(request), clusterName, stepHelper.getRequestAccountId(request));
            stepHelper.setRequestEmrClusterId(request, clusterId);
            String stepId = emrDao.addEmrStep(clusterId, stepHelper.getEmrStepConfig(emrStep), awsParamsDto);
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
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
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

        // Get account and AwsParamDto
        String accountId = request.getAccountId();
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(accountId);

        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper
            .getEmrClusterDefinitionEntity(new EmrClusterDefinitionKey(request.getNamespace(), request.getEmrClusterDefinitionName()));

        List<String> groupIds = null;
        String clusterName = emrHelper
            .buildEmrClusterName(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(), request.getEmrClusterName());
        try
        {
            groupIds = emrDao.addEmrMasterSecurityGroups(emrHelper.getActiveEmrClusterId(request.getEmrClusterId(), clusterName, request.getAccountId()),
                request.getSecurityGroupIds(), awsParamsDto);
        }
        catch (AmazonServiceException ex)
        {
            handleAmazonException(ex, "An Amazon exception occurred while adding EMR security groups: " +
                herdStringHelper.buildStringWithDefaultDelimiter(request.getSecurityGroupIds()) + " to cluster: " + clusterName);
        }

        return createEmrClusterMasterGroupFromRequest(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName(),
            request.getEmrClusterName(), groupIds);
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
