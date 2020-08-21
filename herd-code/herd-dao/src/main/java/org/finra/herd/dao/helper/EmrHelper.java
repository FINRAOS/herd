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
package org.finra.herd.dao.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDevice;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetProvisioningSpecifications;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetStatus;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetTimeline;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeSpecification;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.model.api.xml.EmrClusterEbsBlockDevice;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetProvisioningSpecifications;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetStateChangeReason;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetStatus;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetTimeline;
import org.finra.herd.model.api.xml.EmrClusterInstanceTypeConfiguration;
import org.finra.herd.model.api.xml.EmrClusterInstanceTypeSpecification;
import org.finra.herd.model.api.xml.EmrClusterSpotProvisioningSpecification;
import org.finra.herd.model.api.xml.EmrClusterVolumeSpecification;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrParamsDto;
import org.finra.herd.model.jpa.TrustingAccountEntity;

/**
 * A helper class that provides EMR functions.
 */
@Component
public class EmrHelper extends AwsHelper
{
    @Autowired
    private EmrDao emrDao;

    @Autowired
    private StsDao stsDao;

    @Autowired
    private TrustingAccountDaoHelper trustingAccountDaoHelper;

    /**
     * Returns EMR cluster name constructed according to the template defined.
     *
     * @param namespaceCd the namespace code value.
     * @param emrDefinitionName the EMR definition name value.
     * @param clusterName the cluster name value.
     *
     * @return the cluster name.
     */
    public String buildEmrClusterName(String namespaceCd, String emrDefinitionName, String clusterName)
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        String namespaceToken = tokenDelimiter + "namespace" + tokenDelimiter;
        String emrDefinitionToken = tokenDelimiter + "emrDefinitionName" + tokenDelimiter;
        String clusterNameToken = tokenDelimiter + "clusterName" + tokenDelimiter;

        // Populate a map with the tokens mapped to actual database values.
        Map<String, String> pathToTokenValueMap = new HashMap<>();
        pathToTokenValueMap.put(namespaceToken, namespaceCd);
        pathToTokenValueMap.put(emrDefinitionToken, emrDefinitionName);
        pathToTokenValueMap.put(clusterNameToken, clusterName);

        // Set the default EMR cluster name tokenized template.
        // ~namespace~.~emrDefinitionName~.clusterName
        String defaultClusterNameTemplate = namespaceToken + "." + emrDefinitionToken + "." + clusterNameToken;

        // Get the EMR cluster name template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String emrClusterName = configurationHelper.getProperty(ConfigurationValue.EMR_CLUSTER_NAME_TEMPLATE);

        if (emrClusterName == null)
        {
            emrClusterName = defaultClusterNameTemplate;
        }

        // Substitute the tokens with the actual database values.
        for (Map.Entry<String, String> mapEntry : pathToTokenValueMap.entrySet())
        {
            emrClusterName = emrClusterName.replaceAll(mapEntry.getKey(), mapEntry.getValue());
        }

        return emrClusterName;
    }

    /**
     * Gets the ID of an active EMR cluster which matches the given criteria. If both cluster ID and cluster name is specified, the name of the actual cluster
     * with the given ID must match the specified name. For cases where the cluster is not found (does not exists or not active), the method fails. All
     * parameters are case-insensitive and whitespace trimmed. Blank parameters are equal to null.
     *
     * @param emrClusterId EMR cluster ID
     * @param emrClusterName EMR cluster name
     * @param accountId the account Id that EMR cluster is running under
     *
     * @return The cluster ID
     */
    public String getActiveEmrClusterId(String emrClusterId, String emrClusterName, String accountId)
    {
        boolean emrClusterIdSpecified = StringUtils.isNotBlank(emrClusterId);
        boolean emrClusterNameSpecified = StringUtils.isNotBlank(emrClusterName);

        Assert.isTrue(emrClusterIdSpecified || emrClusterNameSpecified, "One of EMR cluster ID or EMR cluster name must be specified.");
        EmrParamsDto emrParamsDto = getEmrParamsDtoByAccountId(accountId);

        // Get cluster by ID first
        if (emrClusterIdSpecified)
        {
            String emrClusterIdTrimmed = emrClusterId.trim();

            // Assert cluster exists
            Cluster cluster = emrDao.getEmrClusterById(emrClusterIdTrimmed, emrParamsDto);
            Assert.notNull(cluster, String.format("The cluster with ID \"%s\" does not exist.", emrClusterIdTrimmed));

            // Assert the cluster's state is active
            String emrClusterState = cluster.getStatus().getState();
            Assert.isTrue(isActiveEmrState(emrClusterState), String
                .format("The cluster with ID \"%s\" is not active. The cluster state must be in one of %s. Current state is \"%s\"", emrClusterIdTrimmed,
                    Arrays.toString(getActiveEmrClusterStates()), emrClusterState));

            // Assert cluster name equals if cluster name was specified
            if (emrClusterNameSpecified)
            {
                String emrClusterNameTrimmed = emrClusterName.trim();
                Assert.isTrue(cluster.getName().equalsIgnoreCase(emrClusterNameTrimmed), String
                    .format("The cluster with ID \"%s\" does not match the expected name \"%s\". The actual name is \"%s\".", cluster.getId(),
                        emrClusterNameTrimmed, cluster.getName()));
            }

            return cluster.getId();
        }
        else
        {
            String emrClusterNameTrimmed = emrClusterName.trim();
            ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByNameAndAccountId(emrClusterNameTrimmed, accountId, emrParamsDto);
            Assert.notNull(clusterSummary, String.format("The cluster with name \"%s\" does not exist.", emrClusterNameTrimmed));
            return clusterSummary.getId();
        }
    }

    /**
     * Get the EMR parameters Params DTO for the account Id if no account id is specified, use the default
     *
     * @param accountId account Id
     *
     * @return AwsParamsDto
     */
    public EmrParamsDto getEmrParamsDtoByAccountId(String accountId)
    {
        AwsParamsDto awsParamsDto = getAwsParamsDto();

        EmrParamsDto emrParamsDto =
            new EmrParamsDto(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken(), awsParamsDto.getHttpProxyHost(),
                awsParamsDto.getHttpProxyPort(), awsParamsDto.getAwsRegionName(), null);

        if (StringUtils.isNotBlank(accountId))
        {
            updateEmrParamsForCrossAccountAccess(emrParamsDto, accountId.trim());
        }

        return emrParamsDto;
    }

    public EmrDao getEmrDao()
    {
        return emrDao;
    }

    public void setEmrDao(EmrDao emrDao)
    {
        this.emrDao = emrDao;
    }

    /**
     * Builds the StepConfig for the Hadoop jar step.
     *
     * @param stepName the step name.
     * @param jarLocation the location of jar.
     * @param mainClass the main class.
     * @param scriptArguments the arguments.
     * @param isContinueOnError indicate what to do on error.
     *
     * @return the stepConfig.
     */
    public StepConfig getEmrHadoopJarStepConfig(String stepName, String jarLocation, String mainClass, List<String> scriptArguments, Boolean isContinueOnError)
    {
        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;

        if (isContinueOnError != null && isContinueOnError)
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // If there are no arguments
        if (CollectionUtils.isEmpty(scriptArguments))
        {
            // Build the StepConfig object and return
            return new StepConfig().withName(stepName.trim()).withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(new HadoopJarStepConfig().withJar(jarLocation.trim()).withMainClass(mainClass));
        }
        else
        {
            // If there are arguments, include the arguments in the StepConfig object
            return new StepConfig().withName(stepName.trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(
                new HadoopJarStepConfig().withJar(jarLocation.trim()).withMainClass(mainClass)
                    .withArgs(scriptArguments.toArray(new String[scriptArguments.size()])));
        }
    }

    /**
     * Get the S3_STAGING_RESOURCE full path from the bucket name as well as other details.
     *
     * @param trustingAccountStagingBucketName the optional S3 staging bucket name to be used in the trusting account, maybe null or empty
     *
     * @return the s3 managed location.
     */
    public String getS3StagingLocation(String trustingAccountStagingBucketName)
    {
        // If we have S3 staging bucket name specified to be used in the trusting account, then use it.
        // Otherwise, use primary S3 staging bucket configured in the system.
        String s3StagingBucketName = StringUtils.isNotBlank(trustingAccountStagingBucketName) ? trustingAccountStagingBucketName :
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_BUCKET_NAME);

        // Build and return the full path for the staging location that consists of protocol, bucket name, delimiter, and resource base prefix.
        return configurationHelper.getProperty(ConfigurationValue.S3_URL_PROTOCOL) + s3StagingBucketName +
            configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER) +
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE);
    }

    /**
     * Returns {@code true} if the supplied EMR status is considered to be active.
     *
     * @param status the EMR status
     *
     * @return whether the given EMR status is active
     */
    public boolean isActiveEmrState(String status)
    {
        return Arrays.asList(getActiveEmrClusterStates()).contains(status);
    }

    /**
     * Returns {@code true} if the supplied InstanceDefinitions is {@code null} or empty (contains no elements).
     *
     * @param instanceDefinitions the instance group definitions from the EMR cluster definition
     *
     * @return whether the given InstanceDefinitions is empty
     */
    public boolean isInstanceDefinitionsEmpty(InstanceDefinitions instanceDefinitions)
    {
        return (instanceDefinitions == null || (instanceDefinitions.getMasterInstances() == null && instanceDefinitions.getCoreInstances() == null &&
            instanceDefinitions.getTaskInstances() == null));
    }

    private String[] getActiveEmrClusterStates()
    {
        String emrStatesString = configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES);
        return emrStatesString.split("\\" + configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER));
    }

    /*
     * Updates the EMR parameters DTO with the temporary credentials for the cross-account access along with other trusting account specific configuration.
     *
     * @param emrParamsDto the EMR parameter DTO
     * @param accountId the AWS account number
     */
    private void updateEmrParamsForCrossAccountAccess(EmrParamsDto emrParamsDto, String accountId)
    {
        // Retrieve the role ARN and make sure it exists.
        TrustingAccountEntity trustingAccountEntity = trustingAccountDaoHelper.getTrustingAccountEntity(accountId.trim());
        String roleArn = trustingAccountEntity.getRoleArn();

        // Assume the role. Set the duration of the role session to 3600 seconds (1 hour).
        Credentials credentials = stsDao.getTemporarySecurityCredentials(emrParamsDto, UUID.randomUUID().toString(), roleArn, 3600, null);

        // Update the AWS parameters DTO with the temporary credentials.
        emrParamsDto.setAwsAccessKeyId(credentials.getAccessKeyId());
        emrParamsDto.setAwsSecretKey(credentials.getSecretAccessKey());
        emrParamsDto.setSessionToken(credentials.getSessionToken());

        // Update the AWS parameters with an optional S3 staging bucket name to be used in the trusting account.
        emrParamsDto.setTrustingAccountStagingBucketName(trustingAccountEntity.getStagingBucketName());
    }

    /**
     * Returns  EmrClusterInstanceFleet list from AWS call
     *
     * @param awsInstanceFleetsResult AWS Instance Fleets result
     *
     * @return list of  EmrClusterInstanceFleet
     */
    public List<EmrClusterInstanceFleet> buildEmrClusterInstanceFleetFromAwsResult(ListInstanceFleetsResult awsInstanceFleetsResult)
    {
        List<EmrClusterInstanceFleet> emrInstanceFleets = null;

        if (awsInstanceFleetsResult != null && !CollectionUtils.isEmpty(awsInstanceFleetsResult.getInstanceFleets()))
        {
            emrInstanceFleets = new ArrayList<>();

            for (InstanceFleet awsInstanceFleet : awsInstanceFleetsResult.getInstanceFleets())
            {
                if (awsInstanceFleet != null)
                {
                    EmrClusterInstanceFleet emrInstanceFleet = new EmrClusterInstanceFleet();
                    emrInstanceFleet.setId(awsInstanceFleet.getId());
                    emrInstanceFleet.setName(awsInstanceFleet.getName());
                    emrInstanceFleet.setInstanceFleetType(awsInstanceFleet.getInstanceFleetType());
                    emrInstanceFleet.setTargetOnDemandCapacity(awsInstanceFleet.getTargetOnDemandCapacity());
                    emrInstanceFleet.setTargetSpotCapacity(awsInstanceFleet.getTargetSpotCapacity());
                    emrInstanceFleet.setProvisionedOnDemandCapacity(awsInstanceFleet.getProvisionedOnDemandCapacity());
                    emrInstanceFleet.setProvisionedSpotCapacity(awsInstanceFleet.getProvisionedSpotCapacity());
                    emrInstanceFleet.setInstanceTypeSpecifications(getInstanceTypeSpecifications(awsInstanceFleet.getInstanceTypeSpecifications()));
                    emrInstanceFleet.setLaunchSpecifications(getLaunchSpecifications(awsInstanceFleet.getLaunchSpecifications()));
                    emrInstanceFleet.setInstanceFleetStatus(getEmrClusterInstanceFleetStatus(awsInstanceFleet.getStatus()));
                    emrInstanceFleets.add(emrInstanceFleet);
                }
            }
        }

        return emrInstanceFleets;
    }

    /**
     * Returns EmrClusterInstanceFleetStatus
     *
     * @param instanceFleetStatus AWS object
     *
     * @return EmrClusterInstanceFleetStatus
     */
    protected EmrClusterInstanceFleetStatus getEmrClusterInstanceFleetStatus(InstanceFleetStatus instanceFleetStatus)
    {
        EmrClusterInstanceFleetStatus emrClusterInstanceFleetStatus = null;
        if (instanceFleetStatus != null)
        {
            emrClusterInstanceFleetStatus = new EmrClusterInstanceFleetStatus();
            emrClusterInstanceFleetStatus.setState(instanceFleetStatus.getState());

            if (instanceFleetStatus.getStateChangeReason() != null)
            {
                EmrClusterInstanceFleetStateChangeReason emrClusterInstanceFleetStateChangeReason = new EmrClusterInstanceFleetStateChangeReason();
                emrClusterInstanceFleetStateChangeReason.setCode(instanceFleetStatus.getStateChangeReason().getCode());
                emrClusterInstanceFleetStateChangeReason.setMessage(instanceFleetStatus.getStateChangeReason().getMessage());
                emrClusterInstanceFleetStatus.setStateChangeReason(emrClusterInstanceFleetStateChangeReason);
            }
            if (instanceFleetStatus.getTimeline() != null)
            {
                InstanceFleetTimeline instanceFleetTimeline = instanceFleetStatus.getTimeline();
                EmrClusterInstanceFleetTimeline emrClusterInstanceFleetTimeline = new EmrClusterInstanceFleetTimeline();
                emrClusterInstanceFleetTimeline.setCreationDateTime(HerdDateUtils.getXMLGregorianCalendarValue(instanceFleetTimeline.getCreationDateTime()));
                emrClusterInstanceFleetTimeline.setEndDateTime(HerdDateUtils.getXMLGregorianCalendarValue(instanceFleetTimeline.getEndDateTime()));
                emrClusterInstanceFleetTimeline.setReadyDateTime(HerdDateUtils.getXMLGregorianCalendarValue(instanceFleetTimeline.getReadyDateTime()));
                emrClusterInstanceFleetStatus.setTimeline(emrClusterInstanceFleetTimeline);
            }
        }
        return emrClusterInstanceFleetStatus;
    }

    /**
     * Returns  EmrClusterInstanceFleetProvisioningSpecifications
     *
     * @param instanceFleetProvisioningSpecifications AWS object
     *
     * @return EmrClusterInstanceFleetProvisioningSpecifications
     */
    protected EmrClusterInstanceFleetProvisioningSpecifications getLaunchSpecifications(
        InstanceFleetProvisioningSpecifications instanceFleetProvisioningSpecifications)
    {
        EmrClusterInstanceFleetProvisioningSpecifications emrClusterDefinitionLaunchSpecifications = null;

        if (instanceFleetProvisioningSpecifications != null)
        {
            emrClusterDefinitionLaunchSpecifications = new EmrClusterInstanceFleetProvisioningSpecifications();
            emrClusterDefinitionLaunchSpecifications.setSpotSpecification(getSpotSpecification(instanceFleetProvisioningSpecifications.getSpotSpecification()));
        }

        return emrClusterDefinitionLaunchSpecifications;
    }

    /**
     * Returns EmrClusterSpotProvisioningSpecification from AWS call
     *
     * @param spotProvisioningSpecification AWS object
     *
     * @return EmrClusterSpotProvisioningSpecification
     */
    protected EmrClusterSpotProvisioningSpecification getSpotSpecification(SpotProvisioningSpecification spotProvisioningSpecification)
    {
        EmrClusterSpotProvisioningSpecification emrClusterSpotProvisioningSpecification = null;

        if (spotProvisioningSpecification != null)
        {
            emrClusterSpotProvisioningSpecification = new EmrClusterSpotProvisioningSpecification();
            emrClusterSpotProvisioningSpecification.setTimeoutDurationMinutes(spotProvisioningSpecification.getTimeoutDurationMinutes());
            emrClusterSpotProvisioningSpecification.setTimeoutAction(spotProvisioningSpecification.getTimeoutAction());
            emrClusterSpotProvisioningSpecification.setBlockDurationMinutes(spotProvisioningSpecification.getBlockDurationMinutes());
        }

        return emrClusterSpotProvisioningSpecification;
    }

    /**
     * Returns list of EmrClusterEbsBlockDevice
     *
     * @param ebsBlockDevices AWS object
     *
     * @return list of EmrClusterEbsBlockDevice
     */
    protected List<EmrClusterEbsBlockDevice> getEbsBlockDevices(List<EbsBlockDevice> ebsBlockDevices)
    {
        List<EmrClusterEbsBlockDevice> emrClusterEbsBlockDevices = null;

        if (!CollectionUtils.isEmpty(ebsBlockDevices))
        {
            emrClusterEbsBlockDevices = new ArrayList<>();

            for (EbsBlockDevice ebsBlockDevice : ebsBlockDevices)
            {
                if (ebsBlockDevice != null)
                {
                    EmrClusterEbsBlockDevice emrClusterEbsBlockDevice = new EmrClusterEbsBlockDevice();
                    emrClusterEbsBlockDevice.setDevice(ebsBlockDevice.getDevice());
                    emrClusterEbsBlockDevice.setVolumeSpecification(getVolumeSpecification(ebsBlockDevice.getVolumeSpecification()));

                    emrClusterEbsBlockDevices.add(emrClusterEbsBlockDevice);
                }
            }
        }

        return emrClusterEbsBlockDevices;
    }

    /**
     * Returns EmrClusterVolumeSpecification
     *
     * @param volumeSpecification AWS object
     *
     * @return EmrClusterVolumeSpecification
     */
    protected EmrClusterVolumeSpecification getVolumeSpecification(VolumeSpecification volumeSpecification)
    {
        EmrClusterVolumeSpecification emrClusterVolumeSpecification = null;

        if (volumeSpecification != null)
        {
            emrClusterVolumeSpecification = new EmrClusterVolumeSpecification();
            emrClusterVolumeSpecification.setVolumeType(volumeSpecification.getVolumeType());
            emrClusterVolumeSpecification.setIops(volumeSpecification.getIops());
            emrClusterVolumeSpecification.setSizeInGB(volumeSpecification.getSizeInGB());
        }

        return emrClusterVolumeSpecification;
    }

    /**
     * Returns list of  EmrClusterInstanceTypeSpecification
     *
     * @param awsInstanceTypeConfigs AWS object
     *
     * @return list of  EmrClusterInstanceTypeSpecification
     */
    protected List<EmrClusterInstanceTypeSpecification> getInstanceTypeSpecifications(List<InstanceTypeSpecification> awsInstanceTypeConfigs)
    {
        List<EmrClusterInstanceTypeSpecification> emrClusterInstanceTypeSpecifications = null;

        if (!CollectionUtils.isEmpty(awsInstanceTypeConfigs))
        {
            emrClusterInstanceTypeSpecifications = new ArrayList<>();

            for (InstanceTypeSpecification awsInstanceTypeConfig : awsInstanceTypeConfigs)
            {
                if (awsInstanceTypeConfig != null)
                {
                    EmrClusterInstanceTypeSpecification emrClusterInstanceTypeSpecification = new EmrClusterInstanceTypeSpecification();
                    emrClusterInstanceTypeSpecification.setInstanceType(awsInstanceTypeConfig.getInstanceType());
                    emrClusterInstanceTypeSpecification.setWeightedCapacity(awsInstanceTypeConfig.getWeightedCapacity());
                    emrClusterInstanceTypeSpecification.setBidPrice(awsInstanceTypeConfig.getBidPrice());
                    emrClusterInstanceTypeSpecification.setBidPriceAsPercentageOfOnDemandPrice(awsInstanceTypeConfig.getBidPriceAsPercentageOfOnDemandPrice());
                    emrClusterInstanceTypeSpecification.setEbsBlockDevices(getEbsBlockDevices(awsInstanceTypeConfig.getEbsBlockDevices()));
                    emrClusterInstanceTypeSpecification.setEbsOptimized(awsInstanceTypeConfig.getEbsOptimized());
                    emrClusterInstanceTypeSpecification.setConfigurations(getConfigurations(awsInstanceTypeConfig.getConfigurations()));

                    emrClusterInstanceTypeSpecifications.add(emrClusterInstanceTypeSpecification);
                }
            }
        }

        return emrClusterInstanceTypeSpecifications;
    }

    /**
     * Returns list of EmrClusterInstanceTypeConfiguration
     *
     * @param configurations AWS configuration object list
     *
     * @return list of EmrClusterInstanceTypeConfiguration
     */
    protected List<EmrClusterInstanceTypeConfiguration> getConfigurations(List<Configuration> configurations)
    {
        List<EmrClusterInstanceTypeConfiguration> emrClusterInstanceTypeConfigurations = null;

        if (!CollectionUtils.isEmpty(configurations))
        {
            emrClusterInstanceTypeConfigurations = new ArrayList<>();

            for (Configuration configuration : configurations)
            {
                if (configuration != null)
                {
                    EmrClusterInstanceTypeConfiguration emrClusterInstanceTypeConfiguration = new EmrClusterInstanceTypeConfiguration();
                    emrClusterInstanceTypeConfiguration.setClassification(configuration.getClassification());
                    emrClusterInstanceTypeConfiguration.setConfigurations(getConfigurations(configuration.getConfigurations()));
                    emrClusterInstanceTypeConfiguration.setProperties(getParameterList(configuration.getProperties()));

                    emrClusterInstanceTypeConfigurations.add(emrClusterInstanceTypeConfiguration);
                }
            }
        }

        return emrClusterInstanceTypeConfigurations;
    }

    /**
     * Returns parameter list
     *
     * @param properties properties
     *
     * @return list of parameters
     */
    protected List<Parameter> getParameterList(Map<String, String> properties)
    {
        List<Parameter> parameters = null;
        if (!CollectionUtils.isEmpty(properties))
        {
            parameters = new ArrayList<>();

            for (Map.Entry<String, String> entry : properties.entrySet())
            {
                Parameter parameter = new Parameter(entry.getKey(), entry.getValue());
                parameters.add(parameter);
            }
        }
        return parameters;
    }

}
