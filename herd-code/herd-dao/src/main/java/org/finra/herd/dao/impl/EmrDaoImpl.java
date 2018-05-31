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
package org.finra.herd.dao.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetProvisioningSpecifications;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupType;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.KerberosAttributes;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepState;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.Ec2Dao;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.ConfigurationFile;
import org.finra.herd.model.api.xml.ConfigurationFiles;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsBlockDeviceConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceTypeConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKerberosAttributes;
import org.finra.herd.model.api.xml.EmrClusterDefinitionLaunchSpecifications;
import org.finra.herd.model.api.xml.EmrClusterDefinitionSpotSpecification;
import org.finra.herd.model.api.xml.EmrClusterDefinitionVolumeSpecification;
import org.finra.herd.model.api.xml.HadoopJarStep;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.KeyValuePairConfiguration;
import org.finra.herd.model.api.xml.KeyValuePairConfigurations;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.ScriptDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * The EMR DAO implementation.
 */
@Repository
public class EmrDaoImpl implements EmrDao
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrDaoImpl.class);

    @Autowired
    private AwsClientFactory awsClientFactory;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private Ec2Dao ec2Dao;

    @Autowired
    private EmrHelper emrHelper;

    @Autowired
    private EmrOperations emrOperations;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Override
    public List<String> addEmrMasterSecurityGroups(String clusterId, List<String> securityGroups, AwsParamsDto awsParams) throws Exception
    {
        // Get the master EC2 instance
        ListInstancesRequest listInstancesRequest = new ListInstancesRequest().withClusterId(clusterId).withInstanceGroupTypes(InstanceGroupType.MASTER);

        List<Instance> instances = emrOperations.listClusterInstancesRequest(getEmrClient(awsParams), listInstancesRequest).getInstances();

        // Throw error in case there are no master instances found yet
        if (instances.size() == 0)
        {
            throw new IllegalArgumentException("No master instances found for the cluster \"" + clusterId + "\".");
        }

        for (Instance instance : instances)
        {
            ec2Dao.addSecurityGroupsToEc2Instance(instance.getEc2InstanceId(), securityGroups, awsParams);
        }

        return securityGroups;
    }

    @Override
    public String addEmrStep(String clusterId, StepConfig emrStepConfig, AwsParamsDto awsParamsDto) throws Exception
    {
        List<StepConfig> steps = new ArrayList<>();

        steps.add(emrStepConfig);

        // Add the job flow request
        AddJobFlowStepsRequest jobFlowStepRequest = new AddJobFlowStepsRequest(clusterId, steps);
        List<String> emrStepIds = emrOperations.addJobFlowStepsRequest(getEmrClient(awsParamsDto), jobFlowStepRequest);

        return emrStepIds.get(0);
    }

    @Override
    public String createEmrCluster(String clusterName, EmrClusterDefinition emrClusterDefinition, AwsParamsDto awsParams)
    {
        RunJobFlowRequest runJobFlowRequest = getRunJobFlowRequest(clusterName, emrClusterDefinition);
        LOGGER.info("runJobFlowRequest={}", jsonHelper.objectToJson(runJobFlowRequest));
        String clusterId = emrOperations.runEmrJobFlow(getEmrClient(awsParams), runJobFlowRequest);
        LOGGER.info("EMR cluster started. emrClusterId=\"{}\"", clusterId);
        return clusterId;
    }

    @Override
    public ClusterSummary getActiveEmrClusterByName(String clusterName, AwsParamsDto awsParams)
    {
        if (StringUtils.isNotBlank(clusterName))
        {
            /**
             * Call AWSOperations for ListClusters API. Need to list all the active clusters that are in
             * BOOTSTRAPPING/RUNNING/STARTING/WAITING states
             */
            ListClustersRequest listClustersRequest = new ListClustersRequest().withClusterStates(getActiveEmrClusterStates());

            /**
             * ListClusterRequest returns only 50 clusters at a time. However, this returns a marker
             * that can be used for subsequent calls to listClusters to get all the clusters
             */
            String markerForListClusters = listClustersRequest.getMarker();

            // Loop through all the available clusters and look for the given cluster id
            do
            {
                /**
                 * Call AWSOperations for ListClusters API.
                 * Need to include the Marker returned by the previous iteration
                 */
                ListClustersResult clusterResult =
                    emrOperations.listEmrClusters(getEmrClient(awsParams), listClustersRequest.withMarker(markerForListClusters));

                // Loop through all the active clusters returned by AWS
                for (ClusterSummary clusterInstance : clusterResult.getClusters())
                {
                    // If the cluster name matches, then return the status
                    if (StringUtils.isNotBlank(clusterInstance.getName()) && clusterInstance.getName().equalsIgnoreCase(clusterName))
                    {
                        return clusterInstance;
                    }
                }
                markerForListClusters = clusterResult.getMarker();
            }
            while (markerForListClusters != null);
        }

        return null;
    }

    /**
     * Converts the given list of {@link EmrClusterDefinitionApplication} into a list of {@link Application}
     *
     * @param emrClusterDefinitionApplications list of {@link EmrClusterDefinitionApplication}
     *
     * @return list {@link Application}
     */
    public List<Application> getApplications(List<EmrClusterDefinitionApplication> emrClusterDefinitionApplications)
    {
        List<Application> applications = new ArrayList<>();
        for (EmrClusterDefinitionApplication emrClusterDefinitionApplication : emrClusterDefinitionApplications)
        {
            Application application = new Application();
            application.setName(emrClusterDefinitionApplication.getName());
            application.setVersion(emrClusterDefinitionApplication.getVersion());
            application.setArgs(emrClusterDefinitionApplication.getArgs());

            List<Parameter> additionalInfoList = emrClusterDefinitionApplication.getAdditionalInfoList();
            if (!CollectionUtils.isEmpty(additionalInfoList))
            {
                application.setAdditionalInfo(getMap(additionalInfoList));
            }

            applications.add(application);
        }
        return applications;
    }

    @Override
    public StepSummary getClusterActiveStep(String clusterId, AwsParamsDto awsParamsDto)
    {
        ListStepsRequest listStepsRequest = new ListStepsRequest().withClusterId(clusterId).withStepStates(StepState.RUNNING);
        List<StepSummary> stepSummaryList = emrOperations.listStepsRequest(getEmrClient(awsParamsDto), listStepsRequest).getSteps();

        return !stepSummaryList.isEmpty() ? stepSummaryList.get(0) : null;
    }

    @Override
    public Step getClusterStep(String clusterId, String stepId, AwsParamsDto awsParamsDto)
    {
        DescribeStepRequest describeStepRequest = new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId);
        return emrOperations.describeStepRequest(getEmrClient(awsParamsDto), describeStepRequest).getStep();
    }

    @Override
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto)
    {
        return awsClientFactory.getEmrClient(awsParamsDto);
    }

    @Override
    public Cluster getEmrClusterById(String clusterId, AwsParamsDto awsParams)
    {
        Cluster cluster = null;
        if (StringUtils.isNotBlank(clusterId))
        {
            DescribeClusterResult describeClusterResult =
                emrOperations.describeClusterRequest(getEmrClient(awsParams), new DescribeClusterRequest().withClusterId(clusterId));
            if (describeClusterResult != null && describeClusterResult.getCluster() != null)
            {
                cluster = describeClusterResult.getCluster();
            }
        }

        return cluster;
    }

    @Override
    public String getEmrClusterStatusById(String clusterId, AwsParamsDto awsParams)
    {
        Cluster cluster = getEmrClusterById(clusterId, awsParams);

        return ((cluster == null) ? null : cluster.getStatus().getState());
    }

    @Override
    public Instance getEmrMasterInstance(String clusterId, AwsParamsDto awsParams) throws Exception
    {
        // Get the master EC2 instance
        ListInstancesRequest listInstancesRequest = new ListInstancesRequest().withClusterId(clusterId).withInstanceGroupTypes(InstanceGroupType.MASTER);

        List<Instance> instances = emrOperations.listClusterInstancesRequest(getEmrClient(awsParams), listInstancesRequest).getInstances();

        // Throw error in case there are no master instances found yet
        if (instances.size() == 0)
        {
            throw new IllegalArgumentException("No master instances found for the cluster \"" + clusterId + "\".");
        }

        // EMR has only one master node.
        return instances.get(0);
    }

    @Override
    public ListInstanceFleetsResult getListInstanceFleetsResult(String clusterId, AwsParamsDto awsParams)
    {
        return emrOperations.listInstanceFleets(getEmrClient(awsParams), new ListInstanceFleetsRequest().withClusterId(clusterId));
    }

    @Override
    public void terminateEmrCluster(String clusterId, boolean overrideTerminationProtection, AwsParamsDto awsParams)
    {
        emrOperations.terminateEmrCluster(getEmrClient(awsParams), clusterId, overrideTerminationProtection);
    }

    /**
     * Converts the given list of {@link EmrClusterDefinitionConfiguration} into a list of {@link Configuration}.
     *
     * @param emrClusterDefinitionConfigurations list of {@link EmrClusterDefinitionConfiguration}
     *
     * @return list of {@link Configuration}
     */
    protected List<Configuration> getConfigurations(List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations)
    {
        List<Configuration> configurations = null;

        if (!CollectionUtils.isEmpty(emrClusterDefinitionConfigurations))
        {
            configurations = new ArrayList<>();

            for (EmrClusterDefinitionConfiguration emrClusterDefinitionConfiguration : emrClusterDefinitionConfigurations)
            {
                if (emrClusterDefinitionConfiguration != null)
                {
                    Configuration configuration = new Configuration();
                    configuration.setClassification(emrClusterDefinitionConfiguration.getClassification());
                    configuration.setConfigurations(getConfigurations(emrClusterDefinitionConfiguration.getConfigurations()));
                    configuration.setProperties(getMap(emrClusterDefinitionConfiguration.getProperties()));

                    configurations.add(configuration);
                }
            }
        }

        return configurations;
    }

    /**
     * Creates a list of {@link EbsBlockDeviceConfig} from a given list of {@link EmrClusterDefinitionEbsBlockDeviceConfig}.
     *
     * @param emrClusterDefinitionEbsBlockDeviceConfigs the list of {@link EmrClusterDefinitionEbsBlockDeviceConfig}
     *
     * @return the list of {@link EbsBlockDeviceConfig}
     */
    protected List<EbsBlockDeviceConfig> getEbsBlockDeviceConfigs(List<EmrClusterDefinitionEbsBlockDeviceConfig> emrClusterDefinitionEbsBlockDeviceConfigs)
    {
        List<EbsBlockDeviceConfig> ebsBlockDeviceConfigs = null;

        if (!CollectionUtils.isEmpty(emrClusterDefinitionEbsBlockDeviceConfigs))
        {
            ebsBlockDeviceConfigs = new ArrayList<>();

            for (EmrClusterDefinitionEbsBlockDeviceConfig emrClusterDefinitionEbsBlockDeviceConfig : emrClusterDefinitionEbsBlockDeviceConfigs)
            {
                if (emrClusterDefinitionEbsBlockDeviceConfig != null)
                {
                    EbsBlockDeviceConfig ebsBlockDeviceConfig = new EbsBlockDeviceConfig();
                    ebsBlockDeviceConfig.setVolumeSpecification(getVolumeSpecification(emrClusterDefinitionEbsBlockDeviceConfig.getVolumeSpecification()));
                    ebsBlockDeviceConfig.setVolumesPerInstance(emrClusterDefinitionEbsBlockDeviceConfig.getVolumesPerInstance());

                    ebsBlockDeviceConfigs.add(ebsBlockDeviceConfig);
                }
            }
        }

        return ebsBlockDeviceConfigs;
    }

    /**
     * Creates an instance of {@link EbsConfiguration} from a given instance of {@link EmrClusterDefinitionEbsConfiguration}.
     *
     * @param emrClusterDefinitionEbsConfiguration the instance of {@link EmrClusterDefinitionEbsConfiguration}
     *
     * @return the instance of {@link EbsConfiguration}
     */
    protected EbsConfiguration getEbsConfiguration(EmrClusterDefinitionEbsConfiguration emrClusterDefinitionEbsConfiguration)
    {
        EbsConfiguration ebsConfiguration = null;

        if (emrClusterDefinitionEbsConfiguration != null)
        {
            ebsConfiguration = new EbsConfiguration();
            ebsConfiguration.setEbsBlockDeviceConfigs(getEbsBlockDeviceConfigs(emrClusterDefinitionEbsConfiguration.getEbsBlockDeviceConfigs()));
            ebsConfiguration.setEbsOptimized(emrClusterDefinitionEbsConfiguration.isEbsOptimized());
        }

        return ebsConfiguration;
    }

    /**
     * Creates an instance fleet configuration that describes the EC2 instances and instance configurations for clusters that use this feature.
     *
     * @param emrClusterDefinitionInstanceFleets the list of instance fleet configurations from the EMR cluster definition
     *
     * @return the instance fleet configuration
     */
    protected List<InstanceFleetConfig> getInstanceFleets(List<EmrClusterDefinitionInstanceFleet> emrClusterDefinitionInstanceFleets)
    {
        List<InstanceFleetConfig> instanceFleets = null;

        if (!CollectionUtils.isEmpty(emrClusterDefinitionInstanceFleets))
        {
            instanceFleets = new ArrayList<>();

            for (EmrClusterDefinitionInstanceFleet emrClusterDefinitionInstanceFleet : emrClusterDefinitionInstanceFleets)
            {
                if (emrClusterDefinitionInstanceFleet != null)
                {
                    InstanceFleetConfig instanceFleetConfig = new InstanceFleetConfig();
                    instanceFleetConfig.setName(emrClusterDefinitionInstanceFleet.getName());
                    instanceFleetConfig.setInstanceFleetType(emrClusterDefinitionInstanceFleet.getInstanceFleetType());
                    instanceFleetConfig.setTargetOnDemandCapacity(emrClusterDefinitionInstanceFleet.getTargetOnDemandCapacity());
                    instanceFleetConfig.setTargetSpotCapacity(emrClusterDefinitionInstanceFleet.getTargetSpotCapacity());
                    instanceFleetConfig.setInstanceTypeConfigs(getInstanceTypeConfigs(emrClusterDefinitionInstanceFleet.getInstanceTypeConfigs()));
                    instanceFleetConfig.setLaunchSpecifications(getLaunchSpecifications(emrClusterDefinitionInstanceFleet.getLaunchSpecifications()));

                    instanceFleets.add(instanceFleetConfig);
                }
            }
        }

        return instanceFleets;
    }

    /**
     * Creates an instance group configuration.
     *
     * @param roleType role type for the instance group (MASTER/CORE/TASK)
     * @param instanceType EC2 instance type for the instance group
     * @param instanceCount number of instances for the instance group
     * @param bidPrice bid price in case of SPOT instance request
     * @param emrClusterDefinitionEbsConfiguration the instance of {@link EmrClusterDefinitionEbsConfiguration} that contains EBS configurations that will be
     * attached to each EC2 instance in this instance group
     *
     * @return the instance group config object
     */
    protected InstanceGroupConfig getInstanceGroupConfig(InstanceRoleType roleType, String instanceType, Integer instanceCount, BigDecimal bidPrice,
        EmrClusterDefinitionEbsConfiguration emrClusterDefinitionEbsConfiguration)
    {
        // Create an instance group configuration with an optional EBS configuration.
        InstanceGroupConfig instanceGroup =
            new InstanceGroupConfig(roleType, instanceType, instanceCount).withEbsConfiguration(getEbsConfiguration(emrClusterDefinitionEbsConfiguration));

        // Consider spot price, if specified.
        if (bidPrice != null)
        {
            instanceGroup.setMarket(MarketType.SPOT);
            instanceGroup.setBidPrice(bidPrice.toString());
        }

        return instanceGroup;
    }

    /**
     * Create the instance group configuration for MASTER/CORE/TASK nodes as per the input parameters.
     *
     * @param instanceDefinitions the instance group definitions from the EMR cluster definition
     *
     * @return the instance group config list with all the instance group definitions
     */
    protected List<InstanceGroupConfig> getInstanceGroupConfigs(InstanceDefinitions instanceDefinitions)
    {
        List<InstanceGroupConfig> instanceGroupConfigs = null;

        if (!emrHelper.isInstanceDefinitionsEmpty(instanceDefinitions))
        {
            // Create the instance group configurations.
            instanceGroupConfigs = new ArrayList<>();

            // Fill-in the MASTER node details.
            instanceGroupConfigs.add(getInstanceGroupConfig(InstanceRoleType.MASTER, instanceDefinitions.getMasterInstances().getInstanceType(),
                instanceDefinitions.getMasterInstances().getInstanceCount(), instanceDefinitions.getMasterInstances().getInstanceSpotPrice(),
                instanceDefinitions.getMasterInstances().getEbsConfiguration()));

            // if the optional core instances are specified, fill-in the CORE node details.
            if (instanceDefinitions.getCoreInstances() != null)
            {
                instanceGroupConfigs.add(getInstanceGroupConfig(InstanceRoleType.CORE, instanceDefinitions.getCoreInstances().getInstanceType(),
                    instanceDefinitions.getCoreInstances().getInstanceCount(), instanceDefinitions.getCoreInstances().getInstanceSpotPrice(),
                    instanceDefinitions.getCoreInstances().getEbsConfiguration()));
            }

            // If the optional task instances are specified, fill-in the TASK node details.
            if (instanceDefinitions.getTaskInstances() != null)
            {
                instanceGroupConfigs.add(getInstanceGroupConfig(InstanceRoleType.TASK, instanceDefinitions.getTaskInstances().getInstanceType(),
                    instanceDefinitions.getTaskInstances().getInstanceCount(), instanceDefinitions.getTaskInstances().getInstanceSpotPrice(),
                    instanceDefinitions.getTaskInstances().getEbsConfiguration()));
            }
        }

        return instanceGroupConfigs;
    }

    /**
     * Creates a list of {@link InstanceTypeConfig} from a given list of {@link EmrClusterDefinitionInstanceTypeConfig}.
     *
     * @param emrClusterDefinitionInstanceTypeConfigs the list of {@link EmrClusterDefinitionInstanceTypeConfig}
     *
     * @return the list of {@link InstanceTypeConfig}
     */
    protected List<InstanceTypeConfig> getInstanceTypeConfigs(List<EmrClusterDefinitionInstanceTypeConfig> emrClusterDefinitionInstanceTypeConfigs)
    {
        List<InstanceTypeConfig> instanceTypeConfigs = null;

        if (!CollectionUtils.isEmpty(emrClusterDefinitionInstanceTypeConfigs))
        {
            instanceTypeConfigs = new ArrayList<>();

            for (EmrClusterDefinitionInstanceTypeConfig emrClusterDefinitionInstanceTypeConfig : emrClusterDefinitionInstanceTypeConfigs)
            {
                if (emrClusterDefinitionInstanceTypeConfig != null)
                {
                    InstanceTypeConfig instanceTypeConfig = new InstanceTypeConfig();
                    instanceTypeConfig.setInstanceType(emrClusterDefinitionInstanceTypeConfig.getInstanceType());
                    instanceTypeConfig.setWeightedCapacity(emrClusterDefinitionInstanceTypeConfig.getWeightedCapacity());
                    instanceTypeConfig.setBidPrice(emrClusterDefinitionInstanceTypeConfig.getBidPrice());
                    instanceTypeConfig.setBidPriceAsPercentageOfOnDemandPrice(emrClusterDefinitionInstanceTypeConfig.getBidPriceAsPercentageOfOnDemandPrice());
                    instanceTypeConfig.setEbsConfiguration(getEbsConfiguration(emrClusterDefinitionInstanceTypeConfig.getEbsConfiguration()));
                    instanceTypeConfig.setConfigurations(getConfigurations(emrClusterDefinitionInstanceTypeConfig.getConfigurations()));

                    instanceTypeConfigs.add(instanceTypeConfig);
                }
            }
        }

        return instanceTypeConfigs;
    }

    /**
     * Creates an instance of {@link KerberosAttributes} from a given instance of {@link EmrClusterDefinitionKerberosAttributes}.
     *
     * @param emrClusterDefinitionKerberosAttributes the instance of {@link EmrClusterDefinitionKerberosAttributes}, may be null
     *
     * @return the instance of {@link KerberosAttributes}
     */
    protected KerberosAttributes getKerberosAttributes(EmrClusterDefinitionKerberosAttributes emrClusterDefinitionKerberosAttributes)
    {
        KerberosAttributes kerberosAttributes = null;

        if (emrClusterDefinitionKerberosAttributes != null)
        {
            kerberosAttributes = new KerberosAttributes();
            kerberosAttributes.setADDomainJoinPassword(emrClusterDefinitionKerberosAttributes.getADDomainJoinPassword());
            kerberosAttributes.setADDomainJoinUser(emrClusterDefinitionKerberosAttributes.getADDomainJoinUser());
            kerberosAttributes.setCrossRealmTrustPrincipalPassword(emrClusterDefinitionKerberosAttributes.getCrossRealmTrustPrincipalPassword());
            kerberosAttributes.setKdcAdminPassword(emrClusterDefinitionKerberosAttributes.getKdcAdminPassword());
            kerberosAttributes.setRealm(emrClusterDefinitionKerberosAttributes.getRealm());
        }

        return kerberosAttributes;
    }

    /**
     * Creates an instance of {@link InstanceFleetProvisioningSpecifications} from a given instance of {@link EmrClusterDefinitionLaunchSpecifications}.
     *
     * @param emrClusterDefinitionLaunchSpecifications the instance of {@link EmrClusterDefinitionLaunchSpecifications}
     *
     * @return the instance of {@link InstanceFleetProvisioningSpecifications}
     */
    protected InstanceFleetProvisioningSpecifications getLaunchSpecifications(EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications)
    {
        InstanceFleetProvisioningSpecifications instanceFleetProvisioningSpecifications = null;

        if (emrClusterDefinitionLaunchSpecifications != null)
        {
            instanceFleetProvisioningSpecifications = new InstanceFleetProvisioningSpecifications();
            instanceFleetProvisioningSpecifications.setSpotSpecification(getSpotSpecification(emrClusterDefinitionLaunchSpecifications.getSpotSpecification()));
        }

        return instanceFleetProvisioningSpecifications;
    }

    /**
     * Converts the given list of {@link Parameter} into a {@link Map} of {@link String}, {@link String}
     *
     * @param parameters List of {@link Parameter}
     *
     * @return {@link Map}
     */
    protected Map<String, String> getMap(List<Parameter> parameters)
    {
        Map<String, String> map = null;

        if (!CollectionUtils.isEmpty(parameters))
        {
            map = new HashMap<>();

            for (Parameter parameter : parameters)
            {
                if (parameter != null)
                {
                    map.put(parameter.getName(), parameter.getValue());
                }
            }
        }

        return map;
    }

    /**
     * Creates an instance of {@link SpotProvisioningSpecification} from a given instance of {@link EmrClusterDefinitionSpotSpecification}.
     *
     * @param emrClusterDefinitionSpotSpecification the instance of {@link EmrClusterDefinitionSpotSpecification}
     *
     * @return the instance of {@link SpotProvisioningSpecification}
     */
    protected SpotProvisioningSpecification getSpotSpecification(EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification)
    {
        SpotProvisioningSpecification spotProvisioningSpecification = null;

        if (emrClusterDefinitionSpotSpecification != null)
        {
            spotProvisioningSpecification = new SpotProvisioningSpecification();
            spotProvisioningSpecification.setTimeoutDurationMinutes(emrClusterDefinitionSpotSpecification.getTimeoutDurationMinutes());
            spotProvisioningSpecification.setTimeoutAction(emrClusterDefinitionSpotSpecification.getTimeoutAction());
            spotProvisioningSpecification.setBlockDurationMinutes(emrClusterDefinitionSpotSpecification.getBlockDurationMinutes());
        }

        return spotProvisioningSpecification;
    }

    /**
     * Creates an instance of {@link VolumeSpecification} from a given instance of {@link EmrClusterDefinitionVolumeSpecification}.
     *
     * @param emrClusterDefinitionVolumeSpecification the instance of {@link EmrClusterDefinitionVolumeSpecification}
     *
     * @return the instance of {@link VolumeSpecification}
     */
    protected VolumeSpecification getVolumeSpecification(EmrClusterDefinitionVolumeSpecification emrClusterDefinitionVolumeSpecification)
    {
        VolumeSpecification volumeSpecification = null;

        if (emrClusterDefinitionVolumeSpecification != null)
        {
            volumeSpecification = new VolumeSpecification();
            volumeSpecification.setVolumeType(emrClusterDefinitionVolumeSpecification.getVolumeType());
            volumeSpecification.setIops(emrClusterDefinitionVolumeSpecification.getIops());
            volumeSpecification.setSizeInGB(emrClusterDefinitionVolumeSpecification.getSizeInGB());
        }

        return volumeSpecification;
    }

    private void addCustomBootstrapActionConfig(EmrClusterDefinition emrClusterDefinition, ArrayList<BootstrapActionConfig> bootstrapActions)
    {
        // Add Custom bootstrap script support if needed
        if (!CollectionUtils.isEmpty(emrClusterDefinition.getCustomBootstrapActionAll()))
        {
            for (ScriptDefinition scriptDefinition : emrClusterDefinition.getCustomBootstrapActionAll())
            {
                BootstrapActionConfig customActionConfigAll = getBootstrapActionConfig(scriptDefinition.getScriptName(), scriptDefinition.getScriptLocation());

                ArrayList<String> argList = new ArrayList<>();
                if (!CollectionUtils.isEmpty(scriptDefinition.getScriptArguments()))
                {
                    for (String argument : scriptDefinition.getScriptArguments())
                    {
                        // Trim the argument
                        argList.add(argument.trim());
                    }
                }
                // Set arguments to bootstrap action
                customActionConfigAll.getScriptBootstrapAction().setArgs(argList);

                bootstrapActions.add(customActionConfigAll);
            }
        }
    }

    private void addCustomMasterBootstrapActionConfig(EmrClusterDefinition emrClusterDefinition, ArrayList<BootstrapActionConfig> bootstrapActions)
    {
        // Add Master custom bootstrap script support if needed
        if (!CollectionUtils.isEmpty(emrClusterDefinition.getCustomBootstrapActionMaster()))
        {
            for (ScriptDefinition scriptDefinition : emrClusterDefinition.getCustomBootstrapActionMaster())
            {
                BootstrapActionConfig bootstrapActionConfig =
                    getBootstrapActionConfig(scriptDefinition.getScriptName(), configurationHelper.getProperty(ConfigurationValue.EMR_CONDITIONAL_SCRIPT));

                // Add arguments to the bootstrap script
                ArrayList<String> argList = new ArrayList<>();

                // Execute this script only on the master node.
                argList.add(configurationHelper.getProperty(ConfigurationValue.EMR_NODE_CONDITION));
                argList.add(scriptDefinition.getScriptLocation());

                if (!CollectionUtils.isEmpty(scriptDefinition.getScriptArguments()))
                {
                    for (String argument : scriptDefinition.getScriptArguments())
                    {
                        // Trim the argument
                        argList.add(argument.trim());
                    }
                }

                bootstrapActionConfig.getScriptBootstrapAction().setArgs(argList);
                bootstrapActions.add(bootstrapActionConfig);
            }
        }
    }

    private void addDaemonBootstrapActionConfig(EmrClusterDefinition emrClusterDefinition, ArrayList<BootstrapActionConfig> bootstrapActions)
    {
        // Add daemon Configuration support if needed
        if (!CollectionUtils.isEmpty(emrClusterDefinition.getDaemonConfigurations()))
        {
            BootstrapActionConfig daemonBootstrapActionConfig = getBootstrapActionConfig(ConfigurationValue.EMR_CONFIGURE_DAEMON.getKey(),
                configurationHelper.getProperty(ConfigurationValue.EMR_CONFIGURE_DAEMON));

            // Add arguments to the bootstrap script
            ArrayList<String> argList = new ArrayList<>();
            for (Parameter daemonConfig : emrClusterDefinition.getDaemonConfigurations())
            {
                argList.add(daemonConfig.getName() + "=" + daemonConfig.getValue());
            }

            // Add the bootstrap action with arguments
            daemonBootstrapActionConfig.getScriptBootstrapAction().setArgs(argList);
            bootstrapActions.add(daemonBootstrapActionConfig);
        }
    }

    private void addHadoopBootstrapActionConfig(EmrClusterDefinition emrClusterDefinition, ArrayList<BootstrapActionConfig> bootstrapActions)
    {
        // Add hadoop Configuration support if needed
        if (!CollectionUtils.isEmpty(emrClusterDefinition.getHadoopConfigurations()))
        {
            ArrayList<String> argList = new ArrayList<>();
            BootstrapActionConfig hadoopBootstrapActionConfig = getBootstrapActionConfig(ConfigurationValue.EMR_CONFIGURE_HADOOP.getKey(),
                configurationHelper.getProperty(ConfigurationValue.EMR_CONFIGURE_HADOOP));
            // If config files are available, add them as arguments
            for (Object hadoopConfigObject : emrClusterDefinition.getHadoopConfigurations())
            {
                // If the Config Files are available, add them as arguments
                if (hadoopConfigObject instanceof ConfigurationFiles)
                {
                    for (ConfigurationFile configurationFile : ((ConfigurationFiles) hadoopConfigObject).getConfigurationFiles())
                    {
                        argList.add(configurationFile.getFileNameShortcut());
                        argList.add(configurationFile.getConfigFileLocation());
                    }
                }

                // If the key value pairs are available, add them as arguments
                if (hadoopConfigObject instanceof KeyValuePairConfigurations)
                {
                    for (KeyValuePairConfiguration keyValuePairConfiguration : ((KeyValuePairConfigurations) hadoopConfigObject)
                        .getKeyValuePairConfigurations())
                    {
                        argList.add(keyValuePairConfiguration.getKeyValueShortcut());
                        argList.add(keyValuePairConfiguration.getAttribKey() + "=" + keyValuePairConfiguration.getAttribVal());
                    }
                }
            }

            // Add the bootstrap action with arguments
            hadoopBootstrapActionConfig.getScriptBootstrapAction().setArgs(argList);
            bootstrapActions.add(hadoopBootstrapActionConfig);
        }
    }

    private String[] getActiveEmrClusterStates()
    {
        String emrStatesString = configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES);
        return emrStatesString.split("\\" + configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER));
    }

    /**
     * Create the BootstrapActionConfig object from the bootstrap script.
     *
     * @param scriptDescription bootstrap script name to be displayed.
     * @param bootstrapScript location of the bootstrap script.
     *
     * @return bootstrap action configuration that contains all the bootstrap actions for the given configuration.
     */
    private BootstrapActionConfig getBootstrapActionConfig(String scriptDescription, String bootstrapScript)
    {
        // Create the BootstrapActionConfig object
        BootstrapActionConfig bootstrapConfig = new BootstrapActionConfig();
        ScriptBootstrapActionConfig bootstrapConfigScript = new ScriptBootstrapActionConfig();

        // Set the bootstrapScript
        bootstrapConfig.setName(scriptDescription);
        bootstrapConfigScript.setPath(bootstrapScript);
        bootstrapConfig.setScriptBootstrapAction(bootstrapConfigScript);

        // Return the object
        return bootstrapConfig;
    }

    /**
     * Create the bootstrap action configuration List from all the bootstrapping scripts specified.
     *
     * @param emrClusterDefinition the EMR definition name value.
     *
     * @return list of bootstrap action configurations that contains all the bootstrap actions for the given configuration.
     */
    private ArrayList<BootstrapActionConfig> getBootstrapActionConfigList(EmrClusterDefinition emrClusterDefinition)
    {
        // Create the list
        ArrayList<BootstrapActionConfig> bootstrapActions = new ArrayList<>();

        // Add encryption script support if needed
        if (emrClusterDefinition.isEncryptionEnabled() != null && emrClusterDefinition.isEncryptionEnabled())
        {
            bootstrapActions.add(getBootstrapActionConfig(ConfigurationValue.EMR_ENCRYPTION_SCRIPT.getKey(), getEncryptionScriptLocation()));
        }

        // Add bootstrap actions.
        addDaemonBootstrapActionConfig(emrClusterDefinition, bootstrapActions);
        addHadoopBootstrapActionConfig(emrClusterDefinition, bootstrapActions);
        addCustomBootstrapActionConfig(emrClusterDefinition, bootstrapActions);
        addCustomMasterBootstrapActionConfig(emrClusterDefinition, bootstrapActions);

        // Return the object
        return bootstrapActions;
    }

    /**
     * Create the tag list for the EMR nodes.
     *
     * @param emrClusterDefinition the EMR definition name value.
     *
     * @return list of all tag definitions for the given configuration.
     */
    private List<Tag> getEmrTags(EmrClusterDefinition emrClusterDefinition)
    {
        List<Tag> tags = new ArrayList<>();

        // Get the nodeTags from xml
        for (NodeTag thisTag : emrClusterDefinition.getNodeTags())
        {
            // Create a AWS tag and add
            if (StringUtils.isNotBlank(thisTag.getTagName()) && StringUtils.isNotBlank(thisTag.getTagValue()))
            {
                tags.add(new Tag(thisTag.getTagName(), thisTag.getTagValue()));
            }
        }

        // Return the object
        return tags;
    }

    /**
     * Get the encryption script location from the bucket name and encryption script location.
     *
     * @return location of the encryption script.
     */
    private String getEncryptionScriptLocation()
    {
        // Whenever the user requests for encryption, we have an encryption script that is stored in herd bucket.
        // We use this encryption script to encrypt all the volumes of all the instances.
        // Amazon plans to support encryption in EMR soon. Once that support is enabled, we can remove this script and use the one provided by AWS.
        return getS3StagingLocation() + configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER) +
            configurationHelper.getProperty(ConfigurationValue.EMR_ENCRYPTION_SCRIPT);
    }

    /**
     * Creates the job flow instance configuration containing specification of the number and type of Amazon EC2 instances.
     *
     * @param emrClusterDefinition the EMR cluster definition that contains all the EMR parameters
     *
     * @return the job flow instance configuration
     */
    private JobFlowInstancesConfig getJobFlowInstancesConfig(EmrClusterDefinition emrClusterDefinition)
    {
        // Create a new job flow instances configuration object.
        JobFlowInstancesConfig jobFlowInstancesConfig = new JobFlowInstancesConfig();
        // Set up master/slave security group
        jobFlowInstancesConfig.setEmrManagedMasterSecurityGroup(emrClusterDefinition.getMasterSecurityGroup());
        jobFlowInstancesConfig.setEmrManagedSlaveSecurityGroup(emrClusterDefinition.getSlaveSecurityGroup());

        // Add additional security groups to master nodes.
        jobFlowInstancesConfig.setAdditionalMasterSecurityGroups(emrClusterDefinition.getAdditionalMasterSecurityGroups());

        // Add additional security groups to slave nodes.
        jobFlowInstancesConfig.setAdditionalSlaveSecurityGroups(emrClusterDefinition.getAdditionalSlaveSecurityGroups());

        // Fill-in the ssh key.
        if (StringUtils.isNotBlank(emrClusterDefinition.getSshKeyPairName()))
        {
            jobFlowInstancesConfig.setEc2KeyName(emrClusterDefinition.getSshKeyPairName());
        }

        // Fill in configuration for the instance groups in a cluster.
        jobFlowInstancesConfig.setInstanceGroups(getInstanceGroupConfigs(emrClusterDefinition.getInstanceDefinitions()));

        // Fill in instance fleet configuration.
        jobFlowInstancesConfig.setInstanceFleets(getInstanceFleets(emrClusterDefinition.getInstanceFleets()));

        // Fill-in subnet id.
        if (StringUtils.isNotBlank(emrClusterDefinition.getSubnetId()))
        {
            // Use collection of subnet IDs when instance fleet configuration is specified. Otherwise, we expect a single EC2 subnet ID to be passed here.
            if (CollectionUtils.isNotEmpty(jobFlowInstancesConfig.getInstanceFleets()))
            {
                jobFlowInstancesConfig.setEc2SubnetIds(herdStringHelper.splitAndTrim(emrClusterDefinition.getSubnetId(), ","));
            }
            else
            {
                jobFlowInstancesConfig.setEc2SubnetId(emrClusterDefinition.getSubnetId());
            }
        }

        // Fill in optional keep alive flag.
        if (emrClusterDefinition.isKeepAlive() != null)
        {
            jobFlowInstancesConfig.setKeepJobFlowAliveWhenNoSteps(emrClusterDefinition.isKeepAlive());
        }

        // Fill in optional termination protection flag.
        if (emrClusterDefinition.isTerminationProtection() != null)
        {
            jobFlowInstancesConfig.setTerminationProtected(emrClusterDefinition.isTerminationProtection());
        }

        // Fill in optional Hadoop version flag.
        if (StringUtils.isNotBlank(emrClusterDefinition.getHadoopVersion()))
        {
            jobFlowInstancesConfig.setHadoopVersion(emrClusterDefinition.getHadoopVersion());
        }

        // Return the object.
        return jobFlowInstancesConfig;
    }

    /**
     * Create the run job flow request object.
     *
     * @param emrClusterDefinition the EMR definition name value
     * @param clusterName the EMR cluster name
     *
     * @return the run job flow request for the given configuration
     */
    private RunJobFlowRequest getRunJobFlowRequest(String clusterName, EmrClusterDefinition emrClusterDefinition)
    {
        // Create the object
        RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest(clusterName, getJobFlowInstancesConfig(emrClusterDefinition));

        // Set release label
        if (StringUtils.isNotBlank(emrClusterDefinition.getReleaseLabel()))
        {
            runJobFlowRequest.setReleaseLabel(emrClusterDefinition.getReleaseLabel());
        }

        // Set list of Applications
        List<EmrClusterDefinitionApplication> emrClusterDefinitionApplications = emrClusterDefinition.getApplications();
        if (!CollectionUtils.isEmpty(emrClusterDefinitionApplications))
        {
            runJobFlowRequest.setApplications(getApplications(emrClusterDefinitionApplications));
        }

        // Set list of Configurations
        List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations = emrClusterDefinition.getConfigurations();
        if (!CollectionUtils.isEmpty(emrClusterDefinitionConfigurations))
        {
            runJobFlowRequest.setConfigurations(getConfigurations(emrClusterDefinitionConfigurations));
        }

        // Set the log bucket if specified
        if (StringUtils.isNotBlank(emrClusterDefinition.getLogBucket()))
        {
            runJobFlowRequest.setLogUri(emrClusterDefinition.getLogBucket());
        }

        // Set the visible to all flag
        if (emrClusterDefinition.isVisibleToAll() != null)
        {
            runJobFlowRequest.setVisibleToAllUsers(emrClusterDefinition.isVisibleToAll());
        }

        // Set the IAM profile for the nodes
        if (StringUtils.isNotBlank(emrClusterDefinition.getEc2NodeIamProfileName()))
        {
            runJobFlowRequest.setJobFlowRole(emrClusterDefinition.getEc2NodeIamProfileName());
        }
        else
        {
            runJobFlowRequest.setJobFlowRole(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_EC2_NODE_IAM_PROFILE_NAME));
        }

        // Set the IAM profile for the service
        if (StringUtils.isNotBlank(emrClusterDefinition.getServiceIamRole()))
        {
            runJobFlowRequest.setServiceRole(emrClusterDefinition.getServiceIamRole());
        }
        else
        {
            runJobFlowRequest.setServiceRole(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_SERVICE_IAM_ROLE_NAME));
        }

        // Set the AMI version if specified
        if (StringUtils.isNotBlank(emrClusterDefinition.getAmiVersion()))
        {
            runJobFlowRequest.setAmiVersion(emrClusterDefinition.getAmiVersion());
        }

        // Set the additionalInfo if specified
        if (StringUtils.isNotBlank(emrClusterDefinition.getAdditionalInfo()))
        {
            runJobFlowRequest.setAdditionalInfo(emrClusterDefinition.getAdditionalInfo());
        }

        // Set the bootstrap actions
        if (!getBootstrapActionConfigList(emrClusterDefinition).isEmpty())
        {
            runJobFlowRequest.setBootstrapActions(getBootstrapActionConfigList(emrClusterDefinition));
        }

        // Set the app installation steps
        runJobFlowRequest.setSteps(getStepConfig(emrClusterDefinition));

        // Set the tags
        runJobFlowRequest.setTags(getEmrTags(emrClusterDefinition));

        // Assign supported products as applicable
        if (StringUtils.isNotBlank(emrClusterDefinition.getSupportedProduct()))
        {
            List<String> supportedProducts = new ArrayList<>();
            supportedProducts.add(emrClusterDefinition.getSupportedProduct());
            runJobFlowRequest.setSupportedProducts(supportedProducts);
        }

        // Assign security configuration.
        if (StringUtils.isNotBlank(emrClusterDefinition.getSecurityConfiguration()))
        {
            runJobFlowRequest.setSecurityConfiguration(emrClusterDefinition.getSecurityConfiguration());
        }

        // Assign scale down behavior.
        if (StringUtils.isNotBlank(emrClusterDefinition.getScaleDownBehavior()))
        {
            runJobFlowRequest.setScaleDownBehavior(emrClusterDefinition.getScaleDownBehavior());
        }

        // Assign Kerberos attributes.
        runJobFlowRequest.setKerberosAttributes(getKerberosAttributes(emrClusterDefinition.getKerberosAttributes()));

        // Return the object
        return runJobFlowRequest;
    }

    /**
     * Get the S3_STAGING_RESOURCE full path from the bucket name as well as other details.
     *
     * @return the s3 managed location.
     */
    private String getS3StagingLocation()
    {
        return configurationHelper.getProperty(ConfigurationValue.S3_URL_PROTOCOL) +
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_BUCKET_NAME) +
            configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER) +
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE);
    }

    /**
     * Create the step config list of objects for hive/pig installation.
     *
     * @param emrClusterDefinition the EMR definition name value.
     *
     * @return list of step configuration that contains all the steps for the given configuration.
     */
    private List<StepConfig> getStepConfig(EmrClusterDefinition emrClusterDefinition)
    {
        StepFactory stepFactory = new StepFactory();
        List<StepConfig> appSteps = new ArrayList<>();

        // Create install hive step and add to the StepConfig list
        if (StringUtils.isNotBlank(emrClusterDefinition.getHiveVersion()))
        {
            StepConfig installHive =
                new StepConfig().withName("Hive " + emrClusterDefinition.getHiveVersion()).withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                    .withHadoopJarStep(stepFactory.newInstallHiveStep(emrClusterDefinition.getHiveVersion()));
            appSteps.add(installHive);
        }

        // Create install Pig step and add to the StepConfig List
        if (StringUtils.isNotBlank(emrClusterDefinition.getPigVersion()))
        {
            StepConfig installPig =
                new StepConfig().withName("Pig " + emrClusterDefinition.getPigVersion()).withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                    .withHadoopJarStep(stepFactory.newInstallPigStep(emrClusterDefinition.getPigVersion()));
            appSteps.add(installPig);
        }

        // Add the hadoop jar steps that need to be added.
        if (!CollectionUtils.isEmpty(emrClusterDefinition.getHadoopJarSteps()))
        {
            for (HadoopJarStep hadoopJarStep : emrClusterDefinition.getHadoopJarSteps())
            {
                StepConfig stepConfig = emrHelper
                    .getEmrHadoopJarStepConfig(hadoopJarStep.getStepName(), hadoopJarStep.getJarLocation(), hadoopJarStep.getMainClass(),
                        hadoopJarStep.getScriptArguments(), hadoopJarStep.isContinueOnError());

                appSteps.add(stepConfig);
            }
        }

        return appSteps;
    }
}
