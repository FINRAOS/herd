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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.KerberosAttributes;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepState;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.ConfigurationFile;
import org.finra.herd.model.api.xml.ConfigurationFiles;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceTypeConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKerberosAttributes;
import org.finra.herd.model.api.xml.EmrClusterDefinitionLaunchSpecifications;
import org.finra.herd.model.api.xml.HadoopJarStep;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.KeyValuePairConfiguration;
import org.finra.herd.model.api.xml.KeyValuePairConfigurations;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.ScriptDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

public class EmrDaoTest extends AbstractDaoTest
{
    @Autowired
    private EmrDao emrDao;

    @Autowired
    private HerdStringHelper herdStringHelper;

    private EmrOperations originalEmrOperations;

    private Ec2Dao originalEc2Dao;

    private EmrOperations mockEmrOperations = mock(EmrOperations.class);

    private Ec2Dao mockEc2Dao = mock(Ec2Dao.class);

    /**
     * Intialize mocks and inject.
     * <p/>
     * Normally this would be handled by Spring's IOC, but at the moment other unit tests may be affected if we completely replace the mocked implementations.
     */
    @Before
    public void before()
    {
        mockEmrOperations = mock(EmrOperations.class);
        mockEc2Dao = mock(Ec2Dao.class);
        originalEmrOperations = (EmrOperations) ReflectionTestUtils.getField(emrDao, "emrOperations");
        originalEc2Dao = (Ec2Dao) ReflectionTestUtils.getField(emrDao, "ec2Dao");
        ReflectionTestUtils.setField(emrDao, "emrOperations", mockEmrOperations);
        ReflectionTestUtils.setField(emrDao, "ec2Dao", mockEc2Dao);
    }

    /**
     * Reset the injected mocks with the original implementation.
     */
    @After
    public void after()
    {
        ReflectionTestUtils.setField(emrDao, "emrOperations", originalEmrOperations);
        ReflectionTestUtils.setField(emrDao, "ec2Dao", originalEc2Dao);
    }

    @Test
    public void addEmrStepCallsAddJobFlowSteps() throws Exception
    {
        String clusterName = "clusterName";
        StepConfig emrStepConfig = new StepConfig();

        String clusterId = "clusterId";
        String stepId = "stepId";

        /*
         * Mock the EmrOperations.listEmrClusters() call to return a known result.
         */
        ListClustersResult listClustersResult = new ListClustersResult();
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(clusterId);
        clusterSummary.setName(clusterName);
        listClustersResult.setClusters(Arrays.asList(clusterSummary));
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(listClustersResult);

        /*
         * Mock EmrOperations.addJobFlowStepsRequest() and assert parameters passed in.
         */
        when(mockEmrOperations.addJobFlowStepsRequest(any(), any())).thenAnswer(new Answer<List<String>>()
        {
            @Override
            public List<String> answer(InvocationOnMock invocation) throws Throwable
            {
                AddJobFlowStepsRequest addJobFlowStepsRequest = invocation.getArgument(1);
                assertEquals(clusterId, addJobFlowStepsRequest.getJobFlowId());
                List<StepConfig> steps = addJobFlowStepsRequest.getSteps();
                assertEquals(1, steps.size());
                assertEquals(emrStepConfig, steps.get(0));

                // return a single step with the given stepId
                return Arrays.asList(stepId);
            }
        });

        assertEquals(stepId, emrDao.addEmrStep(clusterId, emrStepConfig, new AwsParamsDto()));
    }

    @Test
    public void addEmrMasterSecurityGroupsCallsEc2AddSecurityGroup() throws Exception
    {
        String clusterName = "clusterName";
        List<String> securityGroups = Arrays.asList("securityGroup");
        AwsParamsDto awsParams = new AwsParamsDto();
        String ec2InstanceId = "ec2InstanceId";

        ListClustersResult listClustersResult = new ListClustersResult();
        listClustersResult.setClusters(new ArrayList<>());
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId("clusterId");
        clusterSummary.setName(clusterName);
        listClustersResult.getClusters().add(clusterSummary);
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(listClustersResult);

        ListInstancesResult listInstancesResult = new ListInstancesResult();
        listInstancesResult.setInstances(new ArrayList<>());
        Instance instance = new Instance();
        instance.setEc2InstanceId(ec2InstanceId);
        listInstancesResult.getInstances().add(instance);
        when(mockEmrOperations.listClusterInstancesRequest(any(), any())).thenReturn(listInstancesResult);

        emrDao.addEmrMasterSecurityGroups(clusterName, securityGroups, awsParams);

        verify(mockEc2Dao).addSecurityGroupsToEc2Instance(eq(ec2InstanceId), eq(securityGroups), any());
        verifyNoMoreInteractions(mockEc2Dao);
    }

    @Test
    public void addEmrMasterSecurityGroupsThrowWhenNoInstancesFound() throws Exception
    {
        String clusterName = "clusterName";
        List<String> securityGroups = Arrays.asList("securityGroup");
        AwsParamsDto awsParams = new AwsParamsDto();

        ListClustersResult listClustersResult = new ListClustersResult();
        listClustersResult.setClusters(new ArrayList<>());
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId("clusterId");
        clusterSummary.setName(clusterName);
        listClustersResult.getClusters().add(clusterSummary);
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(listClustersResult);

        when(mockEmrOperations.listClusterInstancesRequest(any(), any())).thenReturn(new ListInstancesResult());

        try
        {
            emrDao.addEmrMasterSecurityGroups(clusterName, securityGroups, awsParams);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("No master instances found for the cluster \"" + clusterName + "\".", e.getMessage());
        }
    }

    @Test
    public void getEmrMasterInstanceReturnsInstance() throws Exception
    {
        String clusterId = "clusterId";

        Instance expectedInstance = new Instance();

        /*
         * Stub EmrOperations.listClusterInstancesRequest() and assert correct parameters are being passed into the method when the call is made.
         */
        when(mockEmrOperations.listClusterInstancesRequest(any(), any())).thenAnswer(new Answer<ListInstancesResult>()
        {
            @Override
            public ListInstancesResult answer(InvocationOnMock invocation) throws Throwable
            {
                /*
                 * Assert correct parameters are used when calling this method.
                 */
                ListInstancesRequest listInstancesRequest = invocation.getArgument(1);
                assertEquals(clusterId, listInstancesRequest.getClusterId());
                List<String> instanceGroupTypes = listInstancesRequest.getInstanceGroupTypes();
                assertEquals(1, instanceGroupTypes.size());
                assertEquals("MASTER", instanceGroupTypes.get(0));

                ListInstancesResult listInstancesResult = new ListInstancesResult();
                listInstancesResult.setInstances(Arrays.asList(expectedInstance));
                return listInstancesResult;
            }
        });

        Instance actualInstance = emrDao.getEmrMasterInstance(clusterId, new AwsParamsDto());

        assertEquals(expectedInstance, actualInstance);
    }

    @Test
    public void getEmrMasterInstanceThrowsWhenNoInstance() throws Exception
    {
        String clusterId = "clusterId";

        when(mockEmrOperations.listClusterInstancesRequest(any(), any())).thenReturn(new ListInstancesResult());

        try
        {
            emrDao.getEmrMasterInstance(clusterId, new AwsParamsDto());
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("No master instances found for the cluster \"" + clusterId + "\".", e.getMessage());
        }
    }

    @Test
    public void createEmrClusterAssertCallRunEmrJobFlowRequiredParamsOnly() throws Exception
    {
        String clusterName = "clusterName";
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();
        instanceDefinitions.setMasterInstances(
            new MasterInstanceDefinition(10, "masterInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE,
                NO_INSTANCE_MAX_SEARCH_PRICE, NO_INSTANCE_ON_DEMAND_THRESHOLD));
        instanceDefinitions.setCoreInstances(
            new InstanceDefinition(20, "coreInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE, NO_INSTANCE_MAX_SEARCH_PRICE,
                NO_INSTANCE_ON_DEMAND_THRESHOLD));
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Arrays.asList(new NodeTag("tagName", "tagValue")));

        String clusterId = "clusterId";

        when(mockEmrOperations.runEmrJobFlow(any(), any())).then(new Answer<String>()
        {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable
            {
                /*
                 * Assert that the given EMR cluster definition produced the correct RunJobFlowRequest
                 */
                RunJobFlowRequest runJobFlowRequest = invocation.getArgument(1);
                JobFlowInstancesConfig jobFlowInstancesConfig = runJobFlowRequest.getInstances();
                List<InstanceGroupConfig> instanceGroupConfigs = jobFlowInstancesConfig.getInstanceGroups();
                assertEquals(2, instanceGroupConfigs.size());
                {
                    InstanceGroupConfig instanceGroupConfig = instanceGroupConfigs.get(0);
                    assertEquals(10, instanceGroupConfig.getInstanceCount().intValue());
                    assertEquals("masterInstanceType", instanceGroupConfig.getInstanceType());
                }
                {
                    InstanceGroupConfig instanceGroupConfig = instanceGroupConfigs.get(1);
                    assertEquals(20, instanceGroupConfig.getInstanceCount().intValue());
                    assertEquals("coreInstanceType", instanceGroupConfig.getInstanceType());
                }
                assertEquals(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_EC2_NODE_IAM_PROFILE_NAME),
                    runJobFlowRequest.getJobFlowRole());
                assertEquals(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_SERVICE_IAM_ROLE_NAME),
                    runJobFlowRequest.getServiceRole());
                List<StepConfig> stepConfigs = runJobFlowRequest.getSteps();
                assertEquals(0, stepConfigs.size());
                List<Tag> tags = runJobFlowRequest.getTags();
                assertEquals(1, tags.size());
                {
                    Tag tag = tags.get(0);
                    assertEquals("tagName", tag.getKey());
                    assertEquals("tagValue", tag.getValue());
                }

                return clusterId;
            }
        });

        assertEquals(clusterId, emrDao.createEmrCluster(clusterName, emrClusterDefinition, new AwsParamsDto()));
    }

    @Test
    public void createEmrClusterAssertCallRunEmrJobFlowWithInstanceFleetAndMultipleSubnets() throws Exception
    {
        // Create objects required for testing.
        final String clusterName = "clusterName";
        final String clusterId = "clusterId";
        final String name = STRING_VALUE;
        final String instanceFleetType = STRING_VALUE_2;
        final Integer targetOnDemandCapacity = INTEGER_VALUE;
        final Integer targetSpotCapacity = INTEGER_VALUE_2;
        final List<EmrClusterDefinitionInstanceTypeConfig> emrClusterDefinitionInstanceTypeConfigs = null;
        final EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications = null;
        final EmrClusterDefinitionInstanceFleet emrClusterDefinitionInstanceFleet =
            new EmrClusterDefinitionInstanceFleet(name, instanceFleetType, targetOnDemandCapacity, targetSpotCapacity, emrClusterDefinitionInstanceTypeConfigs,
                emrClusterDefinitionLaunchSpecifications);

        // Create an EMR cluster definition with instance fleet configuration and multiple EC2 subnet IDs.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleets(Arrays.asList(emrClusterDefinitionInstanceFleet));
        emrClusterDefinition.setSubnetId(String.format("%s , %s  ", EC2_SUBNET, EC2_SUBNET_2));
        emrClusterDefinition.setNodeTags(Arrays.asList(new NodeTag("tagName", "tagValue")));

        when(mockEmrOperations.runEmrJobFlow(any(), any())).then(new Answer<String>()
        {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable
            {
                // Assert that the given EMR cluster definition produced the correct RunJobFlowRequest.
                RunJobFlowRequest runJobFlowRequest = invocation.getArgument(1);
                JobFlowInstancesConfig jobFlowInstancesConfig = runJobFlowRequest.getInstances();
                assertEquals(0, CollectionUtils.size(jobFlowInstancesConfig.getInstanceGroups()));
                final List<InstanceTypeConfig> expectedInstanceTypeConfigs = null;
                assertEquals(Arrays.asList(
                    new InstanceFleetConfig().withName(name).withInstanceFleetType(instanceFleetType).withTargetOnDemandCapacity(targetOnDemandCapacity)
                        .withTargetSpotCapacity(targetSpotCapacity).withInstanceTypeConfigs(expectedInstanceTypeConfigs).withLaunchSpecifications(null)),
                    jobFlowInstancesConfig.getInstanceFleets());
                assertNull(jobFlowInstancesConfig.getEc2SubnetId());
                assertEquals(2, CollectionUtils.size(jobFlowInstancesConfig.getEc2SubnetIds()));
                assertTrue(jobFlowInstancesConfig.getEc2SubnetIds().contains(EC2_SUBNET));
                assertTrue(jobFlowInstancesConfig.getEc2SubnetIds().contains(EC2_SUBNET_2));
                assertEquals(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_EC2_NODE_IAM_PROFILE_NAME),
                    runJobFlowRequest.getJobFlowRole());
                assertEquals(herdStringHelper.getRequiredConfigurationValue(ConfigurationValue.EMR_DEFAULT_SERVICE_IAM_ROLE_NAME),
                    runJobFlowRequest.getServiceRole());
                List<StepConfig> stepConfigs = runJobFlowRequest.getSteps();
                assertEquals(0, stepConfigs.size());
                List<Tag> tags = runJobFlowRequest.getTags();
                assertEquals(1, tags.size());
                {
                    Tag tag = tags.get(0);
                    assertEquals("tagName", tag.getKey());
                    assertEquals("tagValue", tag.getValue());
                }

                return clusterId;
            }
        });

        assertEquals(clusterId, emrDao.createEmrCluster(clusterName, emrClusterDefinition, new AwsParamsDto()));
    }

    @Test
    public void createEmrClusterAssertCallRunEmrJobFlowOptionalParams() throws Exception
    {
        String clusterName = "clusterName";
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();
        instanceDefinitions.setMasterInstances(
            new MasterInstanceDefinition(10, "masterInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE,
                NO_INSTANCE_MAX_SEARCH_PRICE, NO_INSTANCE_ON_DEMAND_THRESHOLD));
        instanceDefinitions.setCoreInstances(
            new InstanceDefinition(20, "coreInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, BigDecimal.ONE, NO_INSTANCE_MAX_SEARCH_PRICE,
                NO_INSTANCE_ON_DEMAND_THRESHOLD));
        instanceDefinitions.setTaskInstances(
            new InstanceDefinition(30, "taskInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE, NO_INSTANCE_MAX_SEARCH_PRICE,
                NO_INSTANCE_ON_DEMAND_THRESHOLD));
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Arrays.asList(new NodeTag("tagName", "tagValue"), new NodeTag("", "tagValue"), new NodeTag("tagName", "")));

        emrClusterDefinition.setSshKeyPairName("sshKeyPairName");
        emrClusterDefinition.setSubnetId("subnetId");
        emrClusterDefinition.setKeepAlive(true);
        emrClusterDefinition.setTerminationProtection(true);
        emrClusterDefinition.setHadoopVersion("hadoopVersion");

        emrClusterDefinition.setReleaseLabel("releaseLabel");
        emrClusterDefinition.setApplications(new ArrayList<>());
        {
            EmrClusterDefinitionApplication emrClusterDefinitionApplication = new EmrClusterDefinitionApplication();
            emrClusterDefinitionApplication.setName("applicationName1");
            emrClusterDefinitionApplication.setVersion("applicationVersion1");
            emrClusterDefinitionApplication.setArgs(Arrays.asList("applicationArg1"));
            emrClusterDefinition.getApplications().add(emrClusterDefinitionApplication);
        }
        {
            EmrClusterDefinitionApplication emrClusterDefinitionApplication = new EmrClusterDefinitionApplication();
            emrClusterDefinitionApplication.setName("applicationName2");
            emrClusterDefinitionApplication.setVersion("applicationVersion2");
            emrClusterDefinitionApplication.setArgs(Arrays.asList("applicationArg2"));
            emrClusterDefinitionApplication
                .setAdditionalInfoList(Arrays.asList(new Parameter("applicationAdditionalInfoName2", "applicationAdditionalInfoValue2")));
            emrClusterDefinition.getApplications().add(emrClusterDefinitionApplication);
        }
        emrClusterDefinition.setConfigurations(new ArrayList<>());
        {
            EmrClusterDefinitionConfiguration emrClusterDefinitionConfiguration = new EmrClusterDefinitionConfiguration();
            emrClusterDefinitionConfiguration.setClassification("classification");
            EmrClusterDefinitionConfiguration emrClusterDefinitionConfigurationInner = new EmrClusterDefinitionConfiguration();
            emrClusterDefinitionConfigurationInner.setClassification("classificationInner");
            emrClusterDefinitionConfiguration.setConfigurations(Arrays.asList(emrClusterDefinitionConfigurationInner));
            emrClusterDefinitionConfiguration.setProperties(Arrays.asList(new Parameter("propertyKey", "propertyValue")));
            emrClusterDefinition.getConfigurations().add(emrClusterDefinitionConfiguration);
        }
        emrClusterDefinition.setLogBucket("logBucket");
        emrClusterDefinition.setVisibleToAll(true);
        emrClusterDefinition.setEc2NodeIamProfileName("ec2NodeIamProfileName");
        emrClusterDefinition.setServiceIamRole("serviceIamRole");
        emrClusterDefinition.setAmiVersion("amiVersion");
        emrClusterDefinition.setAdditionalInfo("additionalInfo");
        emrClusterDefinition.setEncryptionEnabled(true);
        emrClusterDefinition.setDaemonConfigurations(Arrays.asList(new Parameter("daemonConfigurationsKey", "daemonConfigurationsValue")));
        ConfigurationFiles configurationFiles = new ConfigurationFiles();
        configurationFiles.getConfigurationFiles().add(new ConfigurationFile("fileNameShortcut", "configFileLocation"));
        KeyValuePairConfigurations keyValuePairConfigurations = new KeyValuePairConfigurations();
        keyValuePairConfigurations.getKeyValuePairConfigurations().add(new KeyValuePairConfiguration("keyValueShortcut", "attribKey", "attribVal"));
        emrClusterDefinition.setHadoopConfigurations(Arrays.asList(configurationFiles, keyValuePairConfigurations));
        emrClusterDefinition.setCustomBootstrapActionAll(new ArrayList<>());
        {
            ScriptDefinition scriptDefinitionAll = new ScriptDefinition();
            scriptDefinitionAll.setScriptName("scriptDefinitionAllName1");
            scriptDefinitionAll.setScriptLocation("scriptDefinitionAllLocation1");
            scriptDefinitionAll.setScriptArguments(Arrays.asList("scriptDefinitionAllArg1"));
            emrClusterDefinition.getCustomBootstrapActionAll().add(scriptDefinitionAll);
        }
        {
            ScriptDefinition scriptDefinitionAll = new ScriptDefinition();
            scriptDefinitionAll.setScriptName("scriptDefinitionAllName2");
            scriptDefinitionAll.setScriptLocation("scriptDefinitionAllLocation2");
            emrClusterDefinition.getCustomBootstrapActionAll().add(scriptDefinitionAll);
        }
        emrClusterDefinition.setCustomBootstrapActionMaster(new ArrayList<>());
        {
            ScriptDefinition scriptDefinitionMaster = new ScriptDefinition();
            scriptDefinitionMaster.setScriptName("scriptDefinitionMasterName1");
            scriptDefinitionMaster.setScriptLocation("scriptDefinitionMasterLocation1");
            scriptDefinitionMaster.setScriptArguments(Arrays.asList("scriptDefinitionMasterArg1"));
            emrClusterDefinition.getCustomBootstrapActionMaster().add(scriptDefinitionMaster);
        }
        {
            ScriptDefinition scriptDefinitionMaster = new ScriptDefinition();
            scriptDefinitionMaster.setScriptName("scriptDefinitionMasterName2");
            scriptDefinitionMaster.setScriptLocation("scriptDefinitionMasterLocation2");
            emrClusterDefinition.getCustomBootstrapActionMaster().add(scriptDefinitionMaster);
        }
        emrClusterDefinition.setHiveVersion("hiveVersion");
        emrClusterDefinition.setPigVersion("pigVersion");
        emrClusterDefinition.setInstallOozie(true);
        emrClusterDefinition.setHadoopJarSteps(Arrays.asList(new HadoopJarStep("stepName", "jarLocation", "mainClass", null, true)));
        emrClusterDefinition.setSupportedProduct("supportedProduct");
        emrClusterDefinition.setSecurityConfiguration("securityConfiguration");
        emrClusterDefinition.setScaleDownBehavior("scaleDownBehavior");

        emrClusterDefinition.setMasterSecurityGroup(EMR_MASTER_SECURITY_GROUP);
        emrClusterDefinition.setSlaveSecurityGroup(EMR_SLAVE_SECURITY_GROUP);
        emrClusterDefinition.setKerberosAttributes(
            new EmrClusterDefinitionKerberosAttributes(AD_DOMAIN_JOIN_PASSWORD, AD_DOMAIN_JOIN_USER, CROSS_REALM_TRUST_PRINCIPAL_PASSWORD, KDC_ADMIN_PASSWORD,
                REALM));
        String clusterId = "clusterId";

        when(mockEmrOperations.runEmrJobFlow(any(), any())).then(new Answer<String>()
        {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable
            {
                /*
                 * Assert that the given EMR cluster definition produced the correct RunJobFlowRequest
                 */
                RunJobFlowRequest runJobFlowRequest = invocation.getArgument(1);
                assertEquals(3, runJobFlowRequest.getInstances().getInstanceGroups().size());
                {
                    InstanceGroupConfig instanceGroupConfig = runJobFlowRequest.getInstances().getInstanceGroups().get(1);
                    assertEquals("1", instanceGroupConfig.getBidPrice());
                }
                {
                    InstanceGroupConfig instanceGroupConfig = runJobFlowRequest.getInstances().getInstanceGroups().get(2);
                    assertEquals("taskInstanceType", instanceGroupConfig.getInstanceType());
                    assertEquals(30, instanceGroupConfig.getInstanceCount().intValue());
                }
                assertEquals("sshKeyPairName", runJobFlowRequest.getInstances().getEc2KeyName());
                assertEquals("subnetId", runJobFlowRequest.getInstances().getEc2SubnetId());
                assertEquals(true, runJobFlowRequest.getInstances().getKeepJobFlowAliveWhenNoSteps());
                assertEquals(true, runJobFlowRequest.getInstances().getTerminationProtected());
                assertEquals("hadoopVersion", runJobFlowRequest.getInstances().getHadoopVersion());
                assertEquals("releaseLabel", runJobFlowRequest.getReleaseLabel());
                assertEquals(2, runJobFlowRequest.getApplications().size());
                {
                    Application application = runJobFlowRequest.getApplications().get(0);
                    assertEquals("applicationName1", application.getName());
                    assertEquals("applicationVersion1", application.getVersion());
                    assertEquals(Arrays.asList("applicationArg1"), application.getArgs());
                }
                {
                    Application application = runJobFlowRequest.getApplications().get(1);
                    Map<String, String> additionalInfo = application.getAdditionalInfo();
                    assertEquals(1, additionalInfo.size());
                    assertEquals("applicationAdditionalInfoValue2", additionalInfo.get("applicationAdditionalInfoName2"));
                }
                assertEquals(1, runJobFlowRequest.getConfigurations().size());
                {
                    Configuration configuration = runJobFlowRequest.getConfigurations().get(0);
                    assertEquals("classification", configuration.getClassification());
                    assertEquals(1, configuration.getConfigurations().size());
                    {
                        Configuration configurationInner = configuration.getConfigurations().get(0);
                        assertEquals("classificationInner", configurationInner.getClassification());
                    }
                    assertEquals(1, configuration.getProperties().size());
                    assertEquals("propertyValue", configuration.getProperties().get("propertyKey"));
                }
                assertEquals("logBucket", runJobFlowRequest.getLogUri());
                assertEquals(true, runJobFlowRequest.getVisibleToAllUsers());
                assertEquals("ec2NodeIamProfileName", runJobFlowRequest.getJobFlowRole());
                assertEquals("serviceIamRole", runJobFlowRequest.getServiceRole());
                assertEquals("amiVersion", runJobFlowRequest.getAmiVersion());
                assertEquals("additionalInfo", runJobFlowRequest.getAdditionalInfo());
                assertEquals(7, runJobFlowRequest.getBootstrapActions().size());
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(0);
                    assertEquals("emr.encryption.script", bootstrapActionConfig.getName());
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals("s3:////herd_SCRIPTS/encrypt_disks.sh", scriptBootstrapAction.getPath());
                    assertEquals(0, scriptBootstrapAction.getArgs().size());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(1);
                    assertEquals("emr.aws.configure.daemon", bootstrapActionConfig.getName());
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals("s3://elasticmapreduce/bootstrap-actions/configure-daemons", scriptBootstrapAction.getPath());
                    assertEquals(Arrays.asList("daemonConfigurationsKey=daemonConfigurationsValue"), scriptBootstrapAction.getArgs());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(2);
                    assertEquals("emr.aws.configure.hadoop", bootstrapActionConfig.getName());
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals("s3://us-east-1.elasticmapreduce/bootstrap-actions/configure-hadoop", scriptBootstrapAction.getPath());
                    assertEquals(Arrays.asList("fileNameShortcut", "configFileLocation", "keyValueShortcut", "attribKey=attribVal"),
                        scriptBootstrapAction.getArgs());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(3);
                    assertEquals("scriptDefinitionAllName1", bootstrapActionConfig.getName());
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals("scriptDefinitionAllLocation1", scriptBootstrapAction.getPath());
                    assertEquals(Arrays.asList("scriptDefinitionAllArg1"), scriptBootstrapAction.getArgs());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(4);
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals(0, scriptBootstrapAction.getArgs().size());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(5);
                    assertEquals("scriptDefinitionMasterName1", bootstrapActionConfig.getName());
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals("s3://elasticmapreduce/bootstrap-actions/run-if", scriptBootstrapAction.getPath());
                    assertEquals(Arrays.asList("instance.isMaster=true", "scriptDefinitionMasterLocation1", "scriptDefinitionMasterArg1"),
                        scriptBootstrapAction.getArgs());
                }
                {
                    BootstrapActionConfig bootstrapActionConfig = runJobFlowRequest.getBootstrapActions().get(6);
                    ScriptBootstrapActionConfig scriptBootstrapAction = bootstrapActionConfig.getScriptBootstrapAction();
                    assertEquals(Arrays.asList("instance.isMaster=true", "scriptDefinitionMasterLocation2"), scriptBootstrapAction.getArgs());
                }
                assertEquals(Arrays.asList("supportedProduct"), runJobFlowRequest.getSupportedProducts());
                assertEquals("securityConfiguration", runJobFlowRequest.getSecurityConfiguration());
                assertEquals("scaleDownBehavior", runJobFlowRequest.getScaleDownBehavior());
                assertEquals(EMR_MASTER_SECURITY_GROUP, runJobFlowRequest.getInstances().getEmrManagedMasterSecurityGroup());
                assertEquals(EMR_SLAVE_SECURITY_GROUP, runJobFlowRequest.getInstances().getEmrManagedSlaveSecurityGroup());
                assertEquals(new KerberosAttributes().withADDomainJoinPassword(AD_DOMAIN_JOIN_PASSWORD).withADDomainJoinUser(AD_DOMAIN_JOIN_USER)
                        .withCrossRealmTrustPrincipalPassword(CROSS_REALM_TRUST_PRINCIPAL_PASSWORD).withKdcAdminPassword(KDC_ADMIN_PASSWORD).withRealm(REALM),
                    runJobFlowRequest.getKerberosAttributes());
                return clusterId;
            }
        });

        assertEquals(clusterId, emrDao.createEmrCluster(clusterName, emrClusterDefinition, new AwsParamsDto()));
    }

    @Test
    public void createEmrClusterAssertEncryptionDisabled() throws Exception
    {
        /*
         * Use only minimum required options
         */
        String clusterName = "clusterName";
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();
        instanceDefinitions.setMasterInstances(
            new MasterInstanceDefinition(10, "masterInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE,
                NO_INSTANCE_MAX_SEARCH_PRICE, NO_INSTANCE_ON_DEMAND_THRESHOLD));
        instanceDefinitions.setCoreInstances(
            new InstanceDefinition(20, "coreInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE, NO_INSTANCE_MAX_SEARCH_PRICE,
                NO_INSTANCE_ON_DEMAND_THRESHOLD));
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Arrays.asList(new NodeTag("tagName", "tagValue")));

        emrClusterDefinition.setEncryptionEnabled(false);

        String clusterId = "clusterId";

        when(mockEmrOperations.runEmrJobFlow(any(), any())).then(new Answer<String>()
        {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable
            {
                RunJobFlowRequest runJobFlowRequest = invocation.getArgument(1);
                // No bootstrap action should be added
                assertEquals(0, runJobFlowRequest.getBootstrapActions().size());
                return clusterId;
            }
        });

        assertEquals(clusterId, emrDao.createEmrCluster(clusterName, emrClusterDefinition, new AwsParamsDto()));
    }

    @Test
    public void createEmrClusterAssertInstallOozieDisabled() throws Exception
    {
        /*
         * Use only minimum required options
         */
        String clusterName = "clusterName";
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        InstanceDefinitions instanceDefinitions = new InstanceDefinitions();
        instanceDefinitions.setMasterInstances(
            new MasterInstanceDefinition(10, "masterInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE,
                NO_INSTANCE_MAX_SEARCH_PRICE, NO_INSTANCE_ON_DEMAND_THRESHOLD));
        instanceDefinitions.setCoreInstances(
            new InstanceDefinition(20, "coreInstanceType", NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION, NO_INSTANCE_SPOT_PRICE, NO_INSTANCE_MAX_SEARCH_PRICE,
                NO_INSTANCE_ON_DEMAND_THRESHOLD));
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Arrays.asList(new NodeTag("tagName", "tagValue")));

        emrClusterDefinition.setInstallOozie(false);

        String clusterId = "clusterId";

        when(mockEmrOperations.runEmrJobFlow(any(), any())).then(new Answer<String>()
        {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable
            {
                RunJobFlowRequest runJobFlowRequest = invocation.getArgument(1);
                // The oozie step should be skipped.
                assertEquals(0, runJobFlowRequest.getSteps().size());
                return clusterId;
            }
        });

        assertEquals(clusterId, emrDao.createEmrCluster(clusterName, emrClusterDefinition, new AwsParamsDto()));
    }

    @Test
    public void terminateEmrCluster() throws Exception
    {
        String clusterName = "clusterName";
        boolean overrideTerminationProtection = false;
        String clusterId = "clusterId";

        ListClustersResult listClustersResult = new ListClustersResult();
        listClustersResult.setClusters(new ArrayList<>());
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(clusterId);
        clusterSummary.setName(clusterName);
        listClustersResult.getClusters().add(clusterSummary);
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(listClustersResult);

        emrDao.terminateEmrCluster(clusterId, overrideTerminationProtection, new AwsParamsDto());

        // Assert that terminateEmrCluster was called with these parameters ONCE
        verify(mockEmrOperations).terminateEmrCluster(any(), eq(clusterId), eq(overrideTerminationProtection));
    }

    @Test
    public void getEmrClusterByIdAssertCallDescribeCluster() throws Exception
    {
        String clusterId = "clusterId";
        Cluster expectedCluster = new Cluster();

        when(mockEmrOperations.describeClusterRequest(any(), any())).thenAnswer(new Answer<DescribeClusterResult>()
        {
            @Override
            public DescribeClusterResult answer(InvocationOnMock invocation) throws Throwable
            {
                DescribeClusterRequest describeClusterRequest = invocation.getArgument(1);
                assertEquals(clusterId, describeClusterRequest.getClusterId());

                DescribeClusterResult describeClusterResult = new DescribeClusterResult();
                describeClusterResult.setCluster(expectedCluster);
                return describeClusterResult;
            }
        });

        assertEquals(expectedCluster, emrDao.getEmrClusterById(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getEmrClusterByIdAssertNoCallWhenClusterIdIsBlank() throws Exception
    {
        String clusterId = "";

        assertNull(emrDao.getEmrClusterById(clusterId, new AwsParamsDto()));

        // Assert nothing was called on EMR operations
        verifyNoMoreInteractions(mockEmrOperations);
    }

    @Test
    public void getEmrClusterByIdAssertReturnNullWhenDescribeClusterResponseIsNull() throws Exception
    {
        String clusterId = "clusterId";

        when(mockEmrOperations.describeClusterRequest(any(), any())).thenReturn(null);
        assertNull(emrDao.getEmrClusterById(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getEmrClusterByIdAssertReturnNullWhenDescribeClusterResponseClusterIsNull() throws Exception
    {
        String clusterId = "clusterId";

        when(mockEmrOperations.describeClusterRequest(any(), any())).thenReturn(new DescribeClusterResult());
        assertNull(emrDao.getEmrClusterById(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getEmrClusterStatusByIdAssertReturnClusterState() throws Exception
    {
        String clusterId = "clusterId";
        ClusterState expectedState = ClusterState.BOOTSTRAPPING;

        when(mockEmrOperations.describeClusterRequest(any(), any())).then(new Answer<DescribeClusterResult>()
        {
            @Override
            public DescribeClusterResult answer(InvocationOnMock invocation) throws Throwable
            {
                DescribeClusterRequest describeClusterRequest = invocation.getArgument(1);
                assertEquals(clusterId, describeClusterRequest.getClusterId());

                DescribeClusterResult describeClusterResult = new DescribeClusterResult();
                Cluster cluster = new Cluster();
                ClusterStatus status = new ClusterStatus();
                status.setState(expectedState);
                cluster.setStatus(status);
                describeClusterResult.setCluster(cluster);
                return describeClusterResult;
            }
        });
        assertEquals(expectedState.toString(), emrDao.getEmrClusterStatusById(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getEmrClusterStatusByIdAssertReturnNullWhenClusterIsNull() throws Exception
    {
        String clusterId = "clusterId";

        when(mockEmrOperations.describeClusterRequest(any(), any())).then(new Answer<DescribeClusterResult>()
        {
            @Override
            public DescribeClusterResult answer(InvocationOnMock invocation) throws Throwable
            {
                DescribeClusterRequest describeClusterRequest = invocation.getArgument(1);
                assertEquals(clusterId, describeClusterRequest.getClusterId());

                return new DescribeClusterResult();
            }
        });
        assertNull(emrDao.getEmrClusterStatusById(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getActiveEmrClusterByNameAssertOnlyReturnClusterWithMatchingName() throws Exception
    {
        String clusterName = "clusterName";
        String expectedClusterId = "clusterId3";

        ListClustersResult listClustersResult = new ListClustersResult();
        listClustersResult.setClusters(new ArrayList<>());
        {
            ClusterSummary clusterSummary = new ClusterSummary();
            clusterSummary.setId("clusterId1");
            clusterSummary.setName("not_matching");
            listClustersResult.getClusters().add(clusterSummary);
        }
        {
            ClusterSummary clusterSummary = new ClusterSummary();
            clusterSummary.setId("clusterId2");
            clusterSummary.setName("");
            listClustersResult.getClusters().add(clusterSummary);
        }
        {
            ClusterSummary clusterSummary = new ClusterSummary();
            clusterSummary.setId(expectedClusterId);
            clusterSummary.setName(clusterName);
            listClustersResult.getClusters().add(clusterSummary);
        }
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(listClustersResult);

        ClusterSummary result = emrDao.getActiveEmrClusterByName(clusterName, new AwsParamsDto());
        assertNotNull(result);
        assertEquals(expectedClusterId, result.getId());
    }

    @Test
    public void getActiveEmrClusterByNameAssertUsesListMarker() throws Exception
    {
        String clusterName = "clusterName";
        String expectedClusterId = "clusterId";

        when(mockEmrOperations.listEmrClusters(any(), any())).then(new Answer<ListClustersResult>()
        {
            @Override
            public ListClustersResult answer(InvocationOnMock invocation) throws Throwable
            {
                ListClustersRequest listClustersRequest = invocation.getArgument(1);
                String marker = listClustersRequest.getMarker();

                ListClustersResult listClustersResult = new ListClustersResult();
                listClustersResult.setClusters(new ArrayList<>());

                /*
                 * When no marker is given, this is the request for the first page.
                 * Return a known marker. The expectation is that the next call to this method should have a request with this expected marker.
                 */
                if (marker == null)
                {
                    listClustersResult.setMarker("pagination_marker");
                }
                /*
                 * When a marker is given, this is expected to be the subsequent call.
                 */
                else
                {
                    // Assert that the correct marker is passed in
                    assertEquals("pagination_marker", marker);

                    ClusterSummary clusterSummary = new ClusterSummary();
                    clusterSummary.setId(expectedClusterId);
                    clusterSummary.setName(clusterName);
                    listClustersResult.getClusters().add(clusterSummary);
                }
                return listClustersResult;
            }
        });

        ClusterSummary result = emrDao.getActiveEmrClusterByName(clusterName, new AwsParamsDto());
        assertNotNull(result);
        assertEquals(expectedClusterId, result.getId());
    }

    @Test
    public void getActiveEmrClusterByNameAssertReturnNullWhenClusterNameIsBlank() throws Exception
    {
        String clusterName = "";
        when(mockEmrOperations.listEmrClusters(any(), any())).thenReturn(new ListClustersResult());

        assertNull(emrDao.getActiveEmrClusterByName(clusterName, new AwsParamsDto()));
    }

    @Test
    public void getClusterActiveStepAssertCallListStepsAndReturnStepSummary() throws Exception
    {
        String clusterId = "clusterId";
        StepSummary expectedStepSummary = new StepSummary();

        when(mockEmrOperations.listStepsRequest(any(), any())).then(new Answer<ListStepsResult>()
        {
            @Override
            public ListStepsResult answer(InvocationOnMock invocation) throws Throwable
            {
                ListStepsRequest listStepsRequest = invocation.getArgument(1);
                assertEquals(clusterId, listStepsRequest.getClusterId());
                assertEquals(1, listStepsRequest.getStepStates().size());
                assertEquals(StepState.RUNNING.toString(), listStepsRequest.getStepStates().get(0));

                ListStepsResult listStepsResult = new ListStepsResult();
                listStepsResult.setSteps(new ArrayList<>());
                listStepsResult.getSteps().add(expectedStepSummary);
                return listStepsResult;
            }
        });

        assertEquals(expectedStepSummary, emrDao.getClusterActiveStep(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getClusterActiveStepAssertReturnNullWhenStepListIsEmpty() throws Exception
    {
        String clusterId = "clusterId";

        when(mockEmrOperations.listStepsRequest(any(), any())).thenReturn(new ListStepsResult());

        assertNull(emrDao.getClusterActiveStep(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getClusterActiveStepAssertReturnFirstWhenStepListSizeGt1() throws Exception
    {
        String clusterId = "clusterId";
        StepSummary expectedStepSummary = new StepSummary();
        expectedStepSummary.setId("expected");

        ListStepsResult listStepsResult = new ListStepsResult();
        listStepsResult.setSteps(Arrays.asList(expectedStepSummary, new StepSummary()));
        when(mockEmrOperations.listStepsRequest(any(), any())).thenReturn(listStepsResult);

        assertEquals(expectedStepSummary, emrDao.getClusterActiveStep(clusterId, new AwsParamsDto()));
    }

    @Test
    public void getClusterStepAssertCallsDescribeStepAndReturnsStep() throws Exception
    {
        String clusterId = "clusterId";
        String stepId = "stepId";
        Step expectedStep = new Step();

        when(mockEmrOperations.describeStepRequest(any(), any())).then(new Answer<DescribeStepResult>()
        {
            @Override
            public DescribeStepResult answer(InvocationOnMock invocation) throws Throwable
            {
                DescribeStepRequest describeStepRequest = invocation.getArgument(1);
                assertEquals(clusterId, describeStepRequest.getClusterId());
                assertEquals(stepId, describeStepRequest.getStepId());

                DescribeStepResult describeStepResult = new DescribeStepResult();
                describeStepResult.setStep(expectedStep);
                return describeStepResult;
            }
        });

        assertEquals(expectedStep, emrDao.getClusterStep(clusterId, stepId, new AwsParamsDto()));
    }

    @Test
    public void getEmrClientAssertClientConfigurationSet() throws Exception
    {
        String httpProxyHost = "httpProxyHost";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = emrDao.getEmrClient(awsParamsDto);
        ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonElasticMapReduceClient, "clientConfiguration");
        assertNotNull(clientConfiguration);
        assertEquals(httpProxyHost, clientConfiguration.getProxyHost());
        assertEquals(httpProxyPort.intValue(), clientConfiguration.getProxyPort());
    }

    @Test
    public void getEmrClientAssertClientConfigurationNotSetWhenProxyHostIsBlank() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = emrDao.getEmrClient(awsParamsDto);
        ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonElasticMapReduceClient, "clientConfiguration");
        assertNotNull(clientConfiguration);
        assertNull(clientConfiguration.getProxyHost());
    }

    @Test
    public void getEmrClientAssertClientConfigurationNotSetWhenProxyPortIsNull() throws Exception
    {
        String httpProxyHost = "httpProxyHost";
        Integer httpProxyPort = null;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = emrDao.getEmrClient(awsParamsDto);
        ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonElasticMapReduceClient, "clientConfiguration");
        assertNotNull(clientConfiguration);
        assertNull(clientConfiguration.getProxyHost());
    }
}
