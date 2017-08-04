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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetProvisioningSpecifications;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.Ec2Dao;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsBlockDeviceConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceTypeConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionLaunchSpecifications;
import org.finra.herd.model.api.xml.EmrClusterDefinitionSpotSpecification;
import org.finra.herd.model.api.xml.EmrClusterDefinitionVolumeSpecification;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the EMR DAO implementation.
 */
public class EmrDaoImplTest extends AbstractDaoTest
{
    @Mock
    private AwsClientFactory awsClientFactory;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private Ec2Dao ec2Dao;

    @InjectMocks
    private EmrDaoImpl emrDaoImpl;

    @Mock
    private EmrHelper emrHelper;

    @Mock
    private EmrOperations emrOperations;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetActiveEmrClusterByName()
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Create a mock AmazonElasticMapReduceClient.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = mock(AmazonElasticMapReduceClient.class);

        // Create a list cluster request.
        ListClustersRequest listClustersRequest = new ListClustersRequest().withClusterStates(EMR_VALID_STATE);

        // Create a list cluster result with a non-matching cluster and a marker.
        ListClustersResult listClusterResultWithMarker = new ListClustersResult().withClusters(new ClusterSummary().withName(INVALID_VALUE)).withMarker(MARKER);

        // Create a list cluster request with marker.
        ListClustersRequest listClustersRequestWithMarker = new ListClustersRequest().withClusterStates(EMR_VALID_STATE).withMarker(MARKER);

        // Create a cluster summary.
        ClusterSummary clusterSummary = new ClusterSummary().withName(EMR_CLUSTER_NAME);

        // Create a list cluster result with the matching cluster.
        ListClustersResult listClusterResult = new ListClustersResult().withClusters(clusterSummary);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES)).thenReturn(EMR_VALID_STATE);
        when(configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER))
            .thenReturn((String) ConfigurationValue.FIELD_DATA_DELIMITER.getDefaultValue());
        when(awsClientFactory.getEmrClient(awsParamsDto)).thenReturn(amazonElasticMapReduceClient);
        when(emrOperations.listEmrClusters(amazonElasticMapReduceClient, listClustersRequest)).thenReturn(listClusterResultWithMarker);
        when(emrOperations.listEmrClusters(amazonElasticMapReduceClient, listClustersRequestWithMarker)).thenReturn(listClusterResult);

        // Call the method under test.
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsParamsDto);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.EMR_VALID_STATES);
        verify(configurationHelper).getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);
        verify(awsClientFactory, times(2)).getEmrClient(awsParamsDto);
        verify(emrOperations, times(2)).listEmrClusters(eq(amazonElasticMapReduceClient), any(ListClustersRequest.class));
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(clusterSummary, result);
    }

    @Test
    public void testGetActiveEmrClusterByNameWhenClusterNameIsBlank()
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Call the method under test.
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByName(BLANK_TEXT, awsParamsDto);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetConfigurations()
    {
        // Create objects required for testing.
        final String classification = STRING_VALUE;
        final List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations = null;
        final List<Parameter> properties = null;
        final EmrClusterDefinitionConfiguration emrClusterDefinitionConfiguration =
            new EmrClusterDefinitionConfiguration(classification, emrClusterDefinitionConfigurations, properties);

        // Call the method under test.
        List<Configuration> result = emrDaoImpl.getConfigurations(Arrays.asList(emrClusterDefinitionConfiguration));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Arrays.asList(new Configuration().withClassification(classification).withConfigurations(expectedConfigurations).withProperties(null)),
            result);
    }

    @Test
    public void testGetConfigurationsWhenInputListContainsNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionConfigurations having a null element.
        List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations = new ArrayList<>();
        emrClusterDefinitionConfigurations.add(null);

        // Call the method under test.
        List<Configuration> result = emrDaoImpl.getConfigurations(emrClusterDefinitionConfigurations);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetConfigurationsWhenInputListIsEmpty()
    {
        // Call the method under test.
        List<Configuration> result = emrDaoImpl.getConfigurations(new ArrayList<>());

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetEbsBlockDeviceConfigs()
    {
        // Call the method under test.
        List<EbsBlockDeviceConfig> result = emrDaoImpl.getEbsBlockDeviceConfigs(Collections.singletonList(
            new EmrClusterDefinitionEbsBlockDeviceConfig(new EmrClusterDefinitionVolumeSpecification(VOLUME_TYPE, IOPS, SIZE_IN_GB), VOLUMES_PER_INSTANCE)));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(Collections.singletonList(
            new EbsBlockDeviceConfig().withVolumeSpecification(new VolumeSpecification().withVolumeType(VOLUME_TYPE).withIops(IOPS).withSizeInGB(SIZE_IN_GB))
                .withVolumesPerInstance(VOLUMES_PER_INSTANCE)), result);
    }

    @Test
    public void testGetEbsBlockDeviceConfigsWhenInputListContainsNullElement()
    {
        // Create a list of emrClusterDefinitionEbsBlockDeviceConfigs with a null element.
        List<EmrClusterDefinitionEbsBlockDeviceConfig> emrClusterDefinitionEbsBlockDeviceConfigs = new ArrayList<>();
        emrClusterDefinitionEbsBlockDeviceConfigs.add(null);

        // Call the method under test.
        List<EbsBlockDeviceConfig> result = emrDaoImpl.getEbsBlockDeviceConfigs(emrClusterDefinitionEbsBlockDeviceConfigs);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetEbsBlockDeviceConfigsWhenInputListIsEmpty()
    {
        // Call the method under test.
        List<EbsBlockDeviceConfig> result = emrDaoImpl.getEbsBlockDeviceConfigs(new ArrayList<>());

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetEbsConfiguration()
    {
        // Call the method under test.
        EbsConfiguration result = emrDaoImpl.getEbsConfiguration(new EmrClusterDefinitionEbsConfiguration(Collections.singletonList(
            new EmrClusterDefinitionEbsBlockDeviceConfig(new EmrClusterDefinitionVolumeSpecification(VOLUME_TYPE, IOPS, SIZE_IN_GB), VOLUMES_PER_INSTANCE)),
            EBS_OPTIMIZED));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new EbsConfiguration().withEbsBlockDeviceConfigs(
            new EbsBlockDeviceConfig().withVolumeSpecification(new VolumeSpecification().withVolumeType(VOLUME_TYPE).withIops(IOPS).withSizeInGB(SIZE_IN_GB))
                .withVolumesPerInstance(VOLUMES_PER_INSTANCE)).withEbsOptimized(EBS_OPTIMIZED), result);
    }

    @Test
    public void testGetInstanceFleets()
    {
        // Create objects required for testing.
        final String name = STRING_VALUE;
        final String instanceFleetType = STRING_VALUE_2;
        final Integer targetOnDemandCapacity = INTEGER_VALUE;
        final Integer targetSpotCapacity = INTEGER_VALUE_2;
        final List<EmrClusterDefinitionInstanceTypeConfig> emrClusterDefinitionInstanceTypeConfigs = null;
        final EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications = null;
        final EmrClusterDefinitionInstanceFleet emrClusterDefinitionInstanceFleet =
            new EmrClusterDefinitionInstanceFleet(name, instanceFleetType, targetOnDemandCapacity, targetSpotCapacity, emrClusterDefinitionInstanceTypeConfigs,
                emrClusterDefinitionLaunchSpecifications);

        // Call the method under test.
        List<InstanceFleetConfig> result = emrDaoImpl.getInstanceFleets(Arrays.asList(emrClusterDefinitionInstanceFleet));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<InstanceTypeConfig> expectedInstanceTypeConfigs = null;
        assertEquals(Arrays.asList(
            new InstanceFleetConfig().withName(name).withInstanceFleetType(instanceFleetType).withTargetOnDemandCapacity(targetOnDemandCapacity)
                .withTargetSpotCapacity(targetSpotCapacity).withInstanceTypeConfigs(expectedInstanceTypeConfigs).withLaunchSpecifications(null)), result);
    }

    @Test
    public void testGetInstanceFleetsWhenInputListContainsNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionInstanceFleets having a null element.
        List<EmrClusterDefinitionInstanceFleet> emrClusterDefinitionInstanceFleets = new ArrayList<>();
        emrClusterDefinitionInstanceFleets.add(null);

        // Call the method under test.
        List<InstanceFleetConfig> result = emrDaoImpl.getInstanceFleets(emrClusterDefinitionInstanceFleets);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetInstanceGroupConfig() throws Exception
    {
        // Call the method under test.
        InstanceGroupConfig result = emrDaoImpl.getInstanceGroupConfig(InstanceRoleType.MASTER, EC2_INSTANCE_TYPE, INSTANCE_COUNT, BID_PRICE,
            new EmrClusterDefinitionEbsConfiguration(Collections.singletonList(
                new EmrClusterDefinitionEbsBlockDeviceConfig(new EmrClusterDefinitionVolumeSpecification(VOLUME_TYPE, IOPS, SIZE_IN_GB), VOLUMES_PER_INSTANCE)),
                EBS_OPTIMIZED));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new InstanceGroupConfig(InstanceRoleType.MASTER, EC2_INSTANCE_TYPE, INSTANCE_COUNT).withMarket(MarketType.SPOT)
            .withBidPrice(BID_PRICE.toPlainString()).withEbsConfiguration(new EbsConfiguration().withEbsBlockDeviceConfigs(new EbsBlockDeviceConfig()
                .withVolumeSpecification(new VolumeSpecification().withVolumeType(VOLUME_TYPE).withIops(IOPS).withSizeInGB(SIZE_IN_GB))
                .withVolumesPerInstance(VOLUMES_PER_INSTANCE)).withEbsOptimized(EBS_OPTIMIZED)), result);
    }

    @Test
    public void testGetInstanceGroupConfigWhenBidPriceIsNull()
    {
        // Call the method under test.
        InstanceGroupConfig result = emrDaoImpl
            .getInstanceGroupConfig(InstanceRoleType.MASTER, EC2_INSTANCE_TYPE, INSTANCE_COUNT, NO_BID_PRICE, NO_EMR_CLUSTER_DEFINITION_EBS_CONFIGURATION);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new InstanceGroupConfig(InstanceRoleType.MASTER, EC2_INSTANCE_TYPE, INSTANCE_COUNT), result);
    }

    @Test
    public void testGetInstanceGroupConfigs()
    {
        // Create objects required for testing.
        final Integer instanceCount = 0;
        final InstanceDefinitions instanceDefinitions =
            new InstanceDefinitions(new MasterInstanceDefinition(), new InstanceDefinition(), new InstanceDefinition());

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(instanceDefinitions)).thenReturn(false);

        // Call the method under test.
        List<InstanceGroupConfig> result = emrDaoImpl.getInstanceGroupConfigs(instanceDefinitions);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(instanceDefinitions);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(3, CollectionUtils.size(result));
        assertTrue(result.contains(new InstanceGroupConfig(InstanceRoleType.MASTER, null, instanceCount)));
        assertTrue(result.contains(new InstanceGroupConfig(InstanceRoleType.CORE, null, instanceCount)));
        assertTrue(result.contains(new InstanceGroupConfig(InstanceRoleType.TASK, null, instanceCount)));
    }

    @Test
    public void testGetInstanceGroupConfigsMissingOptionalInstanceDefinitions()
    {
        // Create objects required for testing.
        final Integer instanceCount = 0;
        final InstanceDefinitions instanceDefinitions = new InstanceDefinitions(new MasterInstanceDefinition(), null, null);

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(instanceDefinitions)).thenReturn(false);

        // Call the method under test.
        List<InstanceGroupConfig> result = emrDaoImpl.getInstanceGroupConfigs(instanceDefinitions);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(instanceDefinitions);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(Arrays.asList(new InstanceGroupConfig(InstanceRoleType.MASTER, null, instanceCount)), result);
    }

    @Test
    public void testGetInstanceGroupConfigsWhenInstanceDefinitionsObjectIsEmpty()
    {
        // Create objects required for testing.
        final InstanceDefinitions instanceDefinitions = new InstanceDefinitions();

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(instanceDefinitions)).thenReturn(true);

        // Call the method under test.
        List<InstanceGroupConfig> result = emrDaoImpl.getInstanceGroupConfigs(instanceDefinitions);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(instanceDefinitions);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetInstanceTypeConfigs()
    {
        // Create objects required for testing.
        final String instanceType = STRING_VALUE;
        final Integer weightedCapacity = INTEGER_VALUE;
        final String bidPrice = STRING_VALUE_2;
        final Double bidPriceAsPercentageOfOnDemandPrice = DOUBLE_VALUE;
        final EmrClusterDefinitionEbsConfiguration emrClusterDefinitionEbsConfiguration = null;
        final List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations = null;
        final EmrClusterDefinitionInstanceTypeConfig emrClusterDefinitionInstanceTypeConfig =
            new EmrClusterDefinitionInstanceTypeConfig(instanceType, weightedCapacity, bidPrice, bidPriceAsPercentageOfOnDemandPrice,
                emrClusterDefinitionEbsConfiguration, emrClusterDefinitionConfigurations);

        // Call the method under test.
        List<InstanceTypeConfig> result = emrDaoImpl.getInstanceTypeConfigs(Arrays.asList(emrClusterDefinitionInstanceTypeConfig));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Arrays.asList(new InstanceTypeConfig().withInstanceType(instanceType).withWeightedCapacity(weightedCapacity).withBidPrice(bidPrice)
            .withBidPriceAsPercentageOfOnDemandPrice(bidPriceAsPercentageOfOnDemandPrice).withEbsConfiguration(null)
            .withConfigurations(expectedConfigurations)), result);
    }

    @Test
    public void testGetInstanceTypeConfigsWhenInputListContainsNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionInstanceTypeConfigs having a null element.
        List<EmrClusterDefinitionInstanceTypeConfig> emrClusterDefinitionInstanceTypeConfigs = new ArrayList<>();
        emrClusterDefinitionInstanceTypeConfigs.add(null);

        // Call the method under test.
        List<InstanceTypeConfig> result = emrDaoImpl.getInstanceTypeConfigs(emrClusterDefinitionInstanceTypeConfigs);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetLaunchSpecifications()
    {
        // Create objects required for testing.
        final EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification = null;
        final EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications =
            new EmrClusterDefinitionLaunchSpecifications(emrClusterDefinitionSpotSpecification);

        // Call the method under test.
        InstanceFleetProvisioningSpecifications result = emrDaoImpl.getLaunchSpecifications(emrClusterDefinitionLaunchSpecifications);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new InstanceFleetProvisioningSpecifications().withSpotSpecification(null), result);
    }

    @Test
    public void testGetListInstanceFleetsResult()
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Create a mock AmazonElasticMapReduceClient.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = mock(AmazonElasticMapReduceClient.class);

        // Create a list instance fleets request.
        ListInstanceFleetsRequest listInstanceFleetsRequest = new ListInstanceFleetsRequest().withClusterId(EMR_CLUSTER_ID);

        // Create a list instance fleets result.
        ListInstanceFleetsResult listInstanceFleetsResult = new ListInstanceFleetsResult().withMarker(MARKER);

        // Mock the external calls.
        when(awsClientFactory.getEmrClient(awsParamsDto)).thenReturn(amazonElasticMapReduceClient);
        when(emrOperations.listInstanceFleets(amazonElasticMapReduceClient, listInstanceFleetsRequest)).thenReturn(listInstanceFleetsResult);

        // Call the method under test.
        ListInstanceFleetsResult result = emrDaoImpl.getListInstanceFleetsResult(EMR_CLUSTER_ID, awsParamsDto);

        // Verify the external calls.
        verify(awsClientFactory).getEmrClient(awsParamsDto);
        verify(emrOperations).listInstanceFleets(amazonElasticMapReduceClient, listInstanceFleetsRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(listInstanceFleetsResult, result);
    }

    @Test
    public void testGetMap()
    {
        // Create objects required for testing.
        final String name = STRING_VALUE;
        final String value = STRING_VALUE_2;
        final Parameter parameter = new Parameter(name, value);

        // Call the method under test.
        Map<String, String> result = emrDaoImpl.getMap(Arrays.asList(parameter));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(1, CollectionUtils.size(result));
        assertTrue(result.containsKey(name));
        assertEquals(value, result.get(name));
    }

    @Test
    public void testGetMapWhenInputListContainsNullElement()
    {
        // Create objects required for testing with a list of parameters having a null element.
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(null);

        // Call the method under test.
        Map<String, String> result = emrDaoImpl.getMap(parameters);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testGetSpotSpecification()
    {
        // Create objects required for testing.
        final Integer timeoutDurationMinutes = INTEGER_VALUE;
        final String timeoutAction = STRING_VALUE;
        final Integer blockDurationMinutes = INTEGER_VALUE_2;
        final EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification1 =
            new EmrClusterDefinitionSpotSpecification(timeoutDurationMinutes, timeoutAction, blockDurationMinutes);

        // Call the method under test.
        SpotProvisioningSpecification result = emrDaoImpl.getSpotSpecification(emrClusterDefinitionSpotSpecification1);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new SpotProvisioningSpecification().withTimeoutDurationMinutes(timeoutDurationMinutes).withTimeoutAction(timeoutAction)
            .withBlockDurationMinutes(blockDurationMinutes), result);
    }

    @Test
    public void testGetVolumeSpecification()
    {
        // Call the method under test.
        VolumeSpecification result = emrDaoImpl.getVolumeSpecification(new EmrClusterDefinitionVolumeSpecification(VOLUME_TYPE, IOPS, SIZE_IN_GB));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new VolumeSpecification().withVolumeType(VOLUME_TYPE).withIops(IOPS).withSizeInGB(SIZE_IN_GB), result);
    }

    @Test
    public void testGetVolumeSpecificationWhenInputParameterIsNull()
    {
        // Call the method under test.
        VolumeSpecification result = emrDaoImpl.getVolumeSpecification(NO_EMR_CLUSTER_DEFINITION_VOLUME_SPECIFICATION);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsClientFactory, configurationHelper, ec2Dao, emrHelper, emrOperations, herdStringHelper, jsonHelper);
    }
}
