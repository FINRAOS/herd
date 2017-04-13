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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetProvisioningSpecifications;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
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
import org.finra.herd.model.api.xml.Parameter;

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
        verifyZeroInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Arrays.asList(new Configuration().withClassification(classification).withConfigurations(expectedConfigurations).withProperties(null)),
            result);
    }

    @Test
    public void testGetConfigurationsWhenEmrClusterDefinitionConfigurationListHasNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionConfigurations having a null element.
        List<EmrClusterDefinitionConfiguration> emrClusterDefinitionConfigurations = new ArrayList<>();
        emrClusterDefinitionConfigurations.add(null);

        // Call the method under test.
        List<Configuration> result = emrDaoImpl.getConfigurations(emrClusterDefinitionConfigurations);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetEbsBlockDeviceConfigs()
    {
        // Create objects required for testing.
        final EmrClusterDefinitionVolumeSpecification emrClusterDefinitionVolumeSpecification = null;
        final Integer volumesPerInstance = INTEGER_VALUE;
        final EmrClusterDefinitionEbsBlockDeviceConfig emrClusterDefinitionEbsBlockDeviceConfig =
            new EmrClusterDefinitionEbsBlockDeviceConfig(emrClusterDefinitionVolumeSpecification, volumesPerInstance);

        // Call the method under test.
        List<EbsBlockDeviceConfig> result = emrDaoImpl.getEbsBlockDeviceConfigs(Arrays.asList(emrClusterDefinitionEbsBlockDeviceConfig));

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(Arrays.asList(new EbsBlockDeviceConfig().withVolumeSpecification(null).withVolumesPerInstance(volumesPerInstance)), result);
    }

    @Test
    public void testGetEbsBlockDeviceConfigsWhenEmrClusterDefinitionEbsBlockDeviceConfigListHasNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionEbsBlockDeviceConfigs having a null element.
        List<EmrClusterDefinitionEbsBlockDeviceConfig> emrClusterDefinitionEbsBlockDeviceConfigs = new ArrayList<>();
        emrClusterDefinitionEbsBlockDeviceConfigs.add(null);

        // Call the method under test.
        List<EbsBlockDeviceConfig> result = emrDaoImpl.getEbsBlockDeviceConfigs(emrClusterDefinitionEbsBlockDeviceConfigs);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
    }

    @Test
    public void testGetEbsConfiguration()
    {
        // Create objects required for testing.
        final List<EmrClusterDefinitionEbsBlockDeviceConfig> emrClusterDefinitionEbsBlockDeviceConfigs = null;
        final Boolean ebsOptimized = true;
        final EmrClusterDefinitionEbsConfiguration emrClusterDefinitionEbsConfiguration =
            new EmrClusterDefinitionEbsConfiguration(emrClusterDefinitionEbsBlockDeviceConfigs, ebsOptimized);

        // Call the method under test.
        EbsConfiguration result = emrDaoImpl.getEbsConfiguration(emrClusterDefinitionEbsConfiguration);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        final List<EbsBlockDeviceConfig> expectedEbsBlockDeviceConfigs = null;
        assertEquals(new EbsConfiguration().withEbsBlockDeviceConfigs(expectedEbsBlockDeviceConfigs).withEbsOptimized(ebsOptimized), result);
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
        verifyZeroInteractionsHelper();

        // Validate the results.
        final List<InstanceTypeConfig> expectedInstanceTypeConfigs = null;
        assertEquals(Arrays.asList(
            new InstanceFleetConfig().withName(name).withInstanceFleetType(instanceFleetType).withTargetOnDemandCapacity(targetOnDemandCapacity)
                .withTargetSpotCapacity(targetSpotCapacity).withInstanceTypeConfigs(expectedInstanceTypeConfigs).withLaunchSpecifications(null)), result);
    }

    @Test
    public void testGetInstanceFleetsWhenEmrClusterDefinitionInstanceFleetListHasNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionInstanceFleets having a null element.
        List<EmrClusterDefinitionInstanceFleet> emrClusterDefinitionInstanceFleets = new ArrayList<>();
        emrClusterDefinitionInstanceFleets.add(null);

        // Call the method under test.
        List<InstanceFleetConfig> result = emrDaoImpl.getInstanceFleets(emrClusterDefinitionInstanceFleets);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new ArrayList<>(), result);
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
        verifyZeroInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Arrays.asList(new InstanceTypeConfig().withInstanceType(instanceType).withWeightedCapacity(weightedCapacity).withBidPrice(bidPrice)
            .withBidPriceAsPercentageOfOnDemandPrice(bidPriceAsPercentageOfOnDemandPrice).withEbsConfiguration(null)
            .withConfigurations(expectedConfigurations)), result);
    }

    @Test
    public void testGetInstanceTypeConfigsWhenEmrClusterDefinitionInstanceTypeConfigListHasNullElement()
    {
        // Create objects required for testing with a list of emrClusterDefinitionInstanceTypeConfigs having a null element.
        List<EmrClusterDefinitionInstanceTypeConfig> emrClusterDefinitionInstanceTypeConfigs = new ArrayList<>();
        emrClusterDefinitionInstanceTypeConfigs.add(null);

        // Call the method under test.
        List<InstanceTypeConfig> result = emrDaoImpl.getInstanceTypeConfigs(emrClusterDefinitionInstanceTypeConfigs);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

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
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new InstanceFleetProvisioningSpecifications().withSpotSpecification(null), result);
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
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(name));
        assertEquals(value, result.get(name));
    }

    @Test
    public void testGetMapWhenParameterListHasNullElement()
    {
        // Create objects required for testing with a list of parameters having a null element.
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(null);

        // Call the method under test.
        Map<String, String> result = emrDaoImpl.getMap(parameters);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

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
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new SpotProvisioningSpecification().withTimeoutDurationMinutes(timeoutDurationMinutes).withTimeoutAction(timeoutAction)
            .withBlockDurationMinutes(blockDurationMinutes), result);
    }

    @Test
    public void testGetVolumeSpecification()
    {
        // Create objects required for testing.
        final String volumeType = STRING_VALUE;
        final Integer iops = INTEGER_VALUE;
        final Integer sizeInGB = INTEGER_VALUE_2;
        final EmrClusterDefinitionVolumeSpecification emrClusterDefinitionVolumeSpecification =
            new EmrClusterDefinitionVolumeSpecification(volumeType, iops, sizeInGB);

        // Call the method under test.
        VolumeSpecification result = emrDaoImpl.getVolumeSpecification(emrClusterDefinitionVolumeSpecification);

        // Verify the external calls.
        verifyZeroInteractionsHelper();

        // Validate the results.
        assertEquals(new VolumeSpecification().withVolumeType(volumeType).withIops(iops).withSizeInGB(sizeInGB), result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyZeroInteractionsHelper()
    {
        verifyZeroInteractions(awsClientFactory, configurationHelper, ec2Dao, emrHelper, emrOperations, herdStringHelper, jsonHelper);
    }
}
