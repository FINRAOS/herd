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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDevice;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetProvisioningSpecifications;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetStateChangeReason;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetStatus;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetTimeline;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeSpecification;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.model.api.xml.EmrClusterEbsBlockDevice;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetProvisioningSpecifications;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetStateChangeReason;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetStatus;
import org.finra.herd.model.api.xml.EmrClusterInstanceFleetTimeline;
import org.finra.herd.model.api.xml.EmrClusterInstanceTypeSpecification;
import org.finra.herd.model.api.xml.EmrClusterSpotProvisioningSpecification;
import org.finra.herd.model.api.xml.EmrClusterVolumeSpecification;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;

/**
 * This class tests functionality within the AwsHelper class.
 */
public class EmrHelperTest extends AbstractDaoTest
{
    @Autowired
    EmrHelper emrHelper;

    @Test
    public void testBuildEmrClusterName() throws Exception
    {
        String clusterName = emrHelper.buildEmrClusterName(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        assertEquals(NAMESPACE + "." + EMR_CLUSTER_DEFINITION_NAME + "." + EMR_CLUSTER_NAME, clusterName);
    }

    @Test
    public void testGetS3StagingLocation() throws Exception
    {
        String s3StagingLocation = emrHelper.getS3StagingLocation();

        assertNotNull("s3 staging location is null", s3StagingLocation);
    }

    @Test
    public void testIsActiveEmrState() throws Exception
    {
        boolean isActive = emrHelper.isActiveEmrState("RUNNING");

        assertTrue("not active", isActive);
    }

    @Test
    public void testEmrHadoopJarStepConfig() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, false);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigNoContinueOnError() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, null);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigContinueOnError() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, true);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigWithArguments() throws Exception
    {
        List<String> arguments = new ArrayList<>();
        arguments.add("arg1");

        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, arguments, false);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
        assertNotNull("arguments not found", stepConfig.getHadoopJarStep().getArgs());
    }

    @Test
    public void testGetActiveEmrClusterIdAssertReturnActualClusterIdWhenClusterIdSpecifiedAndClusterStateActiveAndNameMatch()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndNameMismatch()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";
            String actualEmrClusterName = "actualEmrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(actualEmrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String
                    .format("The cluster with ID \"%s\" does not match the expected name \"%s\". The actual name is \"%s\".", expectedEmrClusterId,
                        emrClusterName, actualEmrClusterName), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertReturnActualClusterIdWhenClusterStateActiveAndNameNotSpecified()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = null;
            String expectedEmrClusterId = "expectedEmrClusterId";
            String actualEmrClusterName = "actualEmrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(actualEmrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndClusterStateNotActive()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            ClusterState actualClusterState = ClusterState.TERMINATED;
            when(mockEmrDao.getEmrClusterById(any(), any()))
                .thenReturn(new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(actualClusterState)));

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with ID \"%s\" is not active. The cluster state must be in one of [STARTING, BOOTSTRAPPING, RUNNING, " +
                    "WAITING]. Current state is \"%s\"", emrClusterId, actualClusterState), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndClusterDoesNotExist()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(null);

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with ID \"%s\" does not exist.", emrClusterId), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertParametersTrimmed()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId,
                emrHelper.getActiveEmrClusterId(StringUtils.wrap(emrClusterId, BLANK_TEXT), StringUtils.wrap(emrClusterName, BLANK_TEXT), null));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertParametersCaseIgnored()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId,
                emrHelper.getActiveEmrClusterId(StringUtils.upperCase(emrClusterId), StringUtils.upperCase(emrClusterName), null));

            verify(mockEmrDao).getEmrClusterById(eq(StringUtils.upperCase(emrClusterId)), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdNoIdSpecifiedAssertReturnActualClusterId()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getActiveEmrClusterByName(any(), any())).thenReturn(new ClusterSummary().withId(expectedEmrClusterId).withName(emrClusterName));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null));

            verify(mockEmrDao).getActiveEmrClusterByName(eq(emrClusterName), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdNoIdSpecifiedAssertErrorWhenClusterDoesNotExist()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = "emrClusterName";

            when(mockEmrDao.getActiveEmrClusterByName(any(), any())).thenReturn(null);

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with name \"%s\" does not exist.", emrClusterName), e.getMessage());
            }

            verify(mockEmrDao).getActiveEmrClusterByName(eq(emrClusterName), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenBothIdAndNameNotSpecified()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = null;

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName, null);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("One of EMR cluster ID or EMR cluster name must be specified.", e.getMessage());
            }

            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testIsInstanceDefinitionsEmpty()
    {
        assertTrue(emrHelper.isInstanceDefinitionsEmpty(null));
        assertTrue(emrHelper.isInstanceDefinitionsEmpty(new InstanceDefinitions(null, null, null)));

        assertFalse(emrHelper.isInstanceDefinitionsEmpty(new InstanceDefinitions(new MasterInstanceDefinition(), null, null)));
        assertFalse(emrHelper.isInstanceDefinitionsEmpty(new InstanceDefinitions(null, new InstanceDefinition(), null)));
        assertFalse(emrHelper.isInstanceDefinitionsEmpty(new InstanceDefinitions(null, null, new InstanceDefinition())));
    }

    @Test
    public void testBuildEmrClusterInstanceFleetFromAwsResult()
    {
        ListInstanceFleetsResult listInstanceFleetsResult = null;
        assertNull(emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        listInstanceFleetsResult = new ListInstanceFleetsResult();
        assertNull(emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        String instanceId = "instance id";
        String instanceName = "instance name";
        String instanceFleetType = "instance fleet type";
        int targetOnDemandCapacity = 1;
        int targetSpotCapacity = 2;
        int provisionedOnDemandCapacity = 3;
        int provisionedSpotCapacity = 4;
        
        EmrClusterInstanceFleet expectedEmrInstanceFleet = new EmrClusterInstanceFleet();
        expectedEmrInstanceFleet.setId(instanceId);
        expectedEmrInstanceFleet.setName(instanceName);
        expectedEmrInstanceFleet.setInstanceFleetType(instanceFleetType);
        expectedEmrInstanceFleet.setTargetOnDemandCapacity(targetOnDemandCapacity);
        expectedEmrInstanceFleet.setTargetSpotCapacity(targetSpotCapacity);
        expectedEmrInstanceFleet.setProvisionedOnDemandCapacity(provisionedOnDemandCapacity);
        expectedEmrInstanceFleet.setProvisionedSpotCapacity(provisionedSpotCapacity);

        InstanceFleet instanceFleet = new InstanceFleet();
        instanceFleet.setId(instanceId);
        instanceFleet.setName(instanceName);
        instanceFleet.setInstanceFleetType(instanceFleetType);
        instanceFleet.setTargetOnDemandCapacity(targetOnDemandCapacity);
        instanceFleet.setTargetSpotCapacity(targetSpotCapacity);
        instanceFleet.setProvisionedSpotCapacity(provisionedSpotCapacity);
        instanceFleet.setProvisionedOnDemandCapacity(provisionedOnDemandCapacity);

        List<InstanceFleet> instanceFleets = new ArrayList<>();
        instanceFleets.add(null);
        instanceFleets.add(instanceFleet);
        listInstanceFleetsResult.setInstanceFleets(Arrays.asList(instanceFleet));
        
        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        EmrClusterInstanceFleetStatus emrClusterInstanceFleetStatus = new EmrClusterInstanceFleetStatus();
        String  emrClusterInstanceFleetStatus_State = "state 1";
        emrClusterInstanceFleetStatus.setState(emrClusterInstanceFleetStatus_State);
        expectedEmrInstanceFleet.setInstanceFleetStatus(emrClusterInstanceFleetStatus);

        InstanceFleetStatus instanceFleetStatus = new InstanceFleetStatus();
        instanceFleetStatus.setState(emrClusterInstanceFleetStatus_State);
        instanceFleet.setStatus(instanceFleetStatus);

        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        String emrClusterInstanceFleetStatus_StateChangeCode = "change code 1";
        String emrClusterInstanceFleetStatus_StateChangeMsg = "change msg 1";
        InstanceFleetStateChangeReason instanceFleetStateChangeReason = new InstanceFleetStateChangeReason();
        instanceFleetStateChangeReason.setCode(emrClusterInstanceFleetStatus_StateChangeCode);
        instanceFleetStateChangeReason.setMessage(emrClusterInstanceFleetStatus_StateChangeMsg);
        instanceFleetStatus.setStateChangeReason(instanceFleetStateChangeReason);
        InstanceFleetTimeline instanceFleetTimeline = new  InstanceFleetTimeline();
        java.util.Date now = Calendar.getInstance().getTime();
        instanceFleetTimeline.setCreationDateTime(now);
        instanceFleetTimeline.setReadyDateTime(now);
        instanceFleetTimeline.setEndDateTime(now);
        instanceFleetStatus.setTimeline(instanceFleetTimeline);

        EmrClusterInstanceFleetStateChangeReason emrClusterInstanceFleetStateChangeReason = new EmrClusterInstanceFleetStateChangeReason();
        emrClusterInstanceFleetStateChangeReason.setCode(emrClusterInstanceFleetStatus_StateChangeCode);
        emrClusterInstanceFleetStateChangeReason.setMessage(emrClusterInstanceFleetStatus_StateChangeMsg);
        emrClusterInstanceFleetStatus.setStateChangeReason(emrClusterInstanceFleetStateChangeReason);
        EmrClusterInstanceFleetTimeline emrClusterInstanceFleetTimeline = new EmrClusterInstanceFleetTimeline();
        emrClusterInstanceFleetTimeline.setCreationDateTime(HerdDateUtils.getXMLGregorianCalendarValue(now));
        emrClusterInstanceFleetTimeline.setReadyDateTime(HerdDateUtils.getXMLGregorianCalendarValue(now));
        emrClusterInstanceFleetTimeline.setEndDateTime(HerdDateUtils.getXMLGregorianCalendarValue(now));
        emrClusterInstanceFleetStatus.setTimeline(emrClusterInstanceFleetTimeline);

        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        String instanceType = "instance type 1";
        int weightedCapacity = 1;
        String bidPrice = "1.0";
        double bidPricePercent = 0.8;
        boolean ebsOptimized = true;
        InstanceTypeSpecification instanceTypeSpecification = new InstanceTypeSpecification();
        instanceTypeSpecification.setInstanceType(instanceType);
        instanceTypeSpecification.setWeightedCapacity(weightedCapacity);
        instanceTypeSpecification.setBidPrice(bidPrice);
        instanceTypeSpecification.setBidPriceAsPercentageOfOnDemandPrice(bidPricePercent);
        instanceTypeSpecification.setEbsOptimized(ebsOptimized);

        List<InstanceTypeSpecification> instanceTypeSpecifications = new ArrayList<>();
        instanceTypeSpecifications.add(null);
        instanceTypeSpecifications.add(instanceTypeSpecification);
        instanceFleet.setInstanceTypeSpecifications(instanceTypeSpecifications);

        EmrClusterInstanceTypeSpecification emrClusterInstanceTypeSpecification = new EmrClusterInstanceTypeSpecification();
        emrClusterInstanceTypeSpecification.setInstanceType(instanceType);
        emrClusterInstanceTypeSpecification.setWeightedCapacity(weightedCapacity);
        emrClusterInstanceTypeSpecification.setBidPrice(bidPrice);
        emrClusterInstanceTypeSpecification.setBidPriceAsPercentageOfOnDemandPrice(bidPricePercent);
        emrClusterInstanceTypeSpecification.setEbsOptimized(ebsOptimized);
        expectedEmrInstanceFleet.setInstanceTypeSpecifications(Arrays.asList(emrClusterInstanceTypeSpecification));
        
        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        EbsBlockDevice ebsBlockDevice = new EbsBlockDevice();
        String device = "device 1";
        ebsBlockDevice.setDevice(device);
        List<EbsBlockDevice> ebsBlockDevices = new ArrayList<>();
        ebsBlockDevices.add(ebsBlockDevice);
        ebsBlockDevices.add(null);
        instanceTypeSpecification.setEbsBlockDevices(ebsBlockDevices);
        EmrClusterEbsBlockDevice emrClusterEbsBlockDevice = new EmrClusterEbsBlockDevice();
        emrClusterEbsBlockDevice.setDevice(device);
        emrClusterInstanceTypeSpecification.setEbsBlockDevices(Arrays.asList(emrClusterEbsBlockDevice));
        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        int iops = 100;
        int sizeInGB = 20;
        String volumeType = "volume type 1";
        VolumeSpecification volumeSpecification = new VolumeSpecification();
        volumeSpecification.setIops(iops);
        volumeSpecification.setSizeInGB(sizeInGB);
        volumeSpecification.setVolumeType(volumeType);
        ebsBlockDevice.setVolumeSpecification(volumeSpecification);
        EmrClusterVolumeSpecification emrClusterVolumeSpecification = new EmrClusterVolumeSpecification();
        emrClusterVolumeSpecification.setIops(iops);
        emrClusterVolumeSpecification.setSizeInGB(sizeInGB);
        emrClusterVolumeSpecification.setVolumeType(volumeType);
        emrClusterEbsBlockDevice.setVolumeSpecification(emrClusterVolumeSpecification);
        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        InstanceFleetProvisioningSpecifications instanceFleetProvisioningSpecifications = new InstanceFleetProvisioningSpecifications();
        instanceFleet.setLaunchSpecifications(instanceFleetProvisioningSpecifications);
        EmrClusterInstanceFleetProvisioningSpecifications emrClusterInstanceFleetProvisioningSpecifications =
            new EmrClusterInstanceFleetProvisioningSpecifications();
        expectedEmrInstanceFleet.setLaunchSpecifications(emrClusterInstanceFleetProvisioningSpecifications);

        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

        int blockDurationMin = 30;
        String timeoutAction = "action 1";
        int timeoutDurationMin = 60;
        
        //to do time out action
        SpotProvisioningSpecification spotProvisioningSpecification = new SpotProvisioningSpecification();
        spotProvisioningSpecification.setBlockDurationMinutes(blockDurationMin);
        spotProvisioningSpecification.setTimeoutAction(timeoutAction);
        spotProvisioningSpecification.setTimeoutDurationMinutes(timeoutDurationMin);
        instanceFleetProvisioningSpecifications.setSpotSpecification(spotProvisioningSpecification);

        EmrClusterSpotProvisioningSpecification emrClusterSpotProvisioningSpecification = new EmrClusterSpotProvisioningSpecification();
        emrClusterSpotProvisioningSpecification.setBlockDurationMinutes(blockDurationMin);
        emrClusterSpotProvisioningSpecification.setTimeoutAction(timeoutAction);
        emrClusterSpotProvisioningSpecification.setTimeoutDurationMinutes(timeoutDurationMin);
        emrClusterInstanceFleetProvisioningSpecifications.setSpotSpecification(emrClusterSpotProvisioningSpecification);

        assertEquals(Arrays.asList(expectedEmrInstanceFleet), emrHelper.buildEmrClusterInstanceFleetFromAwsResult(listInstanceFleetsResult));

    }
}
