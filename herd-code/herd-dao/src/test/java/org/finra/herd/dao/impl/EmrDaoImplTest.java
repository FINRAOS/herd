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

import static org.finra.herd.dao.config.DaoSpringModuleConfig.EMR_CLUSTER_CACHE_MAP_DEFAULT_AWS_ACCOUNT_ID_KEY;
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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
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
import com.amazonaws.services.elasticmapreduce.model.OnDemandCapacityReservationOptions;
import com.amazonaws.services.elasticmapreduce.model.OnDemandProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.SpotProvisioningSpecification;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionCapacityReservationOptions;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsBlockDeviceConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionEbsConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceTypeConfig;
import org.finra.herd.model.api.xml.EmrClusterDefinitionLaunchSpecifications;
import org.finra.herd.model.api.xml.EmrClusterDefinitionOnDemandSpecification;
import org.finra.herd.model.api.xml.EmrClusterDefinitionSpotSpecification;
import org.finra.herd.model.api.xml.EmrClusterDefinitionVolumeSpecification;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterCacheKey;
import org.finra.herd.model.dto.EmrClusterCacheTimestamps;
import org.finra.herd.model.dto.EmrParamsDto;

/**
 * This class tests functionality within the EMR DAO implementation.
 */
public class EmrDaoImplTest extends AbstractDaoTest
{
    @Mock
    private Map<String, Map<EmrClusterCacheKey, String>> emrClusterCacheMap;

    @Mock
    private Map<String, EmrClusterCacheTimestamps> emrClusterCacheTimestampsMap;

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

    @Captor
    private ArgumentCaptor<AmazonElasticMapReduceClient> amazonElasticMapReduceClientArgumentCaptor;

    @Captor
    private ArgumentCaptor<RunJobFlowRequest> runJobFlowRequestArgumentCaptor;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEmrClusterWithNscdBootstrapScript()
    {
        // Create an EMR parameters DTO.
        final EmrParamsDto emrParamsDto =
            new EmrParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1, S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        final InstanceDefinitions instanceDefinitions =
            new InstanceDefinitions(new MasterInstanceDefinition(), new InstanceDefinition(), new InstanceDefinition());
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Collections.emptyList());
        when(configurationHelper.getProperty(ConfigurationValue.EMR_NSCD_SCRIPT)).thenReturn(EMR_NSCD_SCRIPT);
        when(emrHelper.getS3StagingLocation(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME))
            .thenReturn(String.format("%s%s%s%s", S3_URL_PROTOCOL, S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME, S3_URL_PATH_DELIMITER, S3_STAGING_RESOURCE_BASE));
        when(configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER)).thenReturn(S3_URL_PATH_DELIMITER);
        when(configurationHelper.getProperty(ConfigurationValue.EMR_CONFIGURE_DAEMON)).thenReturn(EMR_CONFIGURE_DAEMON);
        List<Parameter> daemonConfigs = new ArrayList<>();
        Parameter daemonConfig = new Parameter();
        daemonConfig.setName(EMR_CLUSTER_DAEMON_CONFIG_NAME);
        daemonConfig.setValue(EMR_CLUSTER_DAEMON_CONFIG_VALUE);
        daemonConfigs.add(daemonConfig);

        emrClusterDefinition.setDaemonConfigurations(daemonConfigs);
        AmazonElasticMapReduce amazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(emrParamsDto.getAwsRegionName()).build();
        when(awsClientFactory.getEmrClient(emrParamsDto)).thenReturn(amazonElasticMapReduce);
        when(awsClientFactory.getEmrClient(emrParamsDto)).thenReturn(amazonElasticMapReduce);
        when(emrOperations.runEmrJobFlow(amazonElasticMapReduceClientArgumentCaptor.capture(), runJobFlowRequestArgumentCaptor.capture()))
            .thenReturn(EMR_CLUSTER_ID);

        // Create the cluster
        String clusterId = emrDaoImpl.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto);

        // Verifications
        RunJobFlowRequest runJobFlowRequest = runJobFlowRequestArgumentCaptor.getValue();
        assertEquals(clusterId, EMR_CLUSTER_ID);
        verify(configurationHelper).getProperty(ConfigurationValue.EMR_NSCD_SCRIPT);
        verify(emrHelper).getS3StagingLocation(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER);
        verify(configurationHelper).getProperty(ConfigurationValue.EMR_CONFIGURE_DAEMON);
        verify(awsClientFactory).getEmrClient(emrParamsDto);
        verify(emrOperations).runEmrJobFlow((AmazonElasticMapReduceClient) amazonElasticMapReduce, runJobFlowRequest);
        List<BootstrapActionConfig> bootstrapActionConfigs = runJobFlowRequest.getBootstrapActions();

        // There should be two bootstrap actions: NSCD script, and emr daemon config
        assertEquals(2, bootstrapActionConfigs.size());

        // Verify NSCD bootstrap action
        assertEquals(ConfigurationValue.EMR_NSCD_SCRIPT.getKey(), bootstrapActionConfigs.get(0).getName());
        assertEquals(String.format("%s%s%s%s%s%s", S3_URL_PROTOCOL, S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME, S3_URL_PATH_DELIMITER, S3_STAGING_RESOURCE_BASE,
            S3_URL_PATH_DELIMITER, EMR_NSCD_SCRIPT), bootstrapActionConfigs.get(0).getScriptBootstrapAction().getPath());

        // Verify EMR configure daemon bootstrap action
        assertEquals(ConfigurationValue.EMR_CONFIGURE_DAEMON.getKey(), bootstrapActionConfigs.get(1).getName());
        assertEquals(EMR_CONFIGURE_DAEMON, bootstrapActionConfigs.get(1).getScriptBootstrapAction().getPath());
        assertEquals(String.format("%s=%s", EMR_CLUSTER_DAEMON_CONFIG_NAME, EMR_CLUSTER_DAEMON_CONFIG_VALUE),
            bootstrapActionConfigs.get(1).getScriptBootstrapAction().getArgs().get(0));
    }

    @Test
    public void testCreateEmrClusterNoNscdBootstrapScript()
    {
        // Create an AWS parameters DTO.
        final EmrParamsDto emrParamsDto =
            new EmrParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1, NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        final InstanceDefinitions instanceDefinitions =
            new InstanceDefinitions(new MasterInstanceDefinition(), new InstanceDefinition(), new InstanceDefinition());
        emrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinition.setNodeTags(Collections.emptyList());

        AmazonElasticMapReduce amazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(emrParamsDto.getAwsRegionName()).build();
        when(awsClientFactory.getEmrClient(emrParamsDto)).thenReturn(amazonElasticMapReduce);
        when(emrOperations.runEmrJobFlow(amazonElasticMapReduceClientArgumentCaptor.capture(), runJobFlowRequestArgumentCaptor.capture()))
            .thenReturn(EMR_CLUSTER_ID);

        // Create the cluster without NSCD script configuration
        String clusterId = emrDaoImpl.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto);

        // Verifications
        assertEquals(clusterId, EMR_CLUSTER_ID);
        verify(configurationHelper).getProperty(ConfigurationValue.EMR_NSCD_SCRIPT);
        verify(awsClientFactory).getEmrClient(emrParamsDto);
        verify(emrOperations).runEmrJobFlow(any(), any());
        RunJobFlowRequest runJobFlowRequest = runJobFlowRequestArgumentCaptor.getValue();
        List<BootstrapActionConfig> bootstrapActionConfigs = runJobFlowRequest.getBootstrapActions();

        // There should be no bootstrap action
        assertTrue(bootstrapActionConfigs.isEmpty());
    }

    @Test
    public void testGetActiveEmrClusterByName()
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1);

        // Create a mock AmazonElasticMapReduceClient.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = mock(AmazonElasticMapReduceClient.class);

        // Create a list cluster request.
        ListClustersRequest listClustersRequest = new ListClustersRequest().withClusterStates(EMR_VALID_STATE);

        // Create a list cluster result with a non-matching cluster and a marker.
        ListClustersResult listClusterResultWithMarker =
            new ListClustersResult().withClusters(new ClusterSummary().withName(INVALID_VALUE).withId(EMR_CLUSTER_ID)).withMarker(MARKER);

        // Create a list cluster request with marker.
        ListClustersRequest listClustersRequestWithMarker = new ListClustersRequest().withClusterStates(EMR_VALID_STATE).withMarker(MARKER);

        // Create a cluster summary.
        ClusterSummary clusterSummary = new ClusterSummary().withName(EMR_CLUSTER_NAME).withId(EMR_CLUSTER_ID);

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
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, null, awsParamsDto);

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
    public void testGetActiveEmrClusterByNameWithFullReload()
    {
        // Create an EMR cluster cache timestamps dto
        LocalDateTime lastFullReload = LocalDateTime.now().minusMinutes(11);
        LocalDateTime lastDeltaUpdate = LocalDateTime.now().minusMinutes(1);
        EmrClusterCacheTimestamps emrClusterCacheTimestamps = new EmrClusterCacheTimestamps(lastFullReload, lastDeltaUpdate);

        // Call test with timestamps
        testGetActiveEmrClusterByNameWithTimestamps(emrClusterCacheTimestamps, null);
    }

    @Test
    public void testGetActiveEmrClusterByNameWithDeltaReload()
    {
        // Create an EMR cluster cache timestamps dto
        LocalDateTime lastFullReload = LocalDateTime.now().minusMinutes(9);
        LocalDateTime lastDeltaUpdate = LocalDateTime.now().minusMinutes(1);
        EmrClusterCacheTimestamps emrClusterCacheTimestamps = new EmrClusterCacheTimestamps(lastFullReload, lastDeltaUpdate);

        // Create a created after date
        Date createdAfter = Date.from(lastDeltaUpdate.minusMinutes(1).atZone(ZoneId.systemDefault()).toInstant());

        // Call test with timestamps
        testGetActiveEmrClusterByNameWithTimestamps(emrClusterCacheTimestamps, createdAfter);
    }

    private void testGetActiveEmrClusterByNameWithTimestamps(EmrClusterCacheTimestamps emrClusterCacheTimestamps, Date createdAfter)
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1);

        // Create a mock AmazonElasticMapReduceClient.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = mock(AmazonElasticMapReduceClient.class);

        // Create a list cluster request.
        ListClustersRequest listClustersRequest = new ListClustersRequest().withClusterStates(EMR_VALID_STATE).withCreatedAfter(createdAfter);

        // Create a list cluster result with a non-matching cluster and a marker.
        ListClustersResult listClusterResultWithMarker =
            new ListClustersResult().withClusters(new ClusterSummary().withName(INVALID_VALUE).withId(EMR_CLUSTER_ID)).withMarker(MARKER);

        // Create a list cluster request with marker.
        ListClustersRequest listClustersRequestWithMarker =
            new ListClustersRequest().withClusterStates(EMR_VALID_STATE).withMarker(MARKER).withCreatedAfter(createdAfter);

        // Create a cluster summary.
        ClusterSummary clusterSummary = new ClusterSummary().withName(EMR_CLUSTER_NAME).withId(EMR_CLUSTER_ID);

        // Create a list cluster result with the matching cluster.
        ListClustersResult listClusterResult = new ListClustersResult().withClusters(clusterSummary);


        // Mock the external calls.
        when(emrClusterCacheTimestampsMap.get(EMR_CLUSTER_CACHE_MAP_DEFAULT_AWS_ACCOUNT_ID_KEY)).thenReturn(emrClusterCacheTimestamps);
        when(configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES)).thenReturn(EMR_VALID_STATE);
        when(configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER))
            .thenReturn((String) ConfigurationValue.FIELD_DATA_DELIMITER.getDefaultValue());
        when(awsClientFactory.getEmrClient(awsParamsDto)).thenReturn(amazonElasticMapReduceClient);
        when(emrOperations.listEmrClusters(amazonElasticMapReduceClient, listClustersRequest)).thenReturn(listClusterResultWithMarker);
        when(emrOperations.listEmrClusters(amazonElasticMapReduceClient, listClustersRequestWithMarker)).thenReturn(listClusterResult);

        // Call the method under test.
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, null, awsParamsDto);

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
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1);

        // Call the method under test.
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByNameAndAccountId(BLANK_TEXT, null, awsParamsDto);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCache()
    {
        // Create a cluster with a valid state.
        Cluster cluster = new Cluster().withStatus(new ClusterStatus().withState("STARTING"));

        // Test the EMR Cluster Cache is used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(cluster, AWS_ACCOUNT_ID);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCacheWithNullAccountId()
    {
        // Create a cluster with a valid state.
        Cluster cluster = new Cluster().withStatus(new ClusterStatus().withState("STARTING"));

        // Test the EMR Cluster Cache is used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(cluster, null);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCacheWithInvalidState()
    {
        // Create a cluster with an invalid state.
        Cluster cluster = new Cluster().withStatus(new ClusterStatus().withState(EMR_INVALID_STATE));

        // Test that the EMR Cluster Cache is not used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(cluster, AWS_ACCOUNT_ID);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCacheWithInvalidStateWithNullAccountId()
    {
        // Create a cluster with an invalid state.
        Cluster cluster = new Cluster().withStatus(new ClusterStatus().withState(EMR_INVALID_STATE));

        // Test that the EMR Cluster Cache is not used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(cluster, null);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCacheWithNullCluster()
    {
        // Test that the EMR Cluster Cache is not used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(null, AWS_ACCOUNT_ID);
    }

    @Test
    public void testGetActiveEmrClusterByNameAndAccountIdWhenClusterNameIsInCacheWithNullClusterWithNullAccountId()
    {
        // Test that the EMR Cluster Cache is not used.
        getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(null, null);
    }

    private void getActiveEmrClusterByNameAndAccountIdClusterNameIsInCache(Cluster cluster, String accountId)
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1);

        // Create a mock AmazonElasticMapReduceClient.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = mock(AmazonElasticMapReduceClient.class);

        // Create a cluster summary.
        ClusterSummary clusterSummary =
            new ClusterSummary().withName(EMR_CLUSTER_NAME).withId(EMR_CLUSTER_ID).withStatus(cluster == null ? null : cluster.getStatus());

        // Create a list cluster result with the matching cluster.
        ListClustersResult listClusterResult = new ListClustersResult().withClusters(clusterSummary);

        // Create a describe cluster result.
        DescribeClusterResult describeClusterResult = new DescribeClusterResult().withCluster(cluster);

        // Create a describe cluster request.
        DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(EMR_CLUSTER_ID);

        // Build the EMR cluster cache key
        EmrClusterCacheKey emrClusterCacheKey = new EmrClusterCacheKey(EMR_CLUSTER_NAME.toUpperCase(), accountId);

        // Build the EMR cluster cache
        Map<EmrClusterCacheKey, String> emrClusterCache = new ConcurrentHashMap<>();
        emrClusterCache.put(emrClusterCacheKey, EMR_CLUSTER_ID);

        // Mock the external calls.
        if (accountId == null)
        {
            when(emrClusterCacheMap.get(EMR_CLUSTER_CACHE_MAP_DEFAULT_AWS_ACCOUNT_ID_KEY)).thenReturn(emrClusterCache);
        }
        else
        {
            when(emrClusterCacheMap.get(accountId)).thenReturn(emrClusterCache);
        }

        when(emrOperations.describeClusterRequest(eq(amazonElasticMapReduceClient), any(DescribeClusterRequest.class))).thenReturn(describeClusterResult);
        when(configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES)).thenReturn(ConfigurationValue.EMR_VALID_STATES.getDefaultValue().toString());
        when(configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER))
            .thenReturn((String) ConfigurationValue.FIELD_DATA_DELIMITER.getDefaultValue());
        when(awsClientFactory.getEmrClient(awsParamsDto)).thenReturn(amazonElasticMapReduceClient);
        when(emrOperations.listEmrClusters(any(AmazonElasticMapReduceClient.class), any(ListClustersRequest.class))).thenReturn(listClusterResult);

        // Call the method under test.
        ClusterSummary result = emrDaoImpl.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, accountId, awsParamsDto);

        // Verify the external calls.
        verify(emrOperations).describeClusterRequest(eq(amazonElasticMapReduceClient), eq(describeClusterRequest));

        if (cluster == null)
        {
            verify(configurationHelper).getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);
            verify(configurationHelper).getProperty(ConfigurationValue.EMR_VALID_STATES);
            verify(awsClientFactory, times(2)).getEmrClient(awsParamsDto);
            verify(emrOperations).listEmrClusters(eq(amazonElasticMapReduceClient), any(ListClustersRequest.class));
        }
        else if (cluster.getStatus().getState().equals(EMR_INVALID_STATE))
        {
            verify(configurationHelper, times(2)).getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);
            verify(configurationHelper, times(2)).getProperty(ConfigurationValue.EMR_VALID_STATES);
            verify(awsClientFactory, times(2)).getEmrClient(awsParamsDto);
            verify(emrOperations).listEmrClusters(eq(amazonElasticMapReduceClient), any(ListClustersRequest.class));
        }
        else
        {
            verify(configurationHelper).getProperty(ConfigurationValue.FIELD_DATA_DELIMITER);
            verify(configurationHelper).getProperty(ConfigurationValue.EMR_VALID_STATES);
            verify(awsClientFactory).getEmrClient(awsParamsDto);
        }

        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(clusterSummary, result);
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
        List<Configuration> result = emrDaoImpl.getConfigurations(Lists.newArrayList(emrClusterDefinitionConfiguration));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Lists.newArrayList(new Configuration().withClassification(classification).withConfigurations(expectedConfigurations).withProperties(null)),
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
        final EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification =
            new EmrClusterDefinitionSpotSpecification(TIMEOUT_DURATION_MINUTES, TIMEOUT_ACTION, BLOCK_DURATION_MINUTES, ALLOCATION_STRATEGY_1);
        final EmrClusterDefinitionCapacityReservationOptions emrClusterDefinitionCapacityReservationOptions =
            new EmrClusterDefinitionCapacityReservationOptions(CAPACITY_USAGE_STRATEGY_1, CAPACITY_PREFERENCE_1, CAPACITY_RESERVATION_RESOURCE_GROUP_ARN);
        final EmrClusterDefinitionOnDemandSpecification emrClusterDefinitionOnDemandSpecification =
            new EmrClusterDefinitionOnDemandSpecification(ALLOCATION_STRATEGY_2, emrClusterDefinitionCapacityReservationOptions);
        final EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications =
            new EmrClusterDefinitionLaunchSpecifications(emrClusterDefinitionSpotSpecification, emrClusterDefinitionOnDemandSpecification);
        final EmrClusterDefinitionInstanceFleet emrClusterDefinitionInstanceFleet =
            new EmrClusterDefinitionInstanceFleet(name, instanceFleetType, targetOnDemandCapacity, targetSpotCapacity, emrClusterDefinitionInstanceTypeConfigs,
                emrClusterDefinitionLaunchSpecifications);

        // Call the method under test.
        List<InstanceFleetConfig> result = emrDaoImpl.getInstanceFleets(Lists.newArrayList(emrClusterDefinitionInstanceFleet));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<InstanceTypeConfig> expectedInstanceTypeConfigs = null;
        assertEquals(Lists.newArrayList(
            new InstanceFleetConfig().withName(name).withInstanceFleetType(instanceFleetType).withTargetOnDemandCapacity(targetOnDemandCapacity)
                .withTargetSpotCapacity(targetSpotCapacity).withInstanceTypeConfigs(expectedInstanceTypeConfigs).withLaunchSpecifications(
                new InstanceFleetProvisioningSpecifications().withSpotSpecification(
                    new SpotProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_1).withBlockDurationMinutes(BLOCK_DURATION_MINUTES)
                        .withTimeoutAction(TIMEOUT_ACTION).withTimeoutDurationMinutes(TIMEOUT_DURATION_MINUTES)).withOnDemandSpecification(
                    new OnDemandProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_2).withCapacityReservationOptions(
                        new OnDemandCapacityReservationOptions()
                            .withUsageStrategy(CAPACITY_USAGE_STRATEGY_1)
                            .withCapacityReservationPreference(CAPACITY_PREFERENCE_1)
                            .withCapacityReservationResourceGroupArn(CAPACITY_RESERVATION_RESOURCE_GROUP_ARN)
                    )))), result);
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
    public void testGetInstanceFleetsWhenEmrClusterDefinitionInstanceFleetsListIsNull()
    {
        // Call the method under test.
        List<InstanceFleetConfig> result = emrDaoImpl.getInstanceFleets(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetInstanceGroupConfig()
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
        assertEquals(Lists.newArrayList(new InstanceGroupConfig(InstanceRoleType.MASTER, null, instanceCount)), result);
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
        List<InstanceTypeConfig> result = emrDaoImpl.getInstanceTypeConfigs(Lists.newArrayList(emrClusterDefinitionInstanceTypeConfig));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        final List<Configuration> expectedConfigurations = null;
        assertEquals(Lists.newArrayList(new InstanceTypeConfig().withInstanceType(instanceType).withWeightedCapacity(weightedCapacity).withBidPrice(bidPrice)
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
        final EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification =
            new EmrClusterDefinitionSpotSpecification(TIMEOUT_DURATION_MINUTES, TIMEOUT_ACTION, BLOCK_DURATION_MINUTES, ALLOCATION_STRATEGY_1);
        final EmrClusterDefinitionCapacityReservationOptions emrClusterDefinitionCapacityReservationOptions =
            new EmrClusterDefinitionCapacityReservationOptions(CAPACITY_USAGE_STRATEGY_1, CAPACITY_PREFERENCE_1, CAPACITY_RESERVATION_RESOURCE_GROUP_ARN);
        final EmrClusterDefinitionOnDemandSpecification emrClusterDefinitionOnDemandSpecification =
            new EmrClusterDefinitionOnDemandSpecification(ALLOCATION_STRATEGY_2, emrClusterDefinitionCapacityReservationOptions);
        final EmrClusterDefinitionLaunchSpecifications emrClusterDefinitionLaunchSpecifications =
            new EmrClusterDefinitionLaunchSpecifications(emrClusterDefinitionSpotSpecification, emrClusterDefinitionOnDemandSpecification);

        // Call the method under test.
        InstanceFleetProvisioningSpecifications result = emrDaoImpl.getLaunchSpecifications(emrClusterDefinitionLaunchSpecifications);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new InstanceFleetProvisioningSpecifications().withSpotSpecification(
            new SpotProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_1).withBlockDurationMinutes(BLOCK_DURATION_MINUTES)
                .withTimeoutAction(TIMEOUT_ACTION).withTimeoutDurationMinutes(TIMEOUT_DURATION_MINUTES)).withOnDemandSpecification(
            new OnDemandProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_2).withCapacityReservationOptions(
                new OnDemandCapacityReservationOptions()
                    .withUsageStrategy(CAPACITY_USAGE_STRATEGY_1)
                    .withCapacityReservationPreference(CAPACITY_PREFERENCE_1)
                    .withCapacityReservationResourceGroupArn(CAPACITY_RESERVATION_RESOURCE_GROUP_ARN)
            )), result);
    }

    @Test
    public void testGetLaunchSpecificationsWhenEmrClusterDefinitionSpotSpecificationIsNull()
    {
        // Call the method under test.
        InstanceFleetProvisioningSpecifications result = emrDaoImpl.getLaunchSpecifications(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetListInstanceFleetsResult()
    {
        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                NO_AWS_REGION_NAME);

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
        Map<String, String> result = emrDaoImpl.getMap(Lists.newArrayList(parameter));

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
        final EmrClusterDefinitionSpotSpecification emrClusterDefinitionSpotSpecification =
            new EmrClusterDefinitionSpotSpecification(TIMEOUT_DURATION_MINUTES, TIMEOUT_ACTION, BLOCK_DURATION_MINUTES, ALLOCATION_STRATEGY_1);

        // Call the method under test.
        SpotProvisioningSpecification result = emrDaoImpl.getSpotSpecification(emrClusterDefinitionSpotSpecification);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new SpotProvisioningSpecification().withTimeoutDurationMinutes(TIMEOUT_DURATION_MINUTES).withTimeoutAction(TIMEOUT_ACTION)
            .withBlockDurationMinutes(BLOCK_DURATION_MINUTES).withAllocationStrategy(ALLOCATION_STRATEGY_1), result);
    }

    @Test
    public void testGetSpotSpecificationWhenEmrClusterDefinitionSpotSpecificationIsNull()
    {
        // Call the method under test.
        SpotProvisioningSpecification result = emrDaoImpl.getSpotSpecification(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetOnDemandSpecification()
    {
        // Create objects required for testing.
        final EmrClusterDefinitionCapacityReservationOptions emrClusterDefinitionCapacityReservationOptions =
            new EmrClusterDefinitionCapacityReservationOptions(CAPACITY_USAGE_STRATEGY_1, CAPACITY_PREFERENCE_1, CAPACITY_RESERVATION_RESOURCE_GROUP_ARN);
        final EmrClusterDefinitionOnDemandSpecification emrClusterDefinitionOnDemandSpecification =
            new EmrClusterDefinitionOnDemandSpecification(ALLOCATION_STRATEGY_1, emrClusterDefinitionCapacityReservationOptions);

        // Call the method under test.
        OnDemandProvisioningSpecification result = emrDaoImpl.getOnDemandSpecification(emrClusterDefinitionOnDemandSpecification);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new OnDemandProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_1).withCapacityReservationOptions(
            new OnDemandCapacityReservationOptions()
                .withUsageStrategy(CAPACITY_USAGE_STRATEGY_1)
                .withCapacityReservationPreference(CAPACITY_PREFERENCE_1)
                .withCapacityReservationResourceGroupArn(CAPACITY_RESERVATION_RESOURCE_GROUP_ARN)),
            result);
    }

    @Test
    public void testGetOnDemandSpecificationWhenEmrClusterDefinitionOnDemandSpecificationIsNull()
    {
        // Call the method under test.
        OnDemandProvisioningSpecification result = emrDaoImpl.getOnDemandSpecification(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetOnDemandSpecificationWhenEmrClusterDefinitionCapacityReservationOptionsIsNull()
    {
        // Create objects required for testing.
        final EmrClusterDefinitionOnDemandSpecification emrClusterDefinitionOnDemandSpecification =
            new EmrClusterDefinitionOnDemandSpecification(ALLOCATION_STRATEGY_1, null);

        // Call the method under test.
        OnDemandProvisioningSpecification result = emrDaoImpl.getOnDemandSpecification(emrClusterDefinitionOnDemandSpecification);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new OnDemandProvisioningSpecification().withAllocationStrategy(ALLOCATION_STRATEGY_1).withCapacityReservationOptions(null), result);
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
