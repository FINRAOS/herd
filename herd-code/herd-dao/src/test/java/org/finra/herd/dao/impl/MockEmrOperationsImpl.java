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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterStateChangeReason;
import com.amazonaws.services.elasticmapreduce.model.ClusterStateChangeReasonCode;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.ClusterTimeline;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopStepConfig;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepState;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.model.api.xml.StatusChangeReason;
import org.finra.herd.model.api.xml.StatusTimeline;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Mock implementation of AWS EMR operations.
 */
public class MockEmrOperationsImpl implements EmrOperations
{
    public static final String MOCK_CLUSTER_NAME = "mock_cluster_name";

    public static final String MOCK_CLUSTER_NOT_PROVISIONED_NAME = "mock_cluster_not_provisioned_name";

    public static final String MOCK_EMR_MAKER = "mock_cluster_marker";

    public static final String MOCK_STEP_RUNNING_NAME = "mock_step_running_name";

    @Autowired
    protected ConfigurationHelper configurationHelper;

    // Created clusters
    private Map<String, MockEmrJobFlow> emrClusters = new HashMap<>();

    @Override
    public String runEmrJobFlow(AmazonElasticMapReduceClient emrClient, RunJobFlowRequest jobFlowRequest)
    {
        String clusterStatus = ClusterState.BOOTSTRAPPING.toString();

        StatusChangeReason reason = new StatusChangeReason(ClusterStateChangeReasonCode.USER_REQUEST.toString(), "Started " + clusterStatus);
        StatusTimeline timeline = new StatusTimeline();
        timeline.setCreationTime(HerdDateUtils.getXMLGregorianCalendarValue(new Date()));

        if (StringUtils.isNotBlank(jobFlowRequest.getAmiVersion()))
        {
            if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
            {
                AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
                throttlingException.setErrorCode("ThrottlingException");

                throw throttlingException;
            }
            else if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_BAD_REQUEST))
            {
                AmazonServiceException badRequestException = new AmazonServiceException(MockAwsOperationsHelper.AMAZON_BAD_REQUEST);
                badRequestException.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                throw badRequestException;
            }
            else if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_NOT_FOUND))
            {
                AmazonServiceException notFoundException = new AmazonServiceException(MockAwsOperationsHelper.AMAZON_NOT_FOUND);
                notFoundException.setStatusCode(HttpStatus.SC_NOT_FOUND);
                throw notFoundException;
            }
            else if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION))
            {
                throw new AmazonServiceException(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
            }
            else if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING))
            {
                clusterStatus = ClusterState.WAITING.toString();
            }
            else if (jobFlowRequest.getAmiVersion().equals(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_RUNNING))
            {
                clusterStatus = ClusterState.RUNNING.toString();
            }
        }


        return createNewCluster(jobFlowRequest, clusterStatus, reason, timeline).getJobFlowId();
    }

    /**
     * Add Job Flow Step to AmazonElasticMapReduceClient
     */
    @Override
    public List<String> addJobFlowStepsRequest(AmazonElasticMapReduceClient emrClient, AddJobFlowStepsRequest addJobFlowStepsRequest)
    {
        if (addJobFlowStepsRequest.getSteps() != null && addJobFlowStepsRequest.getSteps().get(0) != null)
        {
            StepConfig firstStep = addJobFlowStepsRequest.getSteps().get(0);

            if (firstStep.getName().equals(MockAwsOperationsHelper.AMAZON_BAD_REQUEST))
            {
                AmazonServiceException badRequestException = new AmazonServiceException(MockAwsOperationsHelper.AMAZON_BAD_REQUEST);
                badRequestException.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                throw badRequestException;
            }
            else if (firstStep.getName().equals(MockAwsOperationsHelper.AMAZON_NOT_FOUND))
            {
                AmazonServiceException notFoundException = new AmazonServiceException(MockAwsOperationsHelper.AMAZON_NOT_FOUND);
                notFoundException.setStatusCode(HttpStatus.SC_NOT_FOUND);
                throw notFoundException;
            }
            else if (firstStep.getName().equals(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION))
            {
                throw new AmazonServiceException(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
            }
        }

        MockEmrJobFlow cluster = getClusterById(addJobFlowStepsRequest.getJobFlowId());
        if (cluster == null)
        {
            throw new AmazonServiceException("No Cluster exists with jobFlowId: " + addJobFlowStepsRequest.getJobFlowId());
        }
        List<String> jobIds = new ArrayList<>();
        for (StepConfig step : addJobFlowStepsRequest.getSteps())
        {
            jobIds.add(addClusterStep(cluster.getJobFlowId(), step).getJobFlowId());
        }
        return jobIds;
    }

    @Override
    public DescribeClusterResult describeClusterRequest(AmazonElasticMapReduceClient emrClient, DescribeClusterRequest describeClusterRequest)
    {
        if (describeClusterRequest.getClusterId().equalsIgnoreCase(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        }

        MockEmrJobFlow cluster = getClusterById(describeClusterRequest.getClusterId());
        if (cluster != null)
        {
            return new DescribeClusterResult().withCluster(new Cluster().withId(cluster.getJobFlowId()).withName(cluster.getJobFlowName()).withStatus(
                new ClusterStatus().withState(cluster.getStatus()).withStateChangeReason(
                    new ClusterStateChangeReason().withCode(cluster.getStatusChangeReason().getCode())
                        .withMessage(cluster.getStatusChangeReason().getMessage())).withTimeline(new ClusterTimeline().withCreationDateTime(
                    cluster.getStatusTimeline().getCreationTime() != null ? cluster.getStatusTimeline().getCreationTime().toGregorianCalendar().getTime() :
                        null).withEndDateTime(
                    cluster.getStatusTimeline().getEndTime() != null ? cluster.getStatusTimeline().getEndTime().toGregorianCalendar().getTime() : null)
                    .withReadyDateTime(
                        cluster.getStatusTimeline().getReadyTime() != null ? cluster.getStatusTimeline().getReadyTime().toGregorianCalendar().getTime() :
                            null))));
        }
        else
        {
            return null;
        }
    }

    @Override
    public ListClustersResult listEmrClusters(AmazonElasticMapReduceClient emrClient, ListClustersRequest listClustersRequest)
    {
        List<ClusterSummary> clusterSummaryList = new ArrayList<>();
        for (MockEmrJobFlow cluster : emrClusters.values())
        {
            if (!listClustersRequest.getClusterStates().isEmpty() && listClustersRequest.getClusterStates().contains(cluster.getStatus()))
            {
                ClusterSummary clusterSummary = new ClusterSummary();
                clusterSummary.withId(cluster.getJobFlowId()).withName(cluster.getJobFlowName()).withStatus(new ClusterStatus().withState(cluster.getStatus())
                    .withStateChangeReason(new ClusterStateChangeReason().withCode(cluster.getStatusChangeReason().getCode())
                        .withMessage(cluster.getStatusChangeReason().getMessage())).withTimeline(new ClusterTimeline().withCreationDateTime(
                        cluster.getStatusTimeline().getCreationTime() != null ? cluster.getStatusTimeline().getCreationTime().toGregorianCalendar().getTime() :
                            null).withEndDateTime(
                        cluster.getStatusTimeline().getEndTime() != null ? cluster.getStatusTimeline().getEndTime().toGregorianCalendar().getTime() : null)
                        .withReadyDateTime(
                            cluster.getStatusTimeline().getReadyTime() != null ? cluster.getStatusTimeline().getReadyTime().toGregorianCalendar().getTime() :
                                null)));
                clusterSummaryList.add(clusterSummary);
            }
        }
        if (StringUtils.isBlank(listClustersRequest.getMarker()))
        {
            return new ListClustersResult().withClusters(clusterSummaryList).withMarker(MOCK_EMR_MAKER);
        }
        else
        {
            return new ListClustersResult().withClusters(clusterSummaryList);
        }
    }

    private MockEmrJobFlow getClusterById(String jobFlowId)
    {
        return emrClusters.get(jobFlowId);
    }

    private MockEmrJobFlow getClusterByName(String clusterName)
    {
        MockEmrJobFlow returnCluster = null;
        for (MockEmrJobFlow cluster : emrClusters.values())
        {
            if (cluster.getJobFlowName().equalsIgnoreCase(clusterName))
            {
                returnCluster = cluster;
            }
        }
        return returnCluster;
    }

    private MockEmrJobFlow createNewCluster(RunJobFlowRequest jobFlowRequest, String status, StatusChangeReason reason, StatusTimeline timeline)
    {
        MockEmrJobFlow cluster = new MockEmrJobFlow();
        cluster.setJobFlowId(getNewJobFlowId());
        cluster.setJobFlowName(jobFlowRequest.getName());
        cluster.setStatus(status);
        cluster.setStatusTimeline(timeline);
        cluster.setStatusChangeReason(reason);
        emrClusters.put(cluster.getJobFlowId(), cluster);

        // Add the steps
        for (StepConfig stepConfig : jobFlowRequest.getSteps())
        {
            addClusterStep(cluster.getJobFlowId(), stepConfig);
        }

        return cluster;
    }

    private MockEmrJobFlow addClusterStep(String jobFlowId, StepConfig step)
    {
        List<MockEmrJobFlow> mockSteps = getStepsByClusterId(jobFlowId);
        if (mockSteps == null)
        {
            mockSteps = new ArrayList<>();
        }

        MockEmrJobFlow mockStep = new MockEmrJobFlow();
        mockStep.setJobFlowId(getNewJobFlowId());
        mockStep.setJobFlowName(step.getName());
        if (step.getName().equalsIgnoreCase(MOCK_STEP_RUNNING_NAME))
        {
            mockStep.setStatus(StepState.RUNNING.toString());
        }
        else
        {
            mockStep.setStatus(StepState.PENDING.toString());
        }
        mockStep.setJarLocation(step.getHadoopJarStep().getJar());

        mockSteps.add(mockStep);
        setStepsByClusterId(jobFlowId, mockSteps);
        return mockStep;
    }

    private String getNewJobFlowId()
    {
        return "UT_JobFlowId" + String.format("-%.3f", Math.random());
    }

    private List<MockEmrJobFlow> getStepsByClusterId(String jobFlowId)
    {
        MockEmrJobFlow cluster = getClusterById(jobFlowId);
        if (cluster != null)
        {
            return cluster.getSteps();
        }

        return null;
    }

    private void setStepsByClusterId(String jobFlowId, List<MockEmrJobFlow> steps)
    {
        MockEmrJobFlow cluster = getClusterById(jobFlowId);
        if (cluster != null)
        {
            cluster.setSteps(steps);
        }
    }

    @Override
    public ListInstancesResult listClusterInstancesRequest(AmazonElasticMapReduceClient emrClient, ListInstancesRequest listInstancesRequest)
    {
        MockEmrJobFlow cluster =
            getClusterByName(buildEmrClusterName(AbstractDaoTest.NAMESPACE, AbstractDaoTest.EMR_CLUSTER_DEFINITION_NAME, MOCK_CLUSTER_NOT_PROVISIONED_NAME));

        if (cluster != null && listInstancesRequest.getClusterId().equals(cluster.getJobFlowId()))
        {
            return new ListInstancesResult();
        }
        Instance instance = new Instance().withEc2InstanceId("EC2_EMR_MASTER_INSTANCE").withPrivateIpAddress("INSTANCE_IP_ADDRESS");
        return new ListInstancesResult().withInstances(instance);
    }


    @Override
    public void terminateEmrCluster(AmazonElasticMapReduceClient emrClient, String clusterId, boolean overrideTerminationProtection)
    {
        MockEmrJobFlow cluster = getClusterById(clusterId);
        if (cluster.getJobFlowName().endsWith(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        }
        cluster.setStatus(ClusterState.TERMINATED.toString());
    }

    @Override
    public ListStepsResult listStepsRequest(AmazonElasticMapReduceClient emrClient, ListStepsRequest listStepsRequest)
    {
        MockEmrJobFlow cluster = getClusterById(listStepsRequest.getClusterId());

        if (cluster == null)
        {
            throw new AmazonServiceException("No cluster found with jobFlowId: " + listStepsRequest.getClusterId());
        }

        List<StepSummary> steps = new ArrayList<>();

        // Add steps that are in these states
        for (MockEmrJobFlow step : cluster.getSteps())
        {
            if ((listStepsRequest.getStepStates() == null || listStepsRequest.getStepStates().isEmpty()) ||
                listStepsRequest.getStepStates().contains(step.getStatus()))
            {
                StepSummary stepSummary =
                    new StepSummary().withId(step.getJobFlowId()).withName(step.getJobFlowName()).withStatus(new StepStatus().withState(step.getStatus()))
                        .withConfig(new HadoopStepConfig().withJar(step.getJarLocation()));
                steps.add(stepSummary);
            }
        }

        return new ListStepsResult().withSteps(steps);
    }

    @Override
    public DescribeStepResult describeStepRequest(AmazonElasticMapReduceClient emrClient, DescribeStepRequest describeStepRequest)
    {
        MockEmrJobFlow cluster = getClusterById(describeStepRequest.getClusterId());

        if (cluster == null)
        {
            throw new AmazonServiceException("No cluster found with jobFlowId: " + describeStepRequest.getClusterId());
        }

        Step stepResult = null;
        // Add steps that are in these states
        for (MockEmrJobFlow step : cluster.getSteps())
        {
            if (describeStepRequest.getStepId().equalsIgnoreCase(step.getJobFlowId()))
            {
                HadoopStepConfig hadoopStepConfig = new HadoopStepConfig().withJar(step.getJarLocation());
                stepResult = new Step().withId(step.getJobFlowId()).withName(step.getJobFlowName()).withStatus(new StepStatus().withState(step.getStatus()))
                    .withConfig(hadoopStepConfig);
                break;
            }
        }

        return new DescribeStepResult().withStep(stepResult);
    }

    private String buildEmrClusterName(String namespaceCd, String emrDefinitionName, String clusterName)
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

    @Override
    public ListInstanceFleetsResult listInstanceFleets(AmazonElasticMapReduceClient emrClient, ListInstanceFleetsRequest listInstanceFleetsRequest)
    {
        ListInstanceFleetsResult listInstanceFleetsResult = new ListInstanceFleetsResult();
        List<InstanceFleet> instanceFleets = new ArrayList<>();
        InstanceFleet instanceFleet = new InstanceFleet();
        instanceFleet.setId("mock_instance_id_1");
        instanceFleet.setName("mock_instance_name");
        instanceFleets.add(instanceFleet);
        listInstanceFleetsResult.setInstanceFleets(instanceFleets);
        return listInstanceFleetsResult;
    }
}
