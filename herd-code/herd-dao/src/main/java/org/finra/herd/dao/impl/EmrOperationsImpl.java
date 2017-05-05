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

import java.util.List;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.SetTerminationProtectionRequest;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;

import org.finra.herd.dao.EmrOperations;

public class EmrOperationsImpl implements EmrOperations
{
    /**
     * Describe the cluster
     */
    @Override
    public DescribeClusterResult describeClusterRequest(AmazonElasticMapReduceClient emrClient, DescribeClusterRequest describeClusterRequest)
    {
        return emrClient.describeCluster(describeClusterRequest);
    }

    /**
     * List EMR cluster instances
     */
    @Override
    public ListInstancesResult listClusterInstancesRequest(AmazonElasticMapReduceClient emrClient, ListInstancesRequest listInstancesRequest)
    {
        return emrClient.listInstances(listInstancesRequest);
    }

    /**
     * Add Job Flow Step to AmazonElasticMapReduceClient
     */
    @Override
    public List<String> addJobFlowStepsRequest(AmazonElasticMapReduceClient emrClient, AddJobFlowStepsRequest addJobFlowStepsRequest)
    {
        return emrClient.addJobFlowSteps(addJobFlowStepsRequest).getStepIds();
    }

    /**
     * Run Job Flow to AmazonElasticMapReduceClient
     */
    @Override
    public String runEmrJobFlow(AmazonElasticMapReduceClient emrClient, RunJobFlowRequest jobFlowRequest)
    {
        return emrClient.runJobFlow(jobFlowRequest).getJobFlowId();
    }

    /**
     * List the EMR Clusters in the account
     */
    @Override
    public ListClustersResult listEmrClusters(AmazonElasticMapReduceClient emrClient, ListClustersRequest listClustersRequest)
    {
        return emrClient.listClusters(listClustersRequest);
    }

    /**
     * Terminate EMR cluster, overrides terminate protection if requested.
     */
    @Override
    public void terminateEmrCluster(AmazonElasticMapReduceClient emrClient, String clusterId, boolean overrideTerminationProtection)
    {
        // Override terminate protection if requested.
        if (overrideTerminationProtection)
        {
            // Set termination protection
            emrClient.setTerminationProtection(new SetTerminationProtectionRequest().withJobFlowIds(clusterId).withTerminationProtected(false));
        }

        // Terminate the job flow
        emrClient.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(clusterId));
    }

    /**
     * List the Steps on the cluster.
     */
    @Override
    public ListStepsResult listStepsRequest(AmazonElasticMapReduceClient emrClient, ListStepsRequest listStepsRequest)
    {
        return emrClient.listSteps(listStepsRequest);
    }

    /**
     * Describe the EMR cluster step.
     */
    @Override
    public DescribeStepResult describeStepRequest(AmazonElasticMapReduceClient emrClient, DescribeStepRequest describeStepRequest)
    {
        return emrClient.describeStep(describeStepRequest);
    }

    @Override
    public ListInstanceFleetsResult listInstanceFleets(AmazonElasticMapReduceClient emrClient, ListInstanceFleetsRequest listInstanceFleetsRequest)
    {
        return emrClient.listInstanceFleets(listInstanceFleetsRequest);
    }
}
