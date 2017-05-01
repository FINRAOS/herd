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

/**
 * AWS EMR Operations Service.
 */
public interface EmrOperations
{
    public String runEmrJobFlow(AmazonElasticMapReduceClient emrClient, RunJobFlowRequest jobFlowRequest);

    public List<String> addJobFlowStepsRequest(AmazonElasticMapReduceClient emrClient, AddJobFlowStepsRequest addJobFlowStepsRequest);

    public DescribeClusterResult describeClusterRequest(AmazonElasticMapReduceClient emrClient, DescribeClusterRequest describeClusterRequest);

    public ListClustersResult listEmrClusters(AmazonElasticMapReduceClient emrClient, ListClustersRequest listClustersRequest);

    public ListInstancesResult listClusterInstancesRequest(AmazonElasticMapReduceClient emrClient, ListInstancesRequest listInstancesRequest);

    public void terminateEmrCluster(AmazonElasticMapReduceClient emrClient, String clusterId, boolean overrideTerminationProtection);

    public ListStepsResult listStepsRequest(AmazonElasticMapReduceClient emrClient, ListStepsRequest listStepsRequest);

    public DescribeStepResult describeStepRequest(AmazonElasticMapReduceClient emrClient, DescribeStepRequest describeStepRequest);

    public ListInstanceFleetsResult listInstanceFleets(AmazonElasticMapReduceClient emrClient, ListInstanceFleetsRequest listInstanceFleetsRequest);
}
