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
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;

import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * A DAO for Amazon AWS EMR.
 */
public interface EmrDao
{

    /**
     * Add an EMR Step. This method adds the step to EMR cluster based on the input.
     *
     * @param clusterId EMR cluster ID.
     * @param emrStepConfig the EMR step config to be added.
     * @param awsParamsDto the proxy details.
     * <p/>
     * There are four serializable objects supported currently. They are 1: ShellStep - For shell scripts 2: HiveStep - For hive scripts 3: HadoopJarStep - For
     * Custom Map Reduce Jar files and 4: PigStep - For Pig scripts.
     *
     * @return the step id
     */
    public String addEmrStep(String clusterId, StepConfig emrStepConfig, AwsParamsDto awsParamsDto) throws Exception;

    /**
     * Create the EMR cluster.
     *
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     * @param emrClusterDefinition the EMR cluster definition that contains all the EMR parameters.
     * @param clusterName the cluster name value.
     *
     * @return the cluster Id.
     */
    public String createEmrCluster(String clusterName, EmrClusterDefinition emrClusterDefinition, AwsParamsDto awsParams);

    /**
     * Get an Active EMR cluster by the cluster name and account id. Cluster only in following states are returned: ClusterState.BOOTSTRAPPING,
     * ClusterState.RUNNING, ClusterState.STARTING, ClusterState.WAITING
     *
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     * @param clusterName the cluster name value.
     * @param accountId the account id in which the cluster resides.
     *
     * @return the ClusterSummary object.
     */
    public ClusterSummary getActiveEmrClusterByNameAndAccountId(String clusterName, String accountId, AwsParamsDto awsParams);

    /**
     * Gets the active step on the cluster if any.
     *
     * @param clusterId the cluster id.
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details.
     *
     * @return the step summary object.
     */
    public StepSummary getClusterActiveStep(String clusterId, AwsParamsDto awsParamsDto);

    /**
     * Gets the step on the cluster.
     *
     * @param clusterId the cluster id.
     * @param stepId the step id to get details of.
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details.
     *
     * @return the step object.
     */
    public Step getClusterStep(String clusterId, String stepId, AwsParamsDto awsParamsDto);

    /**
     * Create the EMR client with the given proxy and access key details.
     *
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details.
     *
     * @return the AmazonElasticMapReduceClient object.
     */
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto);

    /**
     * Get EMR cluster by cluster Id.
     *
     * @param clusterId the job Id returned by EMR for the cluster.
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     *
     * @return the cluster status.
     */
    public Cluster getEmrClusterById(String clusterId, AwsParamsDto awsParams);

    /**
     * Get EMR cluster status by cluster Id.
     *
     * @param clusterId the job Id returned by EMR for the cluster.
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     *
     * @return the cluster status.
     */
    public String getEmrClusterStatusById(String clusterId, AwsParamsDto awsParams);

    /**
     * Gets the master instance of the EMR cluster.
     *
     * @param clusterId EMR cluster id.
     * @param awsParams the proxy details.
     *
     * @return the master instance of the cluster.
     */
    public Instance getEmrMasterInstance(String clusterId, AwsParamsDto awsParams) throws Exception;

    /**
     * Terminates the EMR cluster.
     *
     * @param clusterId the cluster Id.
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     */
    public void terminateEmrCluster(String clusterId, boolean overrideTerminationProtection, AwsParamsDto awsParams);

    /**
     * Get the instance fleets
     *
     * @param clusterId the cluster Id.
     * @param awsParams AWS related parameters for access/secret keys and proxy details.
     *
     * @return list instance fleets result
     */
    public ListInstanceFleetsResult getListInstanceFleetsResult(String clusterId, AwsParamsDto awsParams);
}
