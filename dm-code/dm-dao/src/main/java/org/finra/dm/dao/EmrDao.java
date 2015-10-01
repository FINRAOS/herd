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
package org.finra.dm.dao;

import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.Instance;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.api.xml.EmrClusterDefinition;

/**
 * A DAO for Amazon AWS EMR.
 */
public interface EmrDao
{
    public String addEmrStep(String clusterName, StepConfig emrStepConfig, AwsParamsDto awsParamsDto) throws Exception;
    
    public List<String> addEmrMasterSecurityGroups(String clusterName, List<String> securityGroups, AwsParamsDto awsParams) throws Exception;
    
    public Instance getEmrMasterInstance(String clusterId, AwsParamsDto awsParams) throws Exception;
    
    public String createEmrCluster(String clusterName, EmrClusterDefinition emrClusterDefinition, AwsParamsDto awsParams);
    
    public String terminateEmrCluster(String clusterName, boolean overrideTerminationProtection, AwsParamsDto awsParams);
    
    public Cluster getEmrClusterById(String clusterId, AwsParamsDto awsParams);
    
    public String getEmrClusterStatusById(String clusterId, AwsParamsDto awsParams);
    
    public String getActiveEmrClusterIdByName(String clusterName, AwsParamsDto awsParams);
    
    public ClusterSummary getActiveEmrClusterByName(String clusterName, AwsParamsDto awsParams);
    
    public StepSummary getClusterActiveStep(String clusterId, AwsParamsDto awsParamsDto);
    
    public Step getClusterStep(String clusterId, String stepId, AwsParamsDto awsParamsDto);
    
    public AmazonElasticMapReduceClient getEmrClient(AwsParamsDto awsParamsDto);
}
