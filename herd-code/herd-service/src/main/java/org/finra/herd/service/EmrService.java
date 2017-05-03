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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * The EMR service.
 */
public interface EmrService
{
    /**
     * Adds security groups to the master node of an existing EMR Cluster.
     *
     * @param request the EMR master security group add request
     *
     * @return the added EMR master security groups
     * @throws Exception if there were any errors adding the security groups to the cluster master
     */
    public EmrMasterSecurityGroup addSecurityGroupsToClusterMaster(EmrMasterSecurityGroupAddRequest request) throws Exception;

    /**
     * Adds step to an existing EMR Cluster.
     * <p/>
     * There are four serializable objects supported currently. They are 1: ShellStep - For shell scripts 2: HiveStep - For hive scripts 3: HadoopJarStep - For
     * Custom Map Reduce Jar files and 4: PigStep - For Pig scripts.
     *
     * @param request the EMR steps add request
     *
     * @return the EMR steps add object with added steps
     * @throws Exception if there were any errors while adding a step to the cluster.
     */
    public Object addStepToCluster(Object request) throws Exception;

    /**
     * Creates a new EMR Cluster.
     *
     * @param request the EMR cluster create request
     *
     * @return the created EMR cluster object
     * @throws Exception if there were any errors while creating the cluster
     */
    public EmrCluster createCluster(EmrClusterCreateRequest request) throws Exception;

    /**
     * Gets details of an existing EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param emrClusterId the id of the cluster to get details
     * @param emrStepId the step id of the step to get details
     * @param verbose parameter for whether to return detailed information
     * @param accountId the optional AWS account that EMR cluster is running in
     * @param retrieveInstanceFleets parameter for whether to retrieve instance fleets
     *
     * @return the EMR Cluster object with details
     * @throws Exception if there were any errors
     */
    public EmrCluster getCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, String emrClusterId, String emrStepId, boolean verbose, String accountId,
        boolean retrieveInstanceFleets) throws Exception;

    /**
     * Terminates the EMR Cluster.
     *
     * @param emrClusterAlternateKeyDto the EMR cluster alternate key
     * @param overrideTerminationProtection parameter for whether to override termination protection
     * @param emrClusterId the id of the cluster
     * @param accountId the optional AWS account that EMR cluster is running in
     *
     * @return the terminated EMR cluster object
     * @throws Exception if there were any errors while terminating the cluster
     */
    public EmrCluster terminateCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId,
        String accountId) throws Exception;
}
