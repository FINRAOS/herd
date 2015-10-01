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
package org.finra.dm.service;

import org.finra.dm.model.dto.EmrClusterAlternateKeyDto;
import org.finra.dm.model.api.xml.EmrCluster;
import org.finra.dm.model.api.xml.EmrClusterCreateRequest;
import org.finra.dm.model.api.xml.EmrMasterSecurityGroup;
import org.finra.dm.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.dm.model.api.xml.OozieWorkflowJob;
import org.finra.dm.model.api.xml.RunOozieWorkflowRequest;

/**
 * The EMR service.
 */
public interface EmrService
{
    public EmrCluster getCluster(EmrClusterAlternateKeyDto alternateKey, String emrClusterId, String emrStepId, boolean verbose, boolean retrieveOozieJobs)
        throws Exception;

    public EmrCluster createCluster(EmrClusterCreateRequest request) throws Exception;

    public EmrCluster terminateCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection) throws Exception;

    public Object addStepToCluster(Object emrStepAddRequest) throws Exception;

    public EmrMasterSecurityGroup addSecurityGroupsToClusterMaster(EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest) throws Exception;

    public OozieWorkflowJob runOozieWorkflow(RunOozieWorkflowRequest request) throws Exception;

    /**
     * Retrieves an EMR cluster's oozie job by its ID. The EMR cluster must have been created through DM for the retrieval to be successful.
     * A verbose flag may be set to true to retrieve action details of the workflow.
     * 
     * @param namespace EMR cluster namespace
     * @param emrClusterDefinitionName EMR cluster definition name
     * @param emrClusterName EMR cluster name
     * @param oozieWorkflowJobId Oozie workflow job ID
     * @param verbose true to retrieve more details, false otherwise. Defaults to false.
     * @return Oozie workflow details
     * @throws Exception when an error occurs. Most user errors would be a runtime exception.
     */
    public OozieWorkflowJob getEmrOozieWorkflowJob(String namespace, String emrClusterDefinitionName, String emrClusterName, String oozieWorkflowJobId,
        Boolean verbose) throws Exception;
}
