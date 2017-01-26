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
package org.finra.herd.service.impl;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.OozieWorkflowJob;
import org.finra.herd.model.api.xml.RunOozieWorkflowRequest;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * This is a EMR service implementation for test, to overwrite any behavior needed for test.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
@Primary
public class TestEmrServiceImpl extends EmrServiceImpl
{
    @Override
    public EmrCluster getCluster(EmrClusterAlternateKeyDto alternateKey, String emrClusterId, String emrStepId, boolean verbose, boolean retrieveOozieJobs, String accountId)
        throws Exception
    {
        return getClusterImpl(alternateKey, emrClusterId, emrStepId, verbose, retrieveOozieJobs, null);
    }

    @Override
    public EmrCluster createCluster(EmrClusterCreateRequest request) throws Exception
    {
        return createClusterImpl(request);
    }

    @Override
    public Object addStepToCluster(Object request) throws Exception
    {
        return addStepToClusterImpl(request);
    }

    @Override
    public EmrMasterSecurityGroup addSecurityGroupsToClusterMaster(EmrMasterSecurityGroupAddRequest request) throws Exception
    {
        return addSecurityGroupsToClusterMasterImpl(request);
    }

    @Override
    public OozieWorkflowJob runOozieWorkflow(RunOozieWorkflowRequest request) throws Exception
    {
        return runOozieWorkflowImpl(request);
    }

    @Override
    public EmrCluster terminateCluster(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, boolean overrideTerminationProtection, String emrClusterId, String accountId)
        throws Exception
    {
        return terminateClusterImpl(emrClusterAlternateKeyDto, overrideTerminationProtection, emrClusterId, null);
    }

    @Override
    public OozieWorkflowJob getEmrOozieWorkflowJob(String namespace, String emrClusterDefinitionName, String emrClusterName, String oozieWorkflowJobId,
        Boolean verbose, String emrClusterId, String accountId) throws Exception
    {
        return getEmrOozieWorkflowJobImpl(namespace, emrClusterDefinitionName, emrClusterName, oozieWorkflowJobId, verbose, emrClusterId, null);
    }
}
