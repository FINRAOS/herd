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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.repository.Deployment;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This class tests basic functionality of Activiti.
 */
public class ActivitiTest extends AbstractServiceTest
{
    @Autowired
    private RepositoryService repositoryService;

    @Autowired
    private RuntimeService runtimeService;

    @Test
    public void testDeploySubmitAndDeleteWorkflow()
    {
        // Deploy test workflow.
        String deploymentId = deployWorkflow();

        // Submit a job.
        submitJob();

        // Delete the workflow.
        deleteWorkflow(deploymentId);
    }

    private String deployWorkflow()
    {
        Deployment deployment = repositoryService.createDeployment().addClasspathResource(ACTIVITI_XML_HERD_WORKFLOW).deploy();
        assertNotNull(deployment.getId());
        return deployment.getId();
    }

    private String submitJob()
    {
        String processInstanceId = runtimeService.startProcessInstanceByKey(TEST_ACTIVITY_WORKFLOW_ID).getId();
        assertNotNull(processInstanceId);
        return processInstanceId;
    }

    private void deleteWorkflow(String deploymentId)
    {
        repositoryService.deleteDeployment(deploymentId);
        List<String> deployResources = repositoryService.getDeploymentResourceNames(deploymentId);
        assertTrue(deployResources.isEmpty());
    }
}
