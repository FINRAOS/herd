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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.repository.Deployment;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This class tests basic functionality of Activiti.
 */
public class ActivitiTest extends AbstractServiceTest
{
    private static Logger logger = Logger.getLogger(ActivitiTest.class);

    @Autowired
    private RepositoryService repositoryService;

    @Autowired
    private RuntimeService runtimeService;

    @Test
    public void testDeploySubmitAndDeleteWorkflow() throws Exception
    {
        // Deploy test workflow.
        String deploymentId = deployWorkflow();

        // Submit a job.
        submitJob();

        // Delete the workflow.
        deleteWorkflow(deploymentId);
    }

    private String deployWorkflow() throws Exception
    {
        Deployment deployment = repositoryService.createDeployment().addClasspathResource(ACTIVITI_XML_DM_WORKFLOW).deploy();
        assertNotNull(deployment.getId());
        logger.info("Test deployment done with Id: " + deployment.getId());
        return deployment.getId();
    }

    private String submitJob() throws Exception
    {
        String processInstanceId = runtimeService.startProcessInstanceByKey(TEST_ACTIVITY_WORKFLOW_ID).getId();
        assertNotNull(processInstanceId);
        logger.info("Test job submitted with Id: " + processInstanceId);
        return processInstanceId;
    }

    private void deleteWorkflow(String deploymentId) throws Exception
    {
        repositoryService.deleteDeployment(deploymentId);

        List<String> deployResources = repositoryService.getDeploymentResourceNames(deploymentId);

        assertTrue(deployResources.isEmpty());
        logger.info("Deployment removed with Id: " + deploymentId);
    }
}
