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
package org.finra.dm.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.api.xml.EmrCluster;
import org.finra.dm.model.api.xml.EmrClusterCreateRequest;
import org.finra.dm.model.api.xml.EmrHadoopJarStep;
import org.finra.dm.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.dm.model.api.xml.EmrHiveStep;
import org.finra.dm.model.api.xml.EmrHiveStepAddRequest;
import org.finra.dm.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.dm.model.api.xml.EmrOozieStep;
import org.finra.dm.model.api.xml.EmrOozieStepAddRequest;
import org.finra.dm.model.api.xml.EmrPigStep;
import org.finra.dm.model.api.xml.EmrPigStepAddRequest;
import org.finra.dm.model.api.xml.EmrShellStep;
import org.finra.dm.model.api.xml.EmrShellStepAddRequest;
import org.finra.dm.model.api.xml.OozieWorkflowJob;
import org.finra.dm.model.api.xml.RunOozieWorkflowRequest;
import org.junit.Test;
import org.springframework.util.Assert;

/**
 * This class tests various functionality within the EMR REST controller.
 */
public class EmrRestControllerTest extends AbstractRestTest
{
    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testGetEmrCluster() throws Exception
    {
        emrRestController.getEmrCluster(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, "cluster_no_exist", null, null, false, false);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test
    public void testCreateEmrCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a cluster definition create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrCluster emrCluster = emrRestController.createEmrCluster(request);

        Assert.notNull(emrCluster);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testTerminateEmrCluster() throws Exception
    {
        emrRestController.terminateEmrCluster(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, "cluster_no_exist", false);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test
    public void testAddEmrSteps() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE_CD);

        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a cluster definition create request
        EmrClusterCreateRequest clusterCreateRequest = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrRestController.createEmrCluster(clusterCreateRequest);

        Assert.notNull(emrCluster);
        Assert.notNull(emrCluster.getId());

        // Create a EmrShellStepAddRequest entry to pass it to RestController
        EmrShellStepAddRequest shellRequest = getNewEmrShellStepAddRequest();

        EmrShellStep emrShellStep = emrRestController.addShellStepToEmrCluster(shellRequest);

        Assert.notNull(emrShellStep);
        Assert.notNull(emrShellStep.getId());

        EmrCluster emrClusterStatus = emrRestController
            .getEmrCluster(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), emrCluster.getId(),
                emrShellStep.getId(), false, false);
        assertEquals(emrShellStep.getId(), emrClusterStatus.getStep().getId());


        // Create a EmrHiveStepAddRequest entry to pass it to RestController
        EmrHiveStepAddRequest hiveRequest = new EmrHiveStepAddRequest();

        hiveRequest.setNamespace(NAMESPACE_CD);
        hiveRequest.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        hiveRequest.setEmrClusterName(EMR_CLUSTER_NAME);

        hiveRequest.setStepName("A_HIVE_STEP");
        hiveRequest.setScriptLocation("SCRIPT_LOCATION");

        EmrHiveStep emrHiveStep = emrRestController.addHiveStepToEmrCluster(hiveRequest);

        Assert.notNull(emrHiveStep);
        Assert.notNull(emrHiveStep.getId());
        emrClusterStatus = emrRestController
            .getEmrCluster(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), emrCluster.getId(),
                emrHiveStep.getId(), false, false);
        assertEquals(emrHiveStep.getId(), emrClusterStatus.getStep().getId());

        // Create a EmrPigStepAddRequest entry to pass it to RestController
        EmrPigStepAddRequest pigRequest = new EmrPigStepAddRequest();

        pigRequest.setNamespace(NAMESPACE_CD);
        pigRequest.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        pigRequest.setEmrClusterName(EMR_CLUSTER_NAME);

        pigRequest.setStepName("A_HIVE_STEP");
        pigRequest.setScriptLocation("SCRIPT_LOCATION");

        EmrPigStep emrPigStep = emrRestController.addPigStepToEmrCluster(pigRequest);

        Assert.notNull(emrPigStep);
        Assert.notNull(emrPigStep.getId());

        emrClusterStatus = emrRestController
            .getEmrCluster(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), emrCluster.getId(),
                emrPigStep.getId(), false, false);
        assertEquals(emrPigStep.getId(), emrClusterStatus.getStep().getId());

        // Create a EmrOozieStepAddRequest entry to pass it to RestController
        EmrOozieStepAddRequest oozieRequest = new EmrOozieStepAddRequest();

        oozieRequest.setNamespace(NAMESPACE_CD);
        oozieRequest.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        oozieRequest.setEmrClusterName(EMR_CLUSTER_NAME);

        oozieRequest.setStepName("A_HIVE_STEP");
        oozieRequest.setWorkflowXmlLocation("WORKFLOW_LOCATION");
        oozieRequest.setOoziePropertiesFileLocation("SCRIPT_LOCATION");

        EmrOozieStep emrOozieStep = emrRestController.addOozieStepToEmrCluster(oozieRequest);

        Assert.notNull(emrOozieStep);
        Assert.notNull(emrOozieStep.getId());

        emrClusterStatus = emrRestController
            .getEmrCluster(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), emrCluster.getId(),
                emrOozieStep.getId(), false, false);
        assertEquals(emrOozieStep.getId(), emrClusterStatus.getStep().getId());

        // Create a EmrHadoopJarStepAddRequest entry to pass it to RestController
        EmrHadoopJarStepAddRequest hadoopJarRequest = new EmrHadoopJarStepAddRequest();

        hadoopJarRequest.setNamespace(NAMESPACE_CD);
        hadoopJarRequest.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        hadoopJarRequest.setEmrClusterName(EMR_CLUSTER_NAME);

        hadoopJarRequest.setStepName("A_HIVE_STEP");
        hadoopJarRequest.setJarLocation("JAR_LOCATION");

        EmrHadoopJarStep emrHadoopJarStep = emrRestController.addHadoopJarStepToEmrCluster(hadoopJarRequest);

        Assert.notNull(emrHadoopJarStep);
        Assert.notNull(emrHadoopJarStep.getId());

        emrClusterStatus = emrRestController
            .getEmrCluster(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), emrCluster.getId(),
                emrHadoopJarStep.getId(), false, false);
        assertEquals(emrHadoopJarStep.getId(), emrClusterStatus.getStep().getId());
    }

    /**
     * This test is to get unit test coverage for the rest method. Since it is calling the requires_new createCluster method, we expect an exception. Real unit
     * tests are covered in Service layer EmrServiceTest
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testAddSecurityGroupToMaster() throws Exception
    {
        // Create a EmrAddStepRequest entry to pass it to RestController
        EmrMasterSecurityGroupAddRequest request = getNewEmrAddSecurityGroupMasterRequest();

        emrRestController.addGroupsToEmrClusterMaster(request);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest.
     */
    @Test
    public void testRunOozieJob() throws Exception
    {
        // Create a RunOozieWorkflowRequest entry to pass it to RestController
        RunOozieWorkflowRequest runOozieWorkflowRequest =
            new RunOozieWorkflowRequest(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, "test_cluster", OOZIE_WORKFLOW_LOCATION, null);

        try
        {
            emrRestController.runOozieJobToEmrCluster(runOozieWorkflowRequest);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals("Namespace \"" + NAMESPACE_CD + "\" doesn't exist.", ex.getMessage());
        }
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest.
     */
    @Test
    public void testGetEmrOozieWorkflow() throws Exception
    {
        try
        {
            emrRestController.getEmrOozieWorkflow(NAMESPACE_CD, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, "test_oozieWorkflowJobId", false);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals("Namespace \"" + NAMESPACE_CD + "\" doesn't exist.", ex.getMessage());
        }
    }

    /**
     * This method fills-up the parameters required for the EMR cluster create request. This is called from all the other test methods.
     */
    private EmrClusterCreateRequest getNewEmrClusterCreateRequest() throws Exception
    {
        // Create a new ENR cluster create request.
        EmrClusterCreateRequest request = new EmrClusterCreateRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE_CD);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(EMR_CLUSTER_NAME);

        return request;
    }

    /**
     * This method creates a EMR Shell step add request. This is called from all the other test methods.
     */
    private EmrShellStepAddRequest getNewEmrShellStepAddRequest() throws Exception
    {
        // Create the EmrStepsAddRequest object
        EmrShellStepAddRequest request = new EmrShellStepAddRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE_CD);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(EMR_CLUSTER_NAME);

        request.setStepName("A_SHELL_STEP");
        request.setScriptLocation("SCRIPT_LOCATION");

        return request;
    }

    /**
     * This method fills-up the parameters required for the EMR add security group request. This is called from all the other test methods.
     */
    private EmrMasterSecurityGroupAddRequest getNewEmrAddSecurityGroupMasterRequest() throws Exception
    {
        // Create the EmrMasterSecurityGroupAddRequest object
        EmrMasterSecurityGroupAddRequest request = new EmrMasterSecurityGroupAddRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE_CD);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(EMR_CLUSTER_NAME);

        List<String> groupIds = new ArrayList<>();
        groupIds.add("A_TEST_SECURITY_GROUP");

        request.setSecurityGroupIds(groupIds);

        return request;
    }
}