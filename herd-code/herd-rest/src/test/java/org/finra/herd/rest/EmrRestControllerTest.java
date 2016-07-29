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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.Assert;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrHadoopJarStep;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.herd.model.api.xml.EmrHiveStep;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrOozieStep;
import org.finra.herd.model.api.xml.EmrOozieStepAddRequest;
import org.finra.herd.model.api.xml.EmrPigStep;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;
import org.finra.herd.model.api.xml.EmrShellStep;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.api.xml.RunOozieWorkflowRequest;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.EmrService;

/**
 * This class tests various functionality within the EMR REST controller.
 */
public class EmrRestControllerTest extends AbstractRestTest
{
    private static class EqualsEmrClusterAlternateKeyDto extends ArgumentMatcher<EmrClusterAlternateKeyDto>
    {
        private String namespace;

        private String emrClusterDefinitionName;

        private String emrClusterName;

        private EqualsEmrClusterAlternateKeyDto(String namespace, String emrClusterDefinitionName, String emrClusterName)
        {
            this.namespace = namespace;
            this.emrClusterDefinitionName = emrClusterDefinitionName;
            this.emrClusterName = emrClusterName;
        }

        @Override
        public boolean matches(Object argument)
        {
            EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = (EmrClusterAlternateKeyDto) argument;
            return Objects.equal(namespace, emrClusterAlternateKeyDto.getNamespace()) &&
                Objects.equal(emrClusterDefinitionName, emrClusterAlternateKeyDto.getEmrClusterDefinitionName()) &&
                Objects.equal(emrClusterName, emrClusterAlternateKeyDto.getEmrClusterName());
        }
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testGetEmrCluster() throws Exception
    {
        emrRestController.getEmrCluster(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, "cluster_no_exist", null, null, false, false);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test
    public void testCreateEmrCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a cluster definition create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrCluster emrCluster = emrRestController.createEmrCluster(request);

        Assert.notNull(emrCluster);
    }

    @Test
    public void testTerminateEmrCluster() throws Exception
    {
        EmrRestController emrRestController = new EmrRestController();
        EmrService emrService = mock(EmrService.class);
        ReflectionTestUtils.setField(emrRestController, "emrService", emrService);

        String namespace = "namespace";
        String emrClusterDefinitionName = "emrClusterDefinitionName";
        String emrClusterName = "emrClusterName";
        boolean overrideTerminationProtection = false;
        String emrClusterId = "emrClusterId";

        EmrCluster emrCluster = new EmrCluster();
        when(emrService.terminateCluster(any(), anyBoolean(), any())).thenReturn(emrCluster);

        assertEquals(emrCluster,
            emrRestController.terminateEmrCluster(namespace, emrClusterDefinitionName, emrClusterName, overrideTerminationProtection, emrClusterId));

        verify(emrService).terminateCluster(argThat(new EqualsEmrClusterAlternateKeyDto(namespace, emrClusterDefinitionName, emrClusterName)),
            eq(overrideTerminationProtection), eq(emrClusterId));
        verifyNoMoreInteractions(emrService);
    }

    /**
     * This test is to get unit test coverage for the rest method. Real unit tests are covered in Service layer EmrServiceTest
     */
    @Test
    public void testAddEmrSteps() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
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

        hiveRequest.setNamespace(NAMESPACE);
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

        pigRequest.setNamespace(NAMESPACE);
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

        oozieRequest.setNamespace(NAMESPACE);
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

        hadoopJarRequest.setNamespace(NAMESPACE);
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
            new RunOozieWorkflowRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, "test_cluster", OOZIE_WORKFLOW_LOCATION, null, null);

        try
        {
            emrRestController.runOozieJobToEmrCluster(runOozieWorkflowRequest);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals("Namespace \"" + NAMESPACE + "\" doesn't exist.", ex.getMessage());
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
            emrRestController.getEmrOozieWorkflow(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, "test_oozieWorkflowJobId", false, null);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals("Namespace \"" + NAMESPACE + "\" doesn't exist.", ex.getMessage());
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
        request.setNamespace(NAMESPACE);
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
        request.setNamespace(NAMESPACE);
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
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(EMR_CLUSTER_NAME);

        List<String> groupIds = new ArrayList<>();
        groupIds.add("A_TEST_SECURITY_GROUP");

        request.setSecurityGroupIds(groupIds);

        return request;
    }
}