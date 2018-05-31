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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.xml.bind.JAXBException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.impl.MockAwsOperationsHelper;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.dao.impl.MockEmrOperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ConfigurationFiles;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKerberosAttributes;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrHadoopJarStep;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;
import org.finra.herd.model.api.xml.EmrShellStep;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.api.xml.HadoopJarStep;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.KeyValuePairConfigurations;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.ScriptDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity_;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.impl.EmrServiceImpl;

/**
 * This class tests functionality within the EmrService.
 */
public class EmrServiceTest extends AbstractServiceTest
{
    @Autowired
    private EmrOperations emrOperations;

    @Autowired
    @Qualifier(value = "emrServiceImpl")
    private EmrService emrServiceImpl;

    @Before
    public void createDatabaseEntities()
    {
        // Create EC2 on-demand pricing entities required for testing.
        ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntities();
    }

    /**
     * This method tests the happy path scenario for adding security groups
     */
    @Test
    public void testAddSecurityGroup() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest(request.getEmrClusterName());
        EmrMasterSecurityGroup emrMasterSecurityGroup = emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);

        // Validate the returned object against the input.
        assertNotNull(emrMasterSecurityGroup);
        assertTrue(emrMasterSecurityGroup.getNamespace().equals(request.getNamespace()));
        assertTrue(emrMasterSecurityGroup.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrMasterSecurityGroup.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    /**
     * This method tests the scenario where AmazonServiceException is thrown
     */
    @Test(expected = AmazonServiceException.class)
    public void testAddSecurityGroupAmazonException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest(request.getEmrClusterName());
        emrMasterSecurityGroupAddRequest.getSecurityGroupIds().clear();
        emrMasterSecurityGroupAddRequest.getSecurityGroupIds().add(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);
    }

    /**
     * This method tests the scenario where cluster does not exist
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAddSecurityGroupClusterDoesNotExist() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest("DOES_NOT_EXIST");
        emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);
    }

    /**
     * This method tests the scenario where EC2 instances have not been provisioned yet
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAddSecurityGroupEC2InstanceNotProvisioned() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        request.setEmrClusterName(MockEmrOperationsImpl.MOCK_CLUSTER_NOT_PROVISIONED_NAME);
        emrService.createCluster(request);

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest(request.getEmrClusterName());
        emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);
    }

    /**
     * This method tests the scenario when at least one security group must be specified
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAddSecurityGroupNoneSpecified() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest(request.getEmrClusterName());
        emrMasterSecurityGroupAddRequest.setSecurityGroupIds(null);
        emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testCreateEmrCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        assertEquals("scaleDownBehavior", expectedEmrClusterDefinition.getScaleDownBehavior());
        assertNotNull(expectedEmrClusterDefinition.getKerberosAttributes());
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNull(emrCluster.getAccountId());
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    /**
     * This method tests additionalInfo
     */
    @Test
    public void testCreateEmrClusterAdditionalInfo() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the additional Info
        emrClusterDefinition
            .setAdditionalInfo("{ami64: \"ami-e82af080\", amiHvm64: \"ami-e82af080\", hadoopVersion: \"2.4.0\", hadoopConfigurationVersion: \"3.1\"}");

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the error cases for AmazonExceptions for Illegal Argument
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateEmrClusterAmazonBadRequest() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_BAD_REQUEST);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the error cases for AmazonExceptions for Illegal Argument
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateEmrClusterAmazonObjectNotFound() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_NOT_FOUND);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the error cases for AmazonExceptions for AmazonServiceException
     */
    @Test(expected = AmazonServiceException.class)
    public void testCreateEmrClusterAmazonOtherException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the blank values for various parameters
     */
    @Test
    public void testCreateEmrClusterBlankParams() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        List<ScriptDefinition> scriptDefinitions = emrClusterDefinition.getCustomBootstrapActionAll();
        scriptDefinitions.get(0).setScriptArguments(null);
        emrClusterDefinition.setCustomBootstrapActionAll(scriptDefinitions);
        scriptDefinitions = emrClusterDefinition.getCustomBootstrapActionMaster();
        scriptDefinitions.get(0).setScriptArguments(null);
        emrClusterDefinition.setCustomBootstrapActionMaster(scriptDefinitions);
        emrClusterDefinition.setVisibleToAll(null);
        emrClusterDefinition.setServiceIamRole(null);
        emrClusterDefinition.setAmiVersion(null);
        emrClusterDefinition.setServiceIamRole(emrClusterDefinition.getEc2NodeIamProfileName());
        emrClusterDefinition.setScaleDownBehavior(null);
        emrClusterDefinition.setKerberosAttributes(null);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    @Test
    public void testCreateEmrClusterDryRunFalseNoOverride() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        // with dry run set to false
        request.setDryRun(false);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNotNull(emrCluster.isDryRun());
        assertFalse(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterDryRunTrueNoOverride() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        // with dry run set to true
        request.setDryRun(true);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNull(emrCluster.getId());
        assertNotNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isDryRun());
        assertFalse(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        assertEmrClusterCreationLogNotExist(emrCluster);
    }

    @Test
    public void testCreateEmrClusterDuplicate() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertFalse(emrCluster.isEmrClusterCreated());
        assertNull(emrCluster.getEmrClusterDefinition());

        // should throw if not unique
        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    /**
     * This method tests the error cases for Illegal Arguments for Instances not defined properly
     */
    @Test
    public void testCreateEmrClusterInstanceNotDefined() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create the test EMR cluster definition entity with missing instance definitions.
        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setInstanceDefinitions(null);
        configXml = xmlHelper.objectToXml(emrClusterDefinition);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Try to create a new EMR cluster using EMR cluster definition with missing both instance definitions and instance fleets.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        try
        {
            emrService.createCluster(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Instance group definitions or instance fleets must be specified.", e.getMessage());
        }
    }

    /**
     * This method tests the scenario in which the cluster definition is invalid ObjectNotFoundException is expected to be thrown
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateEmrClusterInvalidDefinition() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create the emr cluster request without registering the namespace entity - ${TEST_ACTIVITI_NAMESPACE_CD}
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        // Following method must throw ObjectNotFoundException, as the namespace entity ${TEST_ACTIVITI_NAMESPACE_CD} does not exist.
        emrService.createCluster(request);
    }

    /**
     * This method tests the scenario in which the namespaceCd is invalid ObjectNotFoundException is expected to be thrown
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateEmrClusterInvalidNamespace() throws Exception
    {
        // Create the emr cluster request without registering the namespace entity - ${TEST_ACTIVITI_NAMESPACE_CD}
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        // Following method must throw ObjectNotFoundException, as the namespace entity ${TEST_ACTIVITI_NAMESPACE_CD} does not exist.
        emrService.createCluster(request);
    }

    @Test
    public void testCreateEmrClusterInvalidParameters() throws Exception
    {
        // Try to perform a create when namespace contains a forward slash character.
        try
        {
            EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
            request.setNamespace(addSlash(request.getNamespace()));
            emrService.createCluster(request);
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create when EMR cluster definition name contains a forward slash character.
        try
        {
            EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
            request.setEmrClusterDefinitionName(addSlash(request.getEmrClusterDefinitionName()));
            emrService.createCluster(request);
            fail("Should throw an IllegalArgumentException when EMR cluster definition name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("EMR cluster definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create when EMR cluster name contains a forward slash character.
        try
        {
            EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
            request.setEmrClusterName(addSlash(request.getEmrClusterName()));
            emrService.createCluster(request);
            fail("Should throw an IllegalArgumentException when EMR cluster name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("EMR cluster name can not contain a forward slash character.", e.getMessage());
        }
    }

    /**
     * This method tests the scenario where task instances are there
     */
    @Test
    public void testCreateEmrClusterMandatoryTagsNull() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.MANDATORY_AWS_TAGS.getKey(), null);
        modifyPropertySourceInEnvironment(overrideMap);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));

        restorePropertySourceInEnvironment();
    }

    /**
     * This method tests the multiple bootstrap scripts.
     */
    @Test
    public void testCreateEmrClusterMultipleBootstrap() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        ScriptDefinition allScript = emrClusterDefinition.getCustomBootstrapActionAll().get(0);
        ScriptDefinition secondAllScript = new ScriptDefinition();
        secondAllScript.setScriptName(allScript.getScriptName() + "_second");
        secondAllScript.setScriptLocation(allScript.getScriptLocation());
        secondAllScript.setScriptArguments(allScript.getScriptArguments());
        emrClusterDefinition.getCustomBootstrapActionAll().add(secondAllScript);

        ScriptDefinition masterScript = emrClusterDefinition.getCustomBootstrapActionMaster().get(0);
        ScriptDefinition secondMasterScript = new ScriptDefinition();
        secondMasterScript.setScriptName(masterScript.getScriptName() + "_second");
        secondMasterScript.setScriptLocation(masterScript.getScriptLocation());
        secondMasterScript.setScriptArguments(masterScript.getScriptArguments());
        emrClusterDefinition.getCustomBootstrapActionMaster().add(secondMasterScript);

        emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, xmlHelper.objectToXml(emrClusterDefinition));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    @Test
    public void testCreateEmrClusterOverrideAllNull() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideExistingCoreInstanceTo0InstanceCountAssertSuccess() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceCount(1);
        emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, xmlHelper.objectToXml(emrClusterDefinition));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(emrClusterDefinition.getInstanceDefinitions());
        emrClusterDefinitionOverride.getInstanceDefinitions().getCoreInstances().setInstanceCount(0);
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrClusterCreateResponse = emrService.createCluster(request);
        assertNull(emrClusterCreateResponse.getEmrClusterDefinition().getInstanceDefinitions().getCoreInstances());
    }

    @Test
    public void testCreateEmrClusterOverrideExistingCoreInstanceToNegativeInstanceCountAssertException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceCount(1);
        emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, xmlHelper.objectToXml(emrClusterDefinition));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(emrClusterDefinition.getInstanceDefinitions());
        emrClusterDefinitionOverride.getInstanceDefinitions().getCoreInstances().setInstanceCount(-1);
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        try
        {
            emrService.createCluster(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least 0 core instance must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateEmrClusterOverrideExistingCoreInstanceToNullAssertSuccess() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinition.getInstanceDefinitions().getCoreInstances().setInstanceCount(1);
        emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, xmlHelper.objectToXml(emrClusterDefinition));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(emrClusterDefinition.getInstanceDefinitions());
        emrClusterDefinitionOverride.getInstanceDefinitions().setCoreInstances(null);
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrClusterCreateResponse = emrService.createCluster(request);
        assertNull(emrClusterCreateResponse.getEmrClusterDefinition().getInstanceDefinitions().getCoreInstances());
    }

    @Test
    public void testCreateEmrClusterOverrideHadoopConfigurations() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        ArrayList<Serializable> hadoopConfigurations = new ArrayList<>();
        hadoopConfigurations.add(new ConfigurationFiles());
        hadoopConfigurations.add(new KeyValuePairConfigurations());
        emrClusterDefinitionOverride.setHadoopConfigurations(hadoopConfigurations);
        expectedEmrClusterDefinition.setHadoopConfigurations(emrClusterDefinitionOverride.getHadoopConfigurations());
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideList() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setApplications(Collections.<EmrClusterDefinitionApplication>emptyList());
        expectedEmrClusterDefinition.setApplications(emrClusterDefinitionOverride.getApplications());
        emrClusterDefinitionOverride.setConfigurations(Collections.<EmrClusterDefinitionConfiguration>emptyList());
        expectedEmrClusterDefinition.setConfigurations(emrClusterDefinitionOverride.getConfigurations());
        emrClusterDefinitionOverride.setCustomBootstrapActionAll(Collections.<ScriptDefinition>emptyList());
        expectedEmrClusterDefinition.setCustomBootstrapActionAll(emrClusterDefinitionOverride.getCustomBootstrapActionAll());
        emrClusterDefinitionOverride.setCustomBootstrapActionMaster(Collections.<ScriptDefinition>emptyList());
        expectedEmrClusterDefinition.setCustomBootstrapActionMaster(emrClusterDefinitionOverride.getCustomBootstrapActionMaster());
        emrClusterDefinitionOverride.setDaemonConfigurations(Collections.<Parameter>emptyList());
        expectedEmrClusterDefinition.setDaemonConfigurations(emrClusterDefinitionOverride.getDaemonConfigurations());
        emrClusterDefinitionOverride.setHadoopConfigurations(Collections.<Serializable>emptyList());
        expectedEmrClusterDefinition.setHadoopConfigurations(emrClusterDefinitionOverride.getHadoopConfigurations());
        emrClusterDefinitionOverride.setHadoopJarSteps(Collections.<HadoopJarStep>emptyList());
        expectedEmrClusterDefinition.setHadoopJarSteps(emrClusterDefinitionOverride.getHadoopJarSteps());
        emrClusterDefinitionOverride.setAdditionalMasterSecurityGroups(Collections.emptyList());
        expectedEmrClusterDefinition.setAdditionalMasterSecurityGroups(emrClusterDefinitionOverride.getAdditionalMasterSecurityGroups());
        emrClusterDefinitionOverride.setAdditionalSlaveSecurityGroups(Collections.emptyList());
        expectedEmrClusterDefinition.setAdditionalSlaveSecurityGroups(emrClusterDefinitionOverride.getAdditionalSlaveSecurityGroups());
        List<NodeTag> nodeTags = expectedEmrClusterDefinition.getNodeTags();
        nodeTags.add(new NodeTag("testTag", "test"));
        emrClusterDefinitionOverride.setNodeTags(nodeTags);
        expectedEmrClusterDefinition.setNodeTags(nodeTags);
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideObject() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        int instanceCount = expectedEmrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceCount();
        expectedEmrClusterDefinition.getInstanceDefinitions().getMasterInstances().setInstanceCount(instanceCount + 1);

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(expectedEmrClusterDefinition.getInstanceDefinitions());
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideInstanceDefinitionsWithInstanceFleets() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request that replaces instance definitions with instance fleets.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(new InstanceDefinitions());
        emrClusterDefinitionOverride.setInstanceFleets(Arrays.asList(new EmrClusterDefinitionInstanceFleet()));
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        // Update the expected EMR cluster definition.
        expectedEmrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());
        expectedEmrClusterDefinition.setInstanceFleets(Arrays.asList(new EmrClusterDefinitionInstanceFleet()));

        // Create the cluster.
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideInstanceFleetsWithInstanceDefinitions() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create an EMR cluster definition that uses instance fleets instead of instance definitions.
        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        InstanceDefinitions instanceDefinitions = expectedEmrClusterDefinition.getInstanceDefinitions();
        expectedEmrClusterDefinition.setInstanceDefinitions(null);
        expectedEmrClusterDefinition.setInstanceFleets(Arrays.asList(new EmrClusterDefinitionInstanceFleet()));
        emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, xmlHelper.objectToXml(expectedEmrClusterDefinition));

        // Update the count of master instances in the instance definitions.
        instanceDefinitions.getMasterInstances().setInstanceCount(instanceDefinitions.getMasterInstances().getInstanceCount() + 1);

        // Create a new EMR cluster create request that replaces instance fleets with instance definitions.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setInstanceDefinitions(instanceDefinitions);
        emrClusterDefinitionOverride.setInstanceFleets(new ArrayList<>());
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        // Update the expected EMR cluster definition.
        expectedEmrClusterDefinition.setInstanceDefinitions(instanceDefinitions);
        expectedEmrClusterDefinition.setInstanceFleets(new ArrayList<>());

        // Create the cluster.
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterOverrideScalar() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setReleaseLabel("4.8.0." + Math.random());  // Security configuration is not supported prior to EMR version 4.8.0.
        expectedEmrClusterDefinition.setReleaseLabel(emrClusterDefinitionOverride.getReleaseLabel());
        emrClusterDefinitionOverride.setAdditionalInfo("test" + Math.random());
        expectedEmrClusterDefinition.setAdditionalInfo(emrClusterDefinitionOverride.getAdditionalInfo());
        emrClusterDefinitionOverride.setAmiVersion("test" + Math.random());
        expectedEmrClusterDefinition.setAmiVersion(emrClusterDefinitionOverride.getAmiVersion());
        emrClusterDefinitionOverride.setEc2NodeIamProfileName("test" + Math.random());
        expectedEmrClusterDefinition.setEc2NodeIamProfileName(emrClusterDefinitionOverride.getEc2NodeIamProfileName());
        emrClusterDefinitionOverride.setEncryptionEnabled(!expectedEmrClusterDefinition.isEncryptionEnabled());
        expectedEmrClusterDefinition.setEncryptionEnabled(emrClusterDefinitionOverride.isEncryptionEnabled());
        emrClusterDefinitionOverride.setHadoopVersion("test" + Math.random());
        expectedEmrClusterDefinition.setHadoopVersion(emrClusterDefinitionOverride.getHadoopVersion());
        emrClusterDefinitionOverride.setHiveVersion("test" + Math.random());
        expectedEmrClusterDefinition.setHiveVersion(emrClusterDefinitionOverride.getHiveVersion());
        emrClusterDefinitionOverride.setInstallOozie(!expectedEmrClusterDefinition.isInstallOozie());
        expectedEmrClusterDefinition.setInstallOozie(emrClusterDefinitionOverride.isInstallOozie());
        emrClusterDefinitionOverride.setKeepAlive(!expectedEmrClusterDefinition.isKeepAlive());
        expectedEmrClusterDefinition.setKeepAlive(emrClusterDefinitionOverride.isKeepAlive());
        emrClusterDefinitionOverride.setLogBucket("test" + Math.random());
        expectedEmrClusterDefinition.setLogBucket(emrClusterDefinitionOverride.getLogBucket());
        emrClusterDefinitionOverride.setPigVersion("test" + Math.random());
        expectedEmrClusterDefinition.setPigVersion(emrClusterDefinitionOverride.getPigVersion());
        emrClusterDefinitionOverride.setServiceIamRole("test" + Math.random());
        expectedEmrClusterDefinition.setServiceIamRole(emrClusterDefinitionOverride.getServiceIamRole());
        emrClusterDefinitionOverride.setSshKeyPairName("test" + Math.random());
        expectedEmrClusterDefinition.setSshKeyPairName(emrClusterDefinitionOverride.getSshKeyPairName());
        emrClusterDefinitionOverride.setSubnetId(MockEc2OperationsImpl.SUBNET_1);
        expectedEmrClusterDefinition.setSubnetId(emrClusterDefinitionOverride.getSubnetId());
        emrClusterDefinitionOverride.setSupportedProduct("test" + Math.random());
        expectedEmrClusterDefinition.setSupportedProduct(emrClusterDefinitionOverride.getSupportedProduct());
        emrClusterDefinitionOverride.setSecurityConfiguration("test" + Math.random());
        expectedEmrClusterDefinition.setSecurityConfiguration(emrClusterDefinitionOverride.getSecurityConfiguration());
        emrClusterDefinitionOverride.setScaleDownBehavior("test" + Math.random());
        expectedEmrClusterDefinition.setScaleDownBehavior(emrClusterDefinitionOverride.getScaleDownBehavior());
        emrClusterDefinitionOverride.setTerminationProtection(!expectedEmrClusterDefinition.isTerminationProtection());
        expectedEmrClusterDefinition.setTerminationProtection(emrClusterDefinitionOverride.isTerminationProtection());
        emrClusterDefinitionOverride.setVisibleToAll(!expectedEmrClusterDefinition.isVisibleToAll());
        expectedEmrClusterDefinition.setVisibleToAll(emrClusterDefinitionOverride.isVisibleToAll());
        emrClusterDefinitionOverride.setKerberosAttributes(
            new EmrClusterDefinitionKerberosAttributes("test" + Math.random(), "test" + Math.random(), "test" + Math.random(), "test" + Math.random(),
                "test" + Math.random()));
        expectedEmrClusterDefinition.setKerberosAttributes(emrClusterDefinitionOverride.getKerberosAttributes());
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    /**
     * This method tests security configuration.
     */
    @Test
    public void testCreateEmrClusterSecurityConfiguration() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the security configuration along with the EMR release label.
        // This is needed since security configuration is not supported prior to EMR version 4.8.0.
        emrClusterDefinition.setSecurityConfiguration("securityConfiguration");
        emrClusterDefinition.setReleaseLabel("4.8.0");
        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    @Test
    public void testCreateEmrClusterSecurityConfigurationInvalidReleaseLabel() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the security configuration without an EMR release label less than 4.8.0.
        emrClusterDefinition.setSecurityConfiguration("securityConfiguration");
        emrClusterDefinition.setReleaseLabel("4.7.9");
        configXml = xmlHelper.objectToXml(emrClusterDefinition);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Try to create a new EMR cluster.
        try
        {
            emrService.createCluster(getNewEmrClusterCreateRequest());
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("EMR security configuration is not supported prior to EMR release 4.8.0.", e.getMessage());
        }
    }

    @Test
    public void testCreateEmrClusterSecurityConfigurationNoReleaseLabel() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the security configuration without an EMR release label.
        emrClusterDefinition.setSecurityConfiguration("securityConfiguration");
        emrClusterDefinition.setReleaseLabel(BLANK_TEXT);
        configXml = xmlHelper.objectToXml(emrClusterDefinition);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Try to create a new EMR cluster.
        try
        {
            emrService.createCluster(getNewEmrClusterCreateRequest());
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("EMR security configuration is not supported prior to EMR release 4.8.0.", e.getMessage());
        }
    }

    @Test
    public void testCreateEmrClusterSecurityConfigurationReleaseLabelWithPrefix() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the security configuration along with the EMR release label starting with an "emr-" prefix.
        // This is needed since security configuration is not supported prior to EMR version 4.8.0.
        emrClusterDefinition.setSecurityConfiguration("securityConfiguration");
        emrClusterDefinition.setReleaseLabel("emr-4.8.0");
        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the service IAM role
     */
    @Test
    public void testCreateEmrClusterServiceRole() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setServiceIamRole(emrClusterDefinition.getEc2NodeIamProfileName());

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the cluster creation with the startup hadoop jar steps being added.
     */
    @Test
    public void testCreateEmrClusterStartupSteps() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);

        ListStepsRequest listStepsRequest = (new ListStepsRequest()).withClusterId(emrCluster.getId());

        ListStepsResult listStepsResult = emrOperations.listStepsRequest(null, listStepsRequest);
        List<StepSummary> addedSteps = listStepsResult.getSteps();
        // Validate that the step was added.
        for (HadoopJarStep hadoopJarStep : expectedEmrClusterDefinition.getHadoopJarSteps())
        {
            boolean stepFound = false;

            for (StepSummary stepSummary : addedSteps)
            {
                if (stepSummary.getName().equals(hadoopJarStep.getStepName()))
                {
                    // Step found
                    stepFound = true;
                    break;
                }
            }

            assertTrue(stepFound);
        }
    }

    /**
     * This method tests supported product
     */
    @Test
    public void testCreateEmrClusterSupportedProduct() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Set the supported product
        emrClusterDefinition.setSupportedProduct("mapr-m3");
        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the scenario where task instances are there
     */
    @Test
    public void testCreateEmrClusterWithTaskInstances() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        InstanceDefinition taskDef = new InstanceDefinition();
        taskDef.setInstanceCount(1);

        // This could be any EC2 instance type supported in EMR.
        taskDef.setInstanceType(MockEc2OperationsImpl.INSTANCE_TYPE_1);
        emrClusterDefinition.getInstanceDefinitions().setTaskInstances(taskDef);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    /**
     * This method tests the scenario where instance fleet configuration is specified
     */
    @Test
    public void testCreateEmrClusterWithInstanceFleets() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Retrieve the EMR cluster definition.
        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Update the EMR cluster definition to use instance fleets instead of instance group definitions.
        emrClusterDefinition.setInstanceFleets(Arrays.asList(new EmrClusterDefinitionInstanceFleet()));
        emrClusterDefinition.setInstanceDefinitions(null);

        // Create an entity for the the updated EMR cluster definition.
        configXml = xmlHelper.objectToXml(emrClusterDefinition);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    @Test
    public void testCreateEmrClusterWithSecurityGroups() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Retrieve the EMR cluster definition.
        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Update the EMR cluster definition to use instance fleets instead of instance group definitions.
        emrClusterDefinition.setMasterSecurityGroup(EMR_MASTER_SECURITY_GROUP);
        emrClusterDefinition.setSlaveSecurityGroup(EMR_SLAVE_SECURITY_GROUP);

        // Create an entity for the the updated EMR cluster definition.
        configXml = xmlHelper.objectToXml(emrClusterDefinition);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinition().getMasterSecurityGroup().equals(EMR_MASTER_SECURITY_GROUP));
        assertTrue(emrCluster.getEmrClusterDefinition().getSlaveSecurityGroup().equals(EMR_SLAVE_SECURITY_GROUP));
    }

    @Test
    public void testCreateEmrClusterOverrideSecurityGroups() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        expectedEmrClusterDefinition.setMasterSecurityGroup(EMR_MASTER_SECURITY_GROUP + "override");
        expectedEmrClusterDefinition.setSlaveSecurityGroup(EMR_SLAVE_SECURITY_GROUP + "override");
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setMasterSecurityGroup(EMR_MASTER_SECURITY_GROUP + "override");
        emrClusterDefinitionOverride.setSlaveSecurityGroup(EMR_SLAVE_SECURITY_GROUP + "override");
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);

        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    /**
     * This method tests some of the negative test cases
     */
    @Test
    public void testCreateEmrClusterWrongInstanceConfigs() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));

        // Calling again must return the same cluster id
        emrService.createCluster(request);
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testEmrAddSteps() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create the Add step request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        EmrShellStep emrShellStep = (EmrShellStep) emrService.addStepToCluster(stepRequest);

        // Validate the returned object against the input.
        assertNotNull(emrShellStep);
        assertTrue(emrShellStep.getNamespace().equals(request.getNamespace()));
        assertTrue(emrShellStep.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrShellStep.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    /**
     * This method tests the happy path scenario by testing all the step types
     */
    @Test
    public void testEmrAddStepsAllTypes() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        List<Object> emrSteps = new ArrayList<>();

        List<String> shellScriptArgs = new ArrayList<>();
        shellScriptArgs.add("Hello");
        shellScriptArgs.add("herd");
        shellScriptArgs.add("How Are You");


        EmrShellStepAddRequest shellStepRequest = new EmrShellStepAddRequest();
        shellStepRequest.setScriptLocation("s3://test-bucket-managed/app-a/test/test_script.sh");
        shellStepRequest.setStepName("Test Shell Script");
        shellStepRequest.setScriptArguments(shellScriptArgs);
        emrSteps.add(shellStepRequest);

        EmrHiveStepAddRequest hiveStepRequest = new EmrHiveStepAddRequest();
        List<String> scriptArgs1 = new ArrayList<>();
        scriptArgs1.add("arg2=sampleArg");
        scriptArgs1.add("arg1=tables");
        hiveStepRequest.setStepName("Test Hive");
        hiveStepRequest.setScriptLocation("s3://test-bucket-managed/app-a/test/test_hive.hql");
        hiveStepRequest.setScriptArguments(scriptArgs1);
        emrSteps.add(hiveStepRequest);

        EmrPigStepAddRequest pigStepRequest = new EmrPigStepAddRequest();
        pigStepRequest.setStepName("Test Pig");
        pigStepRequest.setScriptArguments(shellScriptArgs);
        pigStepRequest.setScriptLocation("s3://test-bucket-managed/app-a/test/test_pig.pig");
        emrSteps.add(pigStepRequest);

        shellStepRequest = new EmrShellStepAddRequest();
        shellStepRequest.setScriptLocation("s3://test-bucket-managed/app-a/bootstrap/install_oozie.sh");
        shellStepRequest.setStepName("Install Oozie");
        List<String> shellScriptArgsOozie = new ArrayList<>();
        shellScriptArgsOozie.add("s3://test-bucket-managed/app-a/bootstrap/oozie-4.0.1-distro.tar");
        shellStepRequest.setScriptArguments(shellScriptArgsOozie);

        emrSteps.add(shellStepRequest);

        EmrHadoopJarStepAddRequest hadoopJarStepRequest = new EmrHadoopJarStepAddRequest();
        List<String> scriptArgs2 = new ArrayList<>();
        scriptArgs2.add("oozie_run");
        scriptArgs2.add("wordcountOutput");
        hadoopJarStepRequest.setStepName("Hadoop Jar");
        hadoopJarStepRequest.setJarLocation("s3://test-bucket-managed/app-a/test/hadoop-mapreduce-examples-2.4.0.jar");
        hadoopJarStepRequest.setMainClass("wordcount");
        hadoopJarStepRequest.setScriptArguments(scriptArgs2);
        emrSteps.add(hadoopJarStepRequest);

        EmrStepHelper stepHelper;

        for (Object emrStepAddRequest : emrSteps)
        {
            stepHelper = emrStepHelperFactory.getStepHelper(emrStepAddRequest.getClass().getName());
            stepHelper.setRequestNamespace(emrStepAddRequest, NAMESPACE);
            stepHelper.setRequestEmrClusterDefinitionName(emrStepAddRequest, EMR_CLUSTER_DEFINITION_NAME);
            stepHelper.setRequestEmrClusterName(emrStepAddRequest, request.getEmrClusterName());

            Object emrStep = emrService.addStepToCluster(emrStepAddRequest);

            assertNotNull(emrStep);
            assertNotNull(stepHelper.getStepId(emrStep));

            Method getNameMethod = emrStep.getClass().getMethod("getStepName");
            String emrStepName = (String) getNameMethod.invoke(emrStep);

            assertEquals(stepHelper.getRequestStepName(emrStepAddRequest), emrStepName);

            Method isContinueOnErrorMethod = emrStep.getClass().getMethod("isContinueOnError");
            Object emrStepIsContinueOnError = isContinueOnErrorMethod.invoke(emrStep);

            assertEquals(stepHelper.isRequestContinueOnError(emrStepAddRequest), emrStepIsContinueOnError);
        }
    }

    /**
     * This method tests the Amazon BAD_REQUEST exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmrAddStepsAmazonBadRequest() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create a new EMR cluster create request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        stepRequest.setStepName(MockAwsOperationsHelper.AMAZON_BAD_REQUEST);
        emrService.addStepToCluster(stepRequest);
    }

    /**
     * This method tests the error cases for AmazonExceptions for Illegal Argument
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testEmrAddStepsAmazonObjectNotFound() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create a new EMR cluster create request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        stepRequest.setStepName(MockAwsOperationsHelper.AMAZON_NOT_FOUND);
        emrService.addStepToCluster(stepRequest);
    }

    /**
     * This method tests the Amazon exceptions
     */
    @Test(expected = AmazonServiceException.class)
    public void testEmrAddStepsAmazonOtherException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create a new EMR cluster create request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        stepRequest.setStepName(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        emrService.addStepToCluster(stepRequest);
    }

    /**
     * This method tests the happy path scenario by testing all the step types
     */
    @Test
    public void testEmrAddStepsHadoopNoMainClass() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // TODO: Why are we adding EMR steps to a list, but not doing anything with them once they're added?
        List<Serializable> emrSteps = new ArrayList<>();

        EmrHadoopJarStepAddRequest hadoopJarStepRequest = new EmrHadoopJarStepAddRequest();
        hadoopJarStepRequest.setNamespace(request.getNamespace());
        hadoopJarStepRequest.setEmrClusterDefinitionName(request.getEmrClusterDefinitionName());
        hadoopJarStepRequest.setEmrClusterName(request.getEmrClusterName());
        hadoopJarStepRequest.setStepName("Hadoop Jar");
        hadoopJarStepRequest.setJarLocation("s3://test-bucket-managed/app-a/test/hadoop-mapreduce-examples-2.4.0.jar");
        emrSteps.add(hadoopJarStepRequest);

        EmrHadoopJarStep emrHadoopJarStep = (EmrHadoopJarStep) emrService.addStepToCluster(hadoopJarStepRequest);

        assertNotNull(emrHadoopJarStep);
        assertNotNull(emrHadoopJarStep.getId());
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmrAddStepsInvalidCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);

        // Create a new EMR cluster create request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        stepRequest.setEmrClusterName("InvalidName");
        emrService.addStepToCluster(stepRequest);
    }

    /**
     * This method is to get the coverage for the emr service method that starts the new transaction.
     */
    @Test
    public void testEmrServiceMethodsNewTx() throws Exception
    {
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        try
        {
            emrServiceImpl.createCluster(request);
            fail("Should throw a ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }

        try
        {
            EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
            emrServiceImpl.addStepToCluster(stepRequest);
            fail("Should throw a ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }

        try
        {
            EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequest(request.getEmrClusterName());
            emrServiceImpl.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);
            fail("Should throw a ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }

        try
        {
            EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
                EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                    .withEmrClusterName("test_cluster").build();
            emrServiceImpl.getCluster(emrClusterAlternateKeyDto, null, null, false, null, false);
            fail("Should throw a ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }

        try
        {
            EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
                EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                    .withEmrClusterName("test_cluster").build();
            emrServiceImpl.terminateCluster(emrClusterAlternateKeyDto, false, null, null);
            fail("Should throw a ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testGetEmrClusterById() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, null, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));

        // Terminate the cluster and validate.
        emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, null);

        emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, null, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testGetEmrClusterByIdWithFleetInstance() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, null, true);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertNotNull(emrClusterGet.getInstanceFleets());
        assertTrue(emrClusterGet.getInstanceFleets().size() == 1);
    }

    /**
     * This method tests the scenario with AmazonServiceException.
     */
    @Test(expected = AmazonServiceException.class)
    public void testGetEmrClusterByIdAmazonException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME).withEmrClusterName("test")
                .build();

        emrService.getCluster(emrClusterAlternateKeyDto, MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION, null, true, null, false);

        fail("Should throw an AmazonServiceException.");
    }

    /**
     * This method tests the scenario when cluster specified does not exists.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEmrClusterByIdDoesNotExist() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName("cluster_does_not_exist").build();

        emrService.getCluster(emrClusterAlternateKeyDto, "cluster_does_not_exist", null, true, null, false);

        fail("Should throw an IllegalArgumentException.");
    }

    /**
     * This method tests the scenario when cluster specified does not exists.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEmrClusterByIdDoesNotExistForNamespace() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        //        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = EmrClusterAlternateKeyDto.builder().namespace(NAMESPACE)
        //                .emrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME).withEmrClusterName(request.getEmrClusterName()).build();


        // Create the second namespace entity.
        NamespaceEntity namespaceEntity_2 = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity_2, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto_2 =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE_2).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        emrService.getCluster(emrClusterAlternateKeyDto_2, emrCluster.getId(), null, true, null, false);

        fail("Should throw an IllegalArgumentException.");
    }

    /**
     * This method tests the scenario with providing step Id.
     */
    @Test
    public void testGetEmrClusterByIdWithStepId() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Add a running step
        // Create the Add step request.
        EmrShellStepAddRequest stepRequest = getNewEmrShellStepAddRequest(request.getEmrClusterName());
        stepRequest.setStepName(MockEmrOperationsImpl.MOCK_STEP_RUNNING_NAME);
        EmrShellStep emrShellStep = (EmrShellStep) emrService.addStepToCluster(stepRequest);

        String stepId = emrShellStep.getId();

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), stepId, true, null, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertTrue(stepId.equals(emrClusterGet.getActiveStep().getId()));
        assertTrue(stepId.equals(emrClusterGet.getStep().getId()));

        // Test the non verbose flow
        emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), stepId, false, null, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertTrue(stepId.equals(emrClusterGet.getActiveStep().getId()));
        assertTrue(stepId.equals(emrClusterGet.getStep().getId()));
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testGetEmrClusterByName() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, null, null, true, null, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
    }

    /**
     * This method tests the scenario in which the cluster name is blank for the method getEmrClusterIdByName
     */
    @Test
    public void testGetEmrClusterIdByNameForBlank() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        emrService.createCluster(request);
    }

    /**
     * This method tests the happy path scenario by providing all the parameters
     */
    @Test
    public void testTerminateEmrCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterTerminated = emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, null);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterTerminated);
        assertTrue(emrCluster.getNamespace().equals(emrClusterTerminated.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterTerminated.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterTerminated.getEmrClusterName()));
    }

    /**
     * This method tests the scenario where AmazonServiceException is thrown
     */
    @Test(expected = AmazonServiceException.class)
    public void testTerminateEmrClusterAmazonException() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        request.setEmrClusterName(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, null);
    }

    /**
     * This method tests the scenario when no active cluster exists.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTerminateEmrClusterNoCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName("cluster_not_found").build();

        emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, null);
    }

    @Test
    public void testTerminateEmrClusterWithClusterId() throws Exception
    {
        EmrService emrService = new EmrServiceImpl();

        AlternateKeyHelper mockAlternateKeyHelper = mock(AlternateKeyHelper.class);
        ReflectionTestUtils.setField(emrService, "alternateKeyHelper", mockAlternateKeyHelper);

        EmrHelper mockEmrHelper = mock(EmrHelper.class);
        ReflectionTestUtils.setField(emrService, "emrHelper", mockEmrHelper);

        EmrDao mockEmrDao = mock(EmrDao.class);
        ReflectionTestUtils.setField(emrService, "emrDao", mockEmrDao);

        NamespaceDaoHelper mockNamespaceDaoHelper = mock(NamespaceDaoHelper.class);
        ReflectionTestUtils.setField(emrService, "namespaceDaoHelper", mockNamespaceDaoHelper);

        EmrClusterDefinitionDaoHelper mockEmrClusterDefinitionDaoHelper = mock(EmrClusterDefinitionDaoHelper.class);
        ReflectionTestUtils.setField(emrService, "emrClusterDefinitionDaoHelper", mockEmrClusterDefinitionDaoHelper);

        String namespace = "namespace";
        String emrClusterDefinitionName = "emrClusterDefinitionName";
        String emrClusterName = "emrClusterName";
        boolean overrideTerminationProtection = false;
        String emrClusterId = "emrClusterId";

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto();
        emrClusterAlternateKeyDto.setNamespace(namespace);
        emrClusterAlternateKeyDto.setEmrClusterDefinitionName(emrClusterDefinitionName);
        emrClusterAlternateKeyDto.setEmrClusterName(emrClusterName);

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        when(mockEmrHelper.getAwsParamsDtoByAcccountId(any())).thenReturn(awsParamsDto);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();
        emrClusterDefinitionEntity.setNamespace(namespaceEntity);
        when(mockEmrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(any())).thenReturn(emrClusterDefinitionEntity);

        String buildEmrClusterNameResult = "buildEmrClusterNameResult";
        when(mockEmrHelper.buildEmrClusterName(any(), any(), any())).thenReturn(buildEmrClusterNameResult);

        when(mockEmrHelper.getActiveEmrClusterId(any(), any(), any())).thenReturn(buildEmrClusterNameResult);

        when(mockEmrDao.getEmrClusterStatusById(any(), any())).thenReturn(buildEmrClusterNameResult);

        emrService.terminateCluster(emrClusterAlternateKeyDto, overrideTerminationProtection, emrClusterId, null);

        verify(mockAlternateKeyHelper).validateStringParameter("namespace", namespace);
        verify(mockAlternateKeyHelper).validateStringParameter("An", "EMR cluster definition name", emrClusterDefinitionName);
        verify(mockAlternateKeyHelper).validateStringParameter("An", "EMR cluster name", emrClusterName);
        verify(mockEmrClusterDefinitionDaoHelper).getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName()));
        verify(mockEmrHelper)
            .buildEmrClusterName(namespaceEntity.getCode(), emrClusterDefinitionEntity.getName(), emrClusterAlternateKeyDto.getEmrClusterName());
        verify(mockEmrHelper).getActiveEmrClusterId(emrClusterId, buildEmrClusterNameResult, null);
        verify(mockEmrDao).terminateEmrCluster(buildEmrClusterNameResult, overrideTerminationProtection, awsParamsDto);
        verify(mockEmrDao).getEmrClusterStatusById(buildEmrClusterNameResult, awsParamsDto);
        //verifyNoMoreInteractions(mockEmrHelper, mockNamespaceDaoHelper, mockEmrClusterDefinitionDaoHelper, mockEmrDao);
    }

    /**
     * A test case to validate that the XSD restrictions are loose enough to allow empty elements for nested elements.
     */
    @Test
    public void testUnmarshallXmlWithNestedElements() throws JAXBException
    {
        String xml = "<emrClusterCreateRequest>" + "<namespace>" + NAMESPACE + "</namespace>" + "<emrClusterDefinitionName>" + EMR_CLUSTER_DEFINITION_NAME +
            "</emrClusterDefinitionName>" + "<emrClusterName>cluster1</emrClusterName>" + "<dryRun>true</dryRun>" + "<emrClusterDefinitionOverride>" +
            "<customBootstrapActionMaster/>" + "<customBootstrapActionAll/>" + "<instanceDefinitions/>" + "<nodeTags/>" + "<daemonConfigurations/>" +
            "<hadoopConfigurations/>" + "</emrClusterDefinitionOverride>" + "</emrClusterCreateRequest>";

        xmlHelper.unmarshallXmlToObject(EmrClusterCreateRequest.class, xml);
    }

    /**
     * Returns a list of {@link EmrClusterCreationLogEntity} objects for the given cluster namespace, cluster definition name, and EMR cluster name. All the
     * given parameters are case insensitive. The returned list's order is not guaranteed.
     *
     * @param namespace - EMR cluster namespace
     * @param definitionName - EMR cluster definition name
     * @param clusterName - EMR cluster name
     *
     * @return list of EMR cluster creation logs
     */
    protected List<EmrClusterCreationLogEntity> getEmrClusterCreationLogEntities(String namespace, String definitionName, String clusterName)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<EmrClusterCreationLogEntity> query = builder.createQuery(EmrClusterCreationLogEntity.class);
        Root<EmrClusterCreationLogEntity> emrClusterCreationLogEntity = query.from(EmrClusterCreationLogEntity.class);
        Join<?, NamespaceEntity> namespaceEntity = emrClusterCreationLogEntity.join(EmrClusterCreationLogEntity_.namespace);
        Predicate namespacePredicate = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespace.toUpperCase());
        Predicate definitionNamePredicate =
            builder.equal(builder.upper(emrClusterCreationLogEntity.get(EmrClusterCreationLogEntity_.emrClusterDefinitionName)), definitionName.toUpperCase());
        Predicate clusterNamePredicate =
            builder.equal(builder.upper(emrClusterCreationLogEntity.get(EmrClusterCreationLogEntity_.emrClusterName)), clusterName.toUpperCase());
        query.select(emrClusterCreationLogEntity).where(builder.and(namespacePredicate, definitionNamePredicate, clusterNamePredicate));
        return entityManager.createQuery(query).getResultList();
    }

    /**
     * <p> Validates whether EMR cluster creation log entries for the given cluster's namespace, definition name and cluster name. </p> <p> Asserts that: </p>
     * <ul> <li>There is exactly 1 log for the given data.</li> <li>The contents of the log matches the given expected.</li> </ul>
     *
     * @param emrCluster the EMR cluster.
     * @param expectedEmrClusterDefinition the expected EMR cluster definition.
     *
     * @throws JAXBException
     */
    protected void validateEmrClusterCreationLogUnique(EmrCluster emrCluster, EmrClusterDefinition expectedEmrClusterDefinition) throws JAXBException
    {
        String namespace = emrCluster.getNamespace();
        String emrClusterDefinitionName = emrCluster.getEmrClusterDefinitionName();
        String emrClusterName = emrCluster.getEmrClusterName();

        List<EmrClusterCreationLogEntity> list = getEmrClusterCreationLogEntities(namespace, emrClusterDefinitionName, emrClusterName);
        assertEquals("EMR cluster creation log size", 1, list.size());
        EmrClusterCreationLogEntity log = list.get(0);
        assertEquals("EMR cluster creation log cluster ID", emrCluster.getId(), log.getEmrClusterId());
        assertEquals("EMR cluster creation log namespace", namespace, log.getNamespace().getCode());

        String expectedDefinitionXml = xmlHelper.objectToXml(expectedEmrClusterDefinition);
        assertEquals("EMR cluster creation log definition", expectedDefinitionXml, log.getEmrClusterDefinition());
    }

    /**
     * Asserts that a log entry for the given cluster's namespace, definition name, and cluster name does not exist.
     *
     * @param emrCluster the EMR cluster.
     */
    private void assertEmrClusterCreationLogNotExist(EmrCluster emrCluster)
    {
        List<EmrClusterCreationLogEntity> list =
            getEmrClusterCreationLogEntities(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName());
        assertTrue("EMR cluster creation log empty", list.isEmpty());
    }

    /**
     * This method fills-up the parameters required for the EMR add steps request. This is called from all the other test methods.
     */
    private EmrMasterSecurityGroupAddRequest getNewEmrAddSecurityGroupMasterRequest(String clusterName) throws Exception
    {
        // Create the EmrMasterSecurityGroupAddRequest object
        EmrMasterSecurityGroupAddRequest request = new EmrMasterSecurityGroupAddRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(clusterName);

        List<String> groupIds = new ArrayList<>();
        groupIds.add("A_TEST_SECURITY_GROUP");

        request.setSecurityGroupIds(groupIds);

        return request;
    }

    /**
     * This method fills-up the parameters required for the EMR cluster create request. This is called from all the other test methods.
     */
    private EmrClusterCreateRequest getNewEmrClusterCreateRequest() throws Exception
    {
        // Create the definition.
        EmrClusterCreateRequest request = new EmrClusterCreateRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName("UT_EMR_CLUSTER-" + Math.random());

        return request;
    }

    /**
     * This method creates a EMR Shell step add request. This is called from all the other test methods.
     */
    private EmrShellStepAddRequest getNewEmrShellStepAddRequest(String clusterName) throws Exception
    {
        // Create the request.
        EmrShellStepAddRequest request = new EmrShellStepAddRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName(clusterName);

        request.setStepName("A_SHELL_STEP");
        request.setScriptLocation("SCRIPT_LOCATION");
        request.setContinueOnError(false);
        List<String> arguments = new ArrayList<>();
        arguments.add("one");
        arguments.add("two");
        request.setScriptArguments(arguments);

        return request;
    }
}
