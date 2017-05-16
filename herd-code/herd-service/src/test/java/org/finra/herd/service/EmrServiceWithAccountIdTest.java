package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.finra.herd.dao.impl.MockAwsOperationsHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.EmrStepHelper;

/**
 * This class is used to test EmrService with overriding Account ID.
 */
public class EmrServiceWithAccountIdTest extends EmrServiceTest
{
    /**
     * This method tests the happy path scenario for adding security groups.
     */
    @Test
    public void testAddSecurityGroup() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
        emrService.createCluster(request);

        // Create the Add security group.
        EmrMasterSecurityGroupAddRequest emrMasterSecurityGroupAddRequest = getNewEmrAddSecurityGroupMasterRequestWithAccountId(request.getEmrClusterName());
        EmrMasterSecurityGroup emrMasterSecurityGroup = emrService.addSecurityGroupsToClusterMaster(emrMasterSecurityGroupAddRequest);

        // Validate the returned object against the input.
        assertNotNull(emrMasterSecurityGroup);
        assertTrue(emrMasterSecurityGroup.getNamespace().equals(request.getNamespace()));
        assertTrue(emrMasterSecurityGroup.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrMasterSecurityGroup.getEmrClusterName().equals(request.getEmrClusterName()));
    }

    @Test
    public void testCreateEmrClusterWithAccountId() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request and add an account id to the EMR cluster definition override.
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);
        emrClusterDefinitionOverride.setAccountId(AWS_ACCOUNT_ID);

        // Create an EMR cluster.
        EmrCluster emrCluster = emrService.createCluster(request);

        // Update the expected EMR cluster definition with the account id.
        expectedEmrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertTrue(emrCluster.getNamespace().equals(request.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(request.getEmrClusterName()));
        assertEquals(AWS_ACCOUNT_ID, emrCluster.getAccountId());
        assertNotNull(emrCluster.getId());
        assertNull(emrCluster.isDryRun());
        assertNotNull(emrCluster.getEmrClusterDefinition());
        assertTrue(emrCluster.isEmrClusterCreated());
        assertEquals(expectedEmrClusterDefinition, emrCluster.getEmrClusterDefinition());

        validateEmrClusterCreationLogUnique(emrCluster, expectedEmrClusterDefinition);
    }

    @Test
    public void testCreateEmrClusterWithAccountIdAccountNoExists() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String definitionXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition expectedEmrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, definitionXml);
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, definitionXml);

        // Create a new EMR cluster create request and add an account id to the EMR cluster definition override.
        EmrClusterCreateRequest emrClusterCreateRequest = getNewEmrClusterCreateRequestWithAccountId();
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);
        emrClusterDefinitionOverride.setAccountId(AWS_ACCOUNT_ID);

        // Try to create an EMR cluster using a non-existing AWS account.
        try
        {
            emrService.createCluster(emrClusterCreateRequest);
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Trusting AWS account with id \"%s\" doesn't exist.", AWS_ACCOUNT_ID), e.getMessage());
        }
    }

    /**
     * This method tests the happy path scenario by testing all the step types.
     */
    @Test
    public void testEmrAddStepsAllTypes() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
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
            stepHelper.setRequestAccountId(emrStepAddRequest, AWS_ACCOUNT_ID);
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

    @Test
    public void testGetEmrClusterById() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().withNamespace(NAMESPACE).withEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .withEmrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, AWS_ACCOUNT_ID, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertEquals(emrCluster.getEmrClusterDefinition().getAccountId(), AWS_ACCOUNT_ID);
        assertEquals(AWS_ACCOUNT_ID, emrCluster.getAccountId());

        // Terminate the cluster and validate.
        emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, AWS_ACCOUNT_ID);

        emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, AWS_ACCOUNT_ID, false);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertEquals(emrCluster.getEmrClusterDefinition().getAccountId(), AWS_ACCOUNT_ID);
    }

    /**
     * This method tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testTerminateEmrCluster() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
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
        assertEquals(AWS_ACCOUNT_ID, emrCluster.getAccountId());
    }

    /**
     * This method fills-up the parameters required for the EMR add steps request. This is called from all the other test methods.
     */
    private EmrMasterSecurityGroupAddRequest getNewEmrAddSecurityGroupMasterRequestWithAccountId(String clusterName) throws Exception
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
        //add account id
        request.setAccountId(AWS_ACCOUNT_ID);
        return request;
    }

    /**
     * This method fills-up the parameters required for the EMR cluster create request, with overridden AWS account Id.
     */
    private EmrClusterCreateRequest getNewEmrClusterCreateRequestWithAccountId() throws Exception
    {
        // Create the definition.
        EmrClusterCreateRequest request = new EmrClusterCreateRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName("UT_EMR_CLUSTER-" + Math.random());

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);
        emrClusterDefinitionOverride.setAccountId(AWS_ACCOUNT_ID);

        return request;
    }
}
