package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.impl.MockAwsOperationsHelper;
import org.finra.herd.dao.impl.MockOozieOperationsImpl;
import org.finra.herd.dao.impl.OozieDaoImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrOozieStepAddRequest;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.api.xml.OozieWorkflowJob;
import org.finra.herd.model.api.xml.RunOozieWorkflowRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.EmrStepHelper;

/**
 * This class is used test EmrSerivice with overriding account id
 */
public class EmrServiceWithAccountIdTest extends EmrServiceTest
{
    @Autowired
    @Qualifier(value = "emrServiceImpl")
    private EmrService emrServiceImpl;

    @Autowired
    private EmrOperations emrOperations;
    
    
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
            EmrClusterAlternateKeyDto.builder().namespace(NAMESPACE).emrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .emrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, true, AWS_ACCOUNT_ID);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterGet);
        assertTrue(emrCluster.getId().equals(emrClusterGet.getId()));
        assertTrue(emrCluster.getNamespace().equals(emrClusterGet.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterGet.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterGet.getEmrClusterName()));
        assertEquals(emrCluster.getEmrClusterDefinition().getAccountId(), AWS_ACCOUNT_ID);
        
        // Validate the oozie jobs
        assertNotNull(emrClusterGet.getOozieWorkflowJobs());
        assertTrue(emrClusterGet.getOozieWorkflowJobs().size() ==
            herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.EMR_OOZIE_JOBS_TO_INCLUDE_IN_CLUSTER_STATUS));

        int clientRunningCount = 0;
        int clientNotRunningCount = 0;

        for (OozieWorkflowJob oozieWorkflowJob : emrClusterGet.getOozieWorkflowJobs())
        {
            if (oozieWorkflowJob.getStatus().equals(OozieDaoImpl.OOZIE_WORKFLOW_JOB_STATUS_DM_PREP))
            {
                clientNotRunningCount++;
            }
            else if (oozieWorkflowJob.getStatus().equals(WorkflowJob.Status.RUNNING.toString()))
            {
                clientRunningCount++;
            }
        }

        assertTrue(50 == clientRunningCount);
        assertTrue(50 == clientNotRunningCount);

        // Terminate the cluster and validate.
        emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, AWS_ACCOUNT_ID);

        emrClusterGet = emrService.getCluster(emrClusterAlternateKeyDto, emrCluster.getId(), null, true, true, AWS_ACCOUNT_ID);

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
     * This method tests the happy path scenario by testing all the step types
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

        EmrOozieStepAddRequest oozieStep = new EmrOozieStepAddRequest();
        oozieStep.setStepName("Test Oozie");
        oozieStep.setWorkflowXmlLocation("s3://test-bucket-managed/app-a/test/workflow.xml");
        oozieStep.setOoziePropertiesFileLocation("s3://test-bucket-managed/app-a/test/job.properties");
        emrSteps.add(oozieStep);

        EmrHadoopJarStepAddRequest hadoopJarStepRequest = new EmrHadoopJarStepAddRequest();
        List<String> scriptArgs2 = new ArrayList<>();
        scriptArgs2.add("oozie_run");
        scriptArgs2.add("wordcountOutput");
        hadoopJarStepRequest.setStepName("Hadoop Jar");
        hadoopJarStepRequest.setJarLocation("s3://test-bucket-managed/app-a/test/hadoop-mapreduce-examples-2.4.0.jar");
        hadoopJarStepRequest.setMainClass("wordcount");
        hadoopJarStepRequest.setScriptArguments(scriptArgs2);
        emrSteps.add(hadoopJarStepRequest);

        EmrStepHelper stepHelper = null;

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
    
    
    /**
     * This method fills-up the parameters required for the EMR cluster create request,
     * with overridden AWS account Id.
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
    
    /**
     * This method tests the happy path scenario for adding security groups
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
     * This method tests the Oozie job submission.
     */
    @Test
    public void testRunOozieJob() throws Exception
    {
        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
        EmrCluster emrCluster = emrService.createCluster(request);

        // Create the Run Oozie Job.
        RunOozieWorkflowRequest runOozieRequest =
            new RunOozieWorkflowRequest(namespaceEntity.getCode(), EMR_CLUSTER_DEFINITION_NAME, emrCluster.getEmrClusterName(), OOZIE_WORKFLOW_LOCATION, null,
                null, AWS_ACCOUNT_ID);
        OozieWorkflowJob oozieWorkflowJob = emrService.runOozieWorkflow(runOozieRequest);

        // Validate the returned object against the input.
        assertNotNull(oozieWorkflowJob);
        assertNotNull(oozieWorkflowJob.getId());
        assertTrue(oozieWorkflowJob.getNamespace().equals(request.getNamespace()));
        assertTrue(oozieWorkflowJob.getEmrClusterDefinitionName().equals(request.getEmrClusterDefinitionName()));
        assertTrue(oozieWorkflowJob.getEmrClusterName().equals(request.getEmrClusterName()));
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

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);
        
        // Create a new EMR cluster create request
        EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto =
            EmrClusterAlternateKeyDto.builder().namespace(NAMESPACE).emrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME)
                .emrClusterName(request.getEmrClusterName()).build();

        EmrCluster emrClusterTerminated = emrService.terminateCluster(emrClusterAlternateKeyDto, true, null, null);

        // Validate the returned object against the input.
        assertNotNull(emrCluster);
        assertNotNull(emrClusterTerminated);
        assertTrue(emrCluster.getNamespace().equals(emrClusterTerminated.getNamespace()));
        assertTrue(emrCluster.getEmrClusterDefinitionName().equals(emrClusterTerminated.getEmrClusterDefinitionName()));
        assertTrue(emrCluster.getEmrClusterName().equals(emrClusterTerminated.getEmrClusterName()));
    }
    
    @Test
    public void testGetEmrOozieWorkflowJobWithClusterId() throws Exception
    {
        String oozieWorkflowJobId = MockOozieOperationsImpl.CASE_1_JOB_ID;
        Boolean verbose = null;
        
        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);
        
        EmrCluster emrCluster = createEmrClusterInWaitingStateWithAccountId(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        OozieWorkflowJob oozieWorkflowJob = emrService
            .getEmrOozieWorkflowJob(emrCluster.getNamespace(), emrCluster.getEmrClusterDefinitionName(), emrCluster.getEmrClusterName(), oozieWorkflowJobId,
                verbose, emrCluster.getId(), AWS_ACCOUNT_ID);

        assertEquals("job ID", MockOozieOperationsImpl.CASE_1_JOB_ID, oozieWorkflowJob.getId());
        assertEquals("namespace", emrCluster.getNamespace(), oozieWorkflowJob.getNamespace());
        assertEquals("EMR cluster definition name", emrCluster.getEmrClusterDefinitionName(), oozieWorkflowJob.getEmrClusterDefinitionName());
        assertEquals("EMR cluster name", emrCluster.getEmrClusterName(), oozieWorkflowJob.getEmrClusterName());
        assertNotNull("job start time is null", oozieWorkflowJob.getStartTime());
        assertNull("job end time is not null", oozieWorkflowJob.getEndTime());
        assertNull("actions is not null", oozieWorkflowJob.getWorkflowActions());
    }
    
    private EmrCluster createEmrClusterInWaitingStateWithAccountId(String namespace, String emrClusterDefinitionName)
    {
        String amiVersion = MockAwsOperationsHelper.AMAZON_CLUSTER_STATUS_WAITING;
        
        return createEmrClusterWithAccountId(namespace, emrClusterDefinitionName, amiVersion);
    }
    
    /**
     * Creates a new EMR cluster along with the specified namespace and definition name. A AMI version may be specified to hint the mock for certain behaviors.
     * This method uses the cluster definition specified by EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH.
     *
     * @param namespace - namespace to create
     * @param emrClusterDefinitionName - EMR cluster definition name
     * @param amiVersion - AMI version used to hint. Default behavior will be used if set to null.
     *
     * @return newly created EMR cluster, with generated cluster name
     */
    private EmrCluster createEmrClusterWithAccountId(String namespace, String emrClusterDefinitionName, String amiVersion)
    {
        EmrCluster emrCluster;

        try
        {
            // Create the namespace entity.
            NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(namespace);

            String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH).getInputStream());

            EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

            if (amiVersion != null)
            {
                emrClusterDefinition.setAmiVersion(amiVersion);
            }

            configXml = xmlHelper.objectToXml(emrClusterDefinition);

            emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, emrClusterDefinitionName, configXml);

            EmrClusterCreateRequest request = getNewEmrClusterCreateRequestWithAccountId();
            emrCluster = emrService.createCluster(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Error staging data", e);
        }
        return emrCluster;
    }
}
