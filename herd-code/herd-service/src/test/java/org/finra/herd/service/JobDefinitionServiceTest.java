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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ScriptTask;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.jpa.JobDefinitionEntity;

/**
 * This class tests functionality within the JobDefinitionService.
 */
public class JobDefinitionServiceTest extends AbstractServiceTest
{
    private static final String INVALID_NAME = "Herd_Invalid_Name_" + UUID.randomUUID().toString().substring(0, 3);

    /**
     * This method tests the happy path scenario by providing all the parameters except attributes
     */
    @Test
    public void testCreateJobDefinition() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(null);
    }

    /**
     * This method tests the scenario in which the jobName is invalid IllegalArgumentException is expected to be thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobDefinitionNoJobName() throws Exception
    {
        String invalidJobName = TEST_ACTIVITI_NAMESPACE_CD;

        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Enter the invalid job name.
        request.setJobName(invalidJobName);

        // The following method is expected to throw an IllegalArgumentException.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario in which the namespace is invalid ObjectNotFoundException is expected to be thrown
     */
    @Test(expected = ObjectNotFoundException.class)
    public void testCreateJobDefinitionInvalidNamespace() throws Exception
    {
        // Create the job request without registering the namespace entity - ${TEST_ACTIVITI_NAMESPACE_CD}
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Following method must throw ObjectNotFoundException, as the namespace entity ${TEST_ACTIVITI_NAMESPACE_CD} does not exist.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario in which the user tries to register the same job flow twice AlreadyExistsException is expected to be thrown.
     */
    @Test(expected = AlreadyExistsException.class)
    public void testCreateJobDefinitionAlreadyExists() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        jobDefinitionService.createJobDefinition(request, false);

        // Create the same request again which is an invalid operation, as the workflow exists.
        // Following must throw AlreadyExistsException
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario in which the user passes an ill-formatted xml file XMLException is expected to be thrown
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobDefinitionInvalidActivitiXml() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Get the XML file for the test workflow.
        InputStream xmlStream = resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream();

        // Just remove "startEvent" text from the XML file which makes the following line in the XML file as INVALID
        // <startEvent id="startevent1" name="Start"></startEvent>
        request.setActivitiJobXml(IOUtils.toString(xmlStream).replaceAll("startEvent", ""));

        // Try creating the job definition and this must throw XMLException.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the happy path scenario in which all the parameters including the attributes are given.
     */
    @Test
    public void testCreateJobDefinitionWithParams() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Add parameters
        List<Parameter> parameterEntities = new ArrayList<>();
        Parameter parameterEntity = new Parameter();
        parameterEntity.setName(ATTRIBUTE_NAME_1_MIXED_CASE);
        parameterEntity.setValue(ATTRIBUTE_VALUE_1);
        parameterEntities.add(parameterEntity);
        request.setParameters(parameterEntities);

        // Try creating the job definition.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the happy path scenario in which no parameters are given.
     */
    @Test
    public void testCreateJobDefinitionWithNoParams() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        request.setParameters(null);

        // Try creating the job definition.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario in which an invalid Java class name is given. This must throw IllegalArgumentException from the Activiti layer.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobDefinitionInvalidActivitiElement() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Read the Activiti XML file so that an error can be injected.
        InputStream xmlStream = resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream();

        // Inject an error by having an invalid Activiti element name in the XML file.
        // Note that XML file structure is correct as per the XML schema. However, there is an invalid Activiti element in the XML file.
        // The line below will be affected in the XML file as per this error injection.
        // <serviceTask id="servicetask1" name="Test Service Step" activiti:class="org.activiti.engine.impl.test.NoOpServiceTask">
        request.setActivitiJobXml(IOUtils.toString(xmlStream).replaceAll("serviceTask", "invalidActivitiTask"));

        // Try creating the job definition and the Activiti layer mush throw an exception.
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario when shell type activiti task is given. This must throw IllegalArgumentException from the Activiti layer.
     * It also validates case insensitivity of the actitviti shell task type.
     */
    @Test
    public void testCreateJobDefinitionWithShellTypeServiceTask() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Read the Activiti XML file so that an error can be injected.
        String xmlString = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream(), Charset.defaultCharset());
        String errorMsg = "Activiti XML can not contain activiti shell type service tasks.";

        // Update Activiti XML such that service task is modified to shell type activiti task with all lower case - activiti:type="shell".
        testCreateJobDefinitionWithProhibitedActivitiXml(request, xmlString
            .replaceAll("serviceTask id=\"servicetask1\" name=\"Test Service Step\" activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"",
                "serviceTask id=\"testShellTask\" name=\"Execute test shell task\" activiti:type=\"shell\" activiti:async=\"true\""), errorMsg);

        // Update Activiti XML such that service task is modified to shell type activiti task with all upper case - ACTIVITI:TYPE="SHELL".
        testCreateJobDefinitionWithProhibitedActivitiXml(request, xmlString
            .replaceAll("serviceTask id=\"servicetask1\" name=\"Test Service Step\" activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"",
                "serviceTask id=\"testShellTask\" name=\"Execute test shell task\" ACTIVITI:TYPE=\"SHELL\" activiti:async=\"true\""), errorMsg);

        // Update Activiti XML such that service task is modified to shell type activiti task with all upper case - activiti:type= " shell".
        testCreateJobDefinitionWithProhibitedActivitiXml(request, xmlString
            .replaceAll("serviceTask id=\"servicetask1\" name=\"Test Service Step\" activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"",
                "serviceTask id=\"testShellTask\" name=\"Execute test shell task\" activiti:type= \" shell\" activiti:async=\"true\""), errorMsg);
    }

    /**
     * This method tests the scenario when reflection is used in activiti JUEL expression. This must throw IllegalArgumentException from the Activiti layer.
     */
    @Test
    public void testCreateJobDefinitionWithProhibitedJuelExpression() throws IOException
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a valid job definition request.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Read the Activiti XML file so that an error can be injected.
        String xmlString = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream(), Charset.defaultCharset());

        String errorMsg = "Activiti XML has prohibited expression.";
        // Update Activiti XML such that service task is modified to use activiti:expression with reflection.
        String activitiExpression =
            "activiti:expression=\"\\${''\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request,
            xmlString.replaceAll("activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"", activitiExpression), errorMsg);

        // Update Activiti XML such that service task is modified to use activiti:expression with reflection and whitespace.
        String activitiExpressionWithWhitespace =
            "activiti:expression= \" \\${ ''\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request,
            xmlString.replaceAll("activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"", activitiExpressionWithWhitespace), errorMsg);

        // Update Activiti XML such that service task is modified to use deferred expression with reflection and double quote.
        String activitiExpressionWithDoubleQuote =
            "activiti:expression=\"\\${\"\"\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request,
            xmlString.replaceAll("activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"", activitiExpressionWithDoubleQuote), errorMsg);

        // Update Activiti XML such that service task is modified to use immediate expression with reflection and double quote.
        String immediateExpressionWithDoubleQuote =
            "activiti:expression=\"\\#{\"\"\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request,
            xmlString.replaceAll("activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"", immediateExpressionWithDoubleQuote), errorMsg);

        // Update Activiti XML such that extension element is modified to use deferred expression with reflection.
        String extensionElementExpression =
            "expression= \" \\${ ''\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request, xmlString.replaceAll("stringValue=\"Unit Test\"", extensionElementExpression), errorMsg);

        // Update Activiti XML such that extension element is modified to use immediate evaluate expression with reflection and whitespace.
        String immediateEvaluateExpression =
            "expression= \" \\#{ ''\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}\"";
        testCreateJobDefinitionWithProhibitedActivitiXml(request, xmlString.replaceAll("stringValue=\"Unit Test\"", immediateEvaluateExpression), errorMsg);

        // Update Activiti XML such that extension element is modified to use field injection expression with reflection.
        String filedInjectionExpression =
            " <activiti:field name=\"name\" > <activiti:expression>\\${ ''\\.class\\.forName('java\\.lang\\.Runtime')\\.methods[6]\\.invoke(null)\\.exec('touch /tmp /testing')}</activiti:expression>\n" +
                "        </activiti:field>";
        testCreateJobDefinitionWithProhibitedActivitiXml(request,
            xmlString.replaceAll("<activiti:field name=\"name\" stringValue=\"Unit Test\" />", filedInjectionExpression), errorMsg);
    }

    private void testCreateJobDefinitionWithProhibitedActivitiXml(JobDefinitionCreateRequest request, String activitiXml, String errorMsg)
    {
        // Update Activiti XML such that service task has prohibited activiti xml
        request.setActivitiJobXml(activitiXml);
        // Try creating the job definition and the Activiti layer must throw an exception.
        try
        {
            jobDefinitionService.createJobDefinition(request, false);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals(errorMsg, e.getMessage());
        }
    }

    /**
     * This method tests the scenario when allowed activiti JUEL expression is given.
     */
    @Test
    public void testCreateJobDefinitionWithAllowedJuelServiceTask() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a valid job definition request.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        request.getParameters().add(new Parameter("testKey", "testValue"));
        // Read the Activiti XML file so that an error can be injected.
        String xmlString = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream(), Charset.defaultCharset());

        // Update Activiti XML such that service task has allowed JUEL expression

        request.setActivitiJobXml(
            xmlString.replaceAll("activiti:class=\"org.activiti.engine.impl.test.NoOpServiceTask\"", "activiti:expression=\"\\${testKey2}\""));

        // Try creating the job definition
        jobDefinitionService.createJobDefinition(request, false);
    }

    /**
     * This method tests the scenario when not allowed task class is given. This must throw IllegalArgumentException from the Activiti layer.
     */
    @Test
    public void testCreateJobDefinitionWithNotAllowedTaskClass() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Read the Activiti XML file so that an error can be injected.
        InputStream xmlStream = resourceLoader.getResource(ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH).getInputStream();

        // Update Activiti XML such that service task is modified to use not allowed activiti:class
        request.setActivitiJobXml(
            IOUtils.toString(xmlStream).replaceAll("org.activiti.engine.impl.test.NoOpServiceTask", "org.activiti.engine.impl.behavior.ShellActivityBehavior"));

        // Try creating the job definition and the Activiti layer must throw an exception.
        try
        {
            jobDefinitionService.createJobDefinition(request, false);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Activiti XML has prohibited service task class.", e.getMessage());
        }
    }

    /**
     * Asserts that when a job definition is created using {@link S3PropertiesLocation}, the S3 location information is persisted.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionWithS3PropertiesLocationPersistsEntity() throws Exception
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();

        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        request.setS3PropertiesLocation(s3PropertiesLocation);

        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(request, false);

        Assert.assertEquals("jobDefinition s3PropertiesLocation", request.getS3PropertiesLocation(), jobDefinition.getS3PropertiesLocation());

        JobDefinitionEntity jobDefinitionEntity = herdDao.findById(JobDefinitionEntity.class, jobDefinition.getId());

        Assert.assertNotNull("jobDefinitionEntity is null", jobDefinitionEntity);
        Assert.assertEquals("jobDefinitionEntity s3BucketName", s3PropertiesLocation.getBucketName(), jobDefinitionEntity.getS3BucketName());
        Assert.assertEquals("jobDefinitionEntity s3ObjectKey", s3PropertiesLocation.getKey(), jobDefinitionEntity.getS3ObjectKey());
    }

    /**
     * Asserts that if {@link S3PropertiesLocation} is given, bucket name is required.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionWithS3PropertiesLocationValidateBucketNameRequired() throws Exception
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();
        s3PropertiesLocation.setBucketName(null);
        testCreateJobDefinitionWithS3PropertiesLocationValidate(s3PropertiesLocation, IllegalArgumentException.class,
            "S3 properties location bucket name must be specified.");
    }

    /**
     * Asserts that if {@link S3PropertiesLocation} is given, key is required.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionWithS3PropertiesLocationValidateObjectKeyRequired() throws Exception
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();
        s3PropertiesLocation.setKey(null);
        testCreateJobDefinitionWithS3PropertiesLocationValidate(s3PropertiesLocation, IllegalArgumentException.class,
            "S3 properties location object key must be specified.");
    }

    @Test
    public void testGetJobDefinition() throws Exception
    {
        // Create a new job definition.
        JobDefinition jobDefinition = createJobDefinition();

        // Retrieve the job definition.
        jobDefinition = jobDefinitionService.getJobDefinition(jobDefinition.getNamespace(), jobDefinition.getJobName());

        // Validate that the retrieved job definition matches what we created.
        validateJobDefinition(jobDefinition);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetJobDefinitionNoExist() throws Exception
    {
        // Retrieve a job definition that doesn't exist.
        jobDefinitionService.getJobDefinition(INVALID_NAME, INVALID_NAME);
    }

    @Test
    public void testUpdateJobDefinition() throws Exception
    {
        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest createRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Set 2 distinct parameters.
        List<Parameter> parameters = new ArrayList<>();
        createRequest.setParameters(parameters);

        Parameter parameter = new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        parameters.add(parameter);
        parameter = new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);
        parameters.add(parameter);

        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(createRequest, false);

        // Create an update request with a varied set of data that is based on the same data used in the create request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createRequest);

        // Update the job definition in the database.
        JobDefinition updatedJobDefinition =
            jobDefinitionService.updateJobDefinition(createRequest.getNamespace(), createRequest.getJobName(), updateRequest, false);

        // Validate the updated job definition.
        assertEquals(new JobDefinition(jobDefinition.getId(), jobDefinition.getNamespace(), jobDefinition.getJobName(), updateRequest.getDescription(),
            updateRequest.getActivitiJobXml(), updateRequest.getParameters(), jobDefinition.getS3PropertiesLocation(), HerdDaoSecurityHelper.SYSTEM_USER),
            updatedJobDefinition);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateJobDefinitionNamespaceNoExist() throws Exception
    {
        // Create an update request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest());

        // Update the process Id to match an invalid namespace and invalid job name to pass validation.
        updateRequest.setActivitiJobXml(
            updateRequest.getActivitiJobXml().replace(TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME, INVALID_NAME + "." + INVALID_NAME));

        // Try to update a job definition that has a namespace that doesn't exist.
        jobDefinitionService.updateJobDefinition(INVALID_NAME, INVALID_NAME, updateRequest, false);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateJobDefinitionJobNameNoExist() throws Exception
    {
        // Create an update request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest());

        // Update the process Id to match a valid namespace and invalid job name to pass validation.
        updateRequest.setActivitiJobXml(updateRequest.getActivitiJobXml()
            .replace(TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME, TEST_ACTIVITI_NAMESPACE_CD + "." + INVALID_NAME));

        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Try to update a job definition that has a namespace that exists, but a job name that doesn't exist.
        jobDefinitionService.updateJobDefinition(TEST_ACTIVITI_NAMESPACE_CD, INVALID_NAME, updateRequest, false);
    }

    @Test
    public void testUpdateJobDefinitionWithS3Properties() throws Exception
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();

        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest createRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Create the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(createRequest, false);

        // Create an update request with a varied set of data that is based on the same data used in the create request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createRequest);
        updateRequest.setS3PropertiesLocation(s3PropertiesLocation);

        // Update the job definition in the database.
        JobDefinition updatedJobDefinition =
            jobDefinitionService.updateJobDefinition(createRequest.getNamespace(), createRequest.getJobName(), updateRequest, false);
        JobDefinitionEntity updatedJobDefinitionEntity = herdDao.findById(JobDefinitionEntity.class, updatedJobDefinition.getId());

        // Validate the updated job definition.
        assertEquals(new JobDefinition(jobDefinition.getId(), jobDefinition.getNamespace(), jobDefinition.getJobName(), updateRequest.getDescription(),
            updateRequest.getActivitiJobXml(), updateRequest.getParameters(), s3PropertiesLocation, HerdDaoSecurityHelper.SYSTEM_USER), updatedJobDefinition);

        // Validate the updated job definition entity.
        Assert.assertEquals("updatedJobDefinitionEntity s3BucketName", s3PropertiesLocation.getBucketName(), updatedJobDefinitionEntity.getS3BucketName());
        Assert.assertEquals("updatedJobDefinitionEntity s3ObjectKey", s3PropertiesLocation.getKey(), updatedJobDefinitionEntity.getS3ObjectKey());
    }

    @Test
    public void testUpdateJobDefinitionWithS3PropertiesClear() throws Exception
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();

        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest createRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        createRequest.setS3PropertiesLocation(s3PropertiesLocation);

        // Create the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(createRequest, false);

        // Create an update request with a varied set of data that is based on the same data used in the create request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createRequest);

        // Update the job definition in the database.
        JobDefinition updatedJobDefinition =
            jobDefinitionService.updateJobDefinition(createRequest.getNamespace(), createRequest.getJobName(), updateRequest, false);
        JobDefinitionEntity updatedJobDefinitionEntity = herdDao.findById(JobDefinitionEntity.class, updatedJobDefinition.getId());

        // Validate the updated job definition.
        assertEquals(new JobDefinition(jobDefinition.getId(), jobDefinition.getNamespace(), jobDefinition.getJobName(), updateRequest.getDescription(),
            updateRequest.getActivitiJobXml(), updateRequest.getParameters(), null, HerdDaoSecurityHelper.SYSTEM_USER), updatedJobDefinition);

        // Validate the updated job definition entity.
        Assert.assertNull("updatedJobDefinitionEntity s3BucketName", updatedJobDefinitionEntity.getS3BucketName());
        Assert.assertNull("updatedJobDefinitionEntity s3ObjectKey", updatedJobDefinitionEntity.getS3ObjectKey());
    }

    /**
     * Asserts that create job definition proceeds without exceptions when first task is set to async
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionAssertSuccessWhenFirstTaskAsync() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId(namespace + '.' + jobName);
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            ScriptTask element = new ScriptTask();
            element.setId("script");
            element.setScriptFormat("js");
            element.setScript("// do nothing");
            element.setAsynchronous(true);
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }
        process.addFlowElement(new SequenceFlow("start", "script"));
        process.addFlowElement(new SequenceFlow("script", "end"));
        bpmnModel.addProcess(process);
        String activitiJobXml = getActivitiXmlFromBpmnModel(bpmnModel);
        namespaceDaoTestHelper.createNamespaceEntity(namespace);

        jobDefinitionService.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null), true);

        // Assert no exceptions
    }

    /**
     * Asserts that create job definition proceeds throws an exception when the first task is not async
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionAssertThrowErrorWhenFirstTaskNotAsync() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId(namespace + '.' + jobName);
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            ScriptTask element = new ScriptTask();
            element.setId("script");
            element.setScriptFormat("js");
            element.setScript("// do nothing");
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }
        process.addFlowElement(new SequenceFlow("start", "script"));
        process.addFlowElement(new SequenceFlow("script", "end"));
        bpmnModel.addProcess(process);
        String activitiJobXml = getActivitiXmlFromBpmnModel(bpmnModel);
        namespaceDaoTestHelper.createNamespaceEntity(namespace);
        try
        {
            jobDefinitionService.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null), true);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Element with id \"script\" must be set to activiti:async=true. All tasks which start the workflow must be asynchronous to prevent " +
                "certain undesired transactional behavior, such as records of workflow not being saved on errors. Please refer to Activiti and herd " +
                "documentations for details.", e.getMessage());
        }
    }

    /**
     * Asserts that create job definition proceeds without exceptions when first task does not support async (ex. events)
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobDefinitionAssertSuccessWhenFirstTaskNotAsyncable() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId(namespace + '.' + jobName);
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }
        process.addFlowElement(new SequenceFlow("start", "end"));
        bpmnModel.addProcess(process);
        String activitiJobXml = getActivitiXmlFromBpmnModel(bpmnModel);
        namespaceDaoTestHelper.createNamespaceEntity(namespace);

        jobDefinitionService.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null), true);

        // Assert no exceptions
    }

    /**
     * Asserts that update job definition proceeds without exceptions when first task is set to async
     *
     * @throws Exception
     */
    @Test
    public void testUpdateJobDefinitionAssertSuccessWhenFirstTaskAsync() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId(namespace + '.' + jobName);
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            ScriptTask element = new ScriptTask();
            element.setId("script");
            element.setScriptFormat("js");
            element.setScript("// do nothing");
            element.setAsynchronous(true);
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }
        process.addFlowElement(new SequenceFlow("start", "script"));
        process.addFlowElement(new SequenceFlow("script", "end"));
        bpmnModel.addProcess(process);
        String activitiJobXml = getActivitiXmlFromBpmnModel(bpmnModel);
        namespaceDaoTestHelper.createNamespaceEntity(namespace);

        jobDefinitionService.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null), true);

        jobDefinitionService.updateJobDefinition(namespace, jobName, new JobDefinitionUpdateRequest(null, activitiJobXml, null, null), true);

        // Assert no exceptions
    }

    /**
     * Create an update request with a varied set of data that is based on the same data used in the create request.
     *
     * @param createRequest the create request.
     *
     * @return the update request.
     */
    private JobDefinitionUpdateRequest createUpdateRequest(JobDefinitionCreateRequest createRequest)
    {
        // Create an update request that modifies all data from the create request.
        JobDefinitionUpdateRequest updateRequest = new JobDefinitionUpdateRequest();
        updateRequest.setDescription(createRequest.getDescription() + "2");
        updateRequest.setActivitiJobXml(createRequest.getActivitiJobXml().replace("Unit Test", "Unit Test 2"));

        List<Parameter> parameters = new ArrayList<>();
        updateRequest.setParameters(parameters);

        // Delete the first parameter, update the second parameter, and add a new third parameter.
        Parameter parameter = new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2 + "2");
        parameters.add(parameter);
        parameter = new Parameter(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);
        parameters.add(parameter);
        return updateRequest;
    }

    /**
     * Creates a new standard job definition.
     *
     * @return the created job definition.
     * @throws Exception if any problems were encountered.
     */
    private JobDefinition createJobDefinition() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Create the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(request, false);

        // Validate the created job definition.
        validateJobDefinition(jobDefinition);

        return jobDefinition;
    }

    /**
     * Validates a standard job definition.
     *
     * @param jobDefinition the job definition to validate.
     */
    private void validateJobDefinition(JobDefinition jobDefinition)
    {
        // Validate the basic job definition fields.
        assertNotNull(jobDefinition);
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, jobDefinition.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, jobDefinition.getJobName());
        assertEquals(JOB_DESCRIPTION, jobDefinition.getDescription());
        assertTrue(jobDefinition.getParameters().size() == 1);

        Parameter parameter = jobDefinition.getParameters().get(0);
        assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, parameter.getName());
        assertEquals(ATTRIBUTE_VALUE_1, parameter.getValue());

        assertEquals(HerdDaoSecurityHelper.SYSTEM_USER, jobDefinition.getLastUpdatedByUserId());
    }

    private S3PropertiesLocation getS3PropertiesLocation()
    {
        S3PropertiesLocation s3PropertiesLocation = new S3PropertiesLocation();
        s3PropertiesLocation.setBucketName("testBucketName");
        s3PropertiesLocation.setKey("testKey");
        return s3PropertiesLocation;
    }

    /**
     * Asserts that when a job definition is created with the given {@link S3PropertiesLocation}, then an exception of the given type and message is thrown.
     *
     * @param s3PropertiesLocation {@link S3PropertiesLocation}
     * @param exceptionType expected exception type
     * @param exceptionMessage expected exception message
     */
    private void testCreateJobDefinitionWithS3PropertiesLocationValidate(S3PropertiesLocation s3PropertiesLocation, Class<? extends Exception> exceptionType,
        String exceptionMessage)
    {
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        JobDefinitionCreateRequest request = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        request.setS3PropertiesLocation(s3PropertiesLocation);

        try
        {
            jobDefinitionService.createJobDefinition(request, false);
            Assert.fail("expected " + exceptionType + ", but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", exceptionType, e.getClass());
            Assert.assertEquals("thrown exception message", exceptionMessage, e.getMessage());
        }
    }
}
