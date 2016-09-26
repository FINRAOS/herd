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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.Parameter;

@Component
public class JobDefinitionServiceTestHelper
{
    @Autowired
    private JobDefinitionService jobDefinitionService;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private ResourceLoader resourceLoader;

    /**
     * Creates a job definition based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     *
     * @return the job definition.
     * @throws Exception if any errors were encountered.
     */
    public JobDefinition createJobDefinition(String activitiXmlClasspathResourceName) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest(activitiXmlClasspathResourceName);
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Validate the returned object against the input.
        assertEquals(new JobDefinition(jobDefinition.getId(), jobDefinitionCreateRequest.getNamespace(), jobDefinitionCreateRequest.getJobName(),
            jobDefinitionCreateRequest.getDescription(), jobDefinitionCreateRequest.getActivitiJobXml(), jobDefinitionCreateRequest.getParameters(),
            jobDefinitionCreateRequest.getS3PropertiesLocation(), HerdDaoSecurityHelper.SYSTEM_USER), jobDefinition);

        return jobDefinition;
    }

    /**
     * Creates a new job definition create request based on fixed parameters.
     */
    public JobDefinitionCreateRequest createJobDefinitionCreateRequest()
    {
        return createJobDefinitionCreateRequest(null);
    }

    /**
     * Creates a new job definition create request based on fixed parameters and a specified XML resource location.
     *
     * @param activitiXmlClasspathResourceName the classpath resource location to the Activiti XML. If null is specified, then the default
     * ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH will be used.
     */
    public JobDefinitionCreateRequest createJobDefinitionCreateRequest(String activitiXmlClasspathResourceName)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1);
        parameters.add(parameter);

        if (activitiXmlClasspathResourceName == null)
        {
            activitiXmlClasspathResourceName = AbstractServiceTest.ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH;
        }
        try
        {
            return createJobDefinitionCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME,
                AbstractServiceTest.JOB_DESCRIPTION, IOUtils.toString(resourceLoader.getResource(activitiXmlClasspathResourceName).getInputStream()),
                parameters);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Unable to load Activiti XML from classpath resource: " + activitiXmlClasspathResourceName);
        }
    }

    /**
     * Creates a new job definition create request based on user specified parameters.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job name.
     * @param jobDescription the job description.
     * @param activitiXml the Activiti XML.
     * @param parameters the parameters.
     *
     * @return the job definition create request.
     */
    public JobDefinitionCreateRequest createJobDefinitionCreateRequest(String namespaceCd, String jobName, String jobDescription, String activitiXml,
        List<Parameter> parameters)
    {
        // Create and return a new job definition create request.
        JobDefinitionCreateRequest request = new JobDefinitionCreateRequest();
        request.setNamespace(namespaceCd);
        request.setJobName(jobName);
        request.setDescription(jobDescription);
        request.setActivitiJobXml(activitiXml);
        request.setParameters(parameters);
        return request;
    }

    /**
     * Creates a new job definition create request based on fixed parameters and a specified activiti XML.
     *
     * @param activitiXml the Activiti XML.
     */
    public JobDefinitionCreateRequest createJobDefinitionCreateRequestFromActivitiXml(String activitiXml)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE);
        parameters.add(parameter);

        return createJobDefinitionCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME,
            AbstractServiceTest.JOB_DESCRIPTION, activitiXml, parameters);
    }

    /**
     * Creates a job definition based on the specified Activiti XML.
     *
     * @param activitiXml the Activiti XML.
     *
     * @return the job definition.
     * @throws Exception if any errors were encountered.
     */
    public JobDefinition createJobDefinitionForActivitiXml(String activitiXml) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequestFromActivitiXml(activitiXml);
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Validate the returned object against the input.
        assertNotNull(jobDefinition);
        assertTrue(jobDefinition.getNamespace().equals(jobDefinitionCreateRequest.getNamespace()));
        assertTrue(jobDefinition.getJobName().equals(jobDefinitionCreateRequest.getJobName()));
        assertTrue(jobDefinition.getDescription().equals(jobDefinitionCreateRequest.getDescription()));

        return jobDefinition;
    }
}
