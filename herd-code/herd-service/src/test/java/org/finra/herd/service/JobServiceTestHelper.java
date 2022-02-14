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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.EmrClusterDefinitionDaoTestHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class JobServiceTestHelper
{
    @Autowired
    private EmrClusterDefinitionDaoTestHelper emrClusterDefinitionDaoTestHelper;

    @Autowired
    private JobDefinitionServiceTestHelper jobDefinitionServiceTestHelper;

    @Autowired
    private JobService jobService;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceIamRoleAuthorizationServiceTestHelper namespaceIamRoleAuthorizationServiceTestHelper;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private XmlHelper xmlHelper;

    /**
     * Creates a job based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJob(String activitiXmlClasspathResourceName) throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(activitiXmlClasspathResourceName);
        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME));
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJob(String activitiXmlClasspathResourceName, List<Parameter> parameters) throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(activitiXmlClasspathResourceName);
        // Start the job synchronously.
        return jobService
            .createAndStartJob(createJobCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates job create request using a specified namespace code and job name, but test hard coded parameters will be used.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job definition name.
     *
     * @return the created job create request.
     */
    public JobCreateRequest createJobCreateRequest(String namespaceCd, String jobName)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2);
        parameters.add(parameter);
        parameter = new Parameter("Extra Attribute With No Value", null);
        parameters.add(parameter);

        return createJobCreateRequest(namespaceCd, jobName, parameters);
    }

    /**
     * Creates job create request using test hard coded values.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job definition name.
     * @param parameters the job parameters.
     *
     * @return the created job create request.
     */
    public JobCreateRequest createJobCreateRequest(String namespaceCd, String jobName, List<Parameter> parameters)
    {
        // Create a job create request.
        JobCreateRequest jobCreateRequest = new JobCreateRequest();
        jobCreateRequest.setNamespace(namespaceCd);
        jobCreateRequest.setJobName(jobName);
        jobCreateRequest.setParameters(parameters);
        return jobCreateRequest;
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location and defines a EMR cluster definition.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateCluster(String activitiXmlClasspathResourceName, List<Parameter> parameters, String amiVersion) throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(activitiXmlClasspathResourceName);

        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD);

        // Create a namespace IAM role authorization.
        namespaceIamRoleAuthorizationServiceTestHelper
            .createNamespaceIamRoleAuthorization(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_EC2_NODE_IAM_PROFILE_NAME,
                AbstractServiceTest.TEST_EC2_NODE_IAM_PROFILE_NAME_DESCRIPTION);

        String configXml = IOUtils.toString(resourceLoader.getResource(AbstractServiceTest.EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream(), StandardCharsets.UTF_8);

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(amiVersion);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        EmrClusterDefinitionEntity emrClusterDefinitionEntity =
            emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, AbstractServiceTest.EMR_CLUSTER_DEFINITION_NAME, configXml);

        Parameter parameter = new Parameter("emrClusterDefinitionName", emrClusterDefinitionEntity.getName());
        parameters.add(parameter);
        parameter = new Parameter("namespace", AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD);
        parameters.add(parameter);

        // Start the job synchronously.
        return jobService
            .createAndStartJob(createJobCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location and defines a EMR cluster definition.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateCluster(String activitiXmlClasspathResourceName, List<Parameter> parameters) throws Exception
    {
        return createJobForCreateCluster(activitiXmlClasspathResourceName, parameters, null);
    }

    /**
     * Creates a job based on the specified Activiti XML and defines a EMR cluster definition.
     *
     * @param activitiXml the Activiti XML.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateClusterForActivitiXml(String activitiXml, List<Parameter> parameters) throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinitionForActivitiXml(activitiXml);

        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD);

        // Create a namespace IAM role authorization.
        namespaceIamRoleAuthorizationServiceTestHelper
            .createNamespaceIamRoleAuthorization(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_EC2_NODE_IAM_PROFILE_NAME,
                AbstractServiceTest.TEST_EC2_NODE_IAM_PROFILE_NAME_DESCRIPTION);

        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, AbstractServiceTest.EMR_CLUSTER_DEFINITION_NAME,
                IOUtils.toString(resourceLoader.getResource(AbstractServiceTest.EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream(), StandardCharsets.UTF_8));

        Parameter parameter = new Parameter("namespace", namespaceEntity.getCode());
        parameters.add(parameter);

        parameter = new Parameter("emrClusterDefinitionName", emrClusterDefinitionEntity.getName());
        parameters.add(parameter);

        parameter = new Parameter("dryRun", null);
        parameters.add(parameter);

        parameter = new Parameter("contentType", null);
        parameters.add(parameter);

        parameter = new Parameter("emrClusterDefinitionOverride", null);
        parameters.add(parameter);

        // Start the job synchronously.
        return jobService
            .createAndStartJob(createJobCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates a job based on the specified Activiti XML.
     *
     * @param activitiXml the Activiti XML.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobFromActivitiXml(String activitiXml, List<Parameter> parameters) throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinitionForActivitiXml(activitiXml);
        // Start the job synchronously.
        return jobService
            .createAndStartJob(createJobCreateRequest(AbstractServiceTest.TEST_ACTIVITI_NAMESPACE_CD, AbstractServiceTest.TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Sets specified namespace authorizations for the current user by updating the security context.
     *
     * @param namespace the namespace
     * @param namespacePermissions the list of namespace permissions
     */
    public void setCurrentUserNamespaceAuthorizations(String namespace, List<NamespacePermissionEnum> namespacePermissions)
    {
        String username = AbstractServiceTest.USER_ID;
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(namespace, namespacePermissions));
        applicationUser.setNamespaceAuthorizations(namespaceAuthorizations);
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));
    }

    /**
     * Converts the given list of parameters into a map for easy access.
     *
     * @param parameters the list of parameters
     *
     * @return the map of key-values
     */
    public Map<String, Parameter> toMap(List<Parameter> parameters)
    {
        Map<String, Parameter> map = new HashMap<>();

        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                map.put(parameter.getName(), parameter);
            }
        }

        return map;
    }
}
