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
package org.finra.herd.service.activiti.task;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.JobDefinitionService;

@Component
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ExecuteJdbcTestHelper
{
    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    @Autowired
    private JobDefinitionService jobDefinitionService;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    protected ResourceLoader resourceLoader;

    /**
     * Cleans up Herd database after ExecuteJdbcWithReceiveTask test by deleting the test job definition related entities.\
     *
     * @param jobDefinitionNamespace the namespace for the job definition
     * @param jobDefinitionName the name of the job definition
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanUpHerdDatabaseAfterExecuteJdbcWithReceiveTaskTest(String jobDefinitionNamespace, String jobDefinitionName)
    {
        // Retrieve the test job definition entity.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(jobDefinitionNamespace, jobDefinitionName);

        // Delete the test job definition entity.
        if (jobDefinitionEntity != null)
        {
            jobDefinitionDao.delete(jobDefinitionEntity);
        }

        // Retrieve the test namespace entity.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(jobDefinitionNamespace);

        // Delete the test namespace entity.
        if (namespaceEntity != null)
        {
            namespaceDao.delete(namespaceEntity);
        }
    }

    /**
     * Prepares Herd database for ExecuteJdbcWithReceiveTask test by creating and persisting a test job definition entity.
     *
     * @param jobDefinitionNamespace the namespace for the job definition
     * @param jobDefinitionName the name of the job definition
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location
     *
     * @throws Exception
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void prepareHerdDatabaseForExecuteJdbcWithReceiveTaskTest(String jobDefinitionNamespace, String jobDefinitionName,
        String activitiXmlClasspathResourceName) throws Exception
    {
        // Create the test namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(jobDefinitionNamespace);

        // Create a job definition create request.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = new JobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setNamespace(jobDefinitionNamespace);
        jobDefinitionCreateRequest.setJobName(jobDefinitionName);
        jobDefinitionCreateRequest.setDescription("This is a test job definition.");
        jobDefinitionCreateRequest.setActivitiJobXml(IOUtils.toString(resourceLoader.getResource(activitiXmlClasspathResourceName).getInputStream()));
        jobDefinitionCreateRequest.setParameters(null);

        // Create and persist a valid test job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);
    }
}
