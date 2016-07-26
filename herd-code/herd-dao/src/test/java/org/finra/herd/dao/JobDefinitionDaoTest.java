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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class JobDefinitionDaoTest extends AbstractDaoTest
{
    /**
     * Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetJobDefinitionByAltKey()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create and persist a job definition entity.
        jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing the app and job details
        JobDefinitionEntity jobDefinitionEntityResult = jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, JOB_NAME);

        // Fail if there is any problem in the result
        assertNotNull(jobDefinitionEntityResult);
        assertEquals(NAMESPACE, jobDefinitionEntityResult.getNamespace().getCode());
        assertEquals(JOB_NAME, jobDefinitionEntityResult.getName());
        assertEquals(JOB_DESCRIPTION, jobDefinitionEntityResult.getDescription());
        assertEquals(ACTIVITI_ID, jobDefinitionEntityResult.getActivitiId());
    }

    /**
     * Tests the scenario by providing a job name that doesn't exist.
     */
    @Test
    public void testGetJobDefinitionByAltKeyJobNameNoExists()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a job definition entity
        jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing the app and a job name that doesn't exist.
        JobDefinitionEntity jobDefinitionEntityResult = jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, JOB_NAME_2);

        // Validate the results.
        assertNull(jobDefinitionEntityResult);
    }

    /**
     * Tests the scenario by finding multiple job definition records.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetJobDefinitionByAltKeyMultipleRecordsFound()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create two job definitions different.
        for (String jobName : Arrays.asList(JOB_NAME.toUpperCase(), JOB_NAME.toLowerCase()))
        {
            // Create a job definition entity. Please note that we need to pass unique activity ID value.
            jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, jobName, JOB_DESCRIPTION, jobName + ACTIVITI_ID);
        }

        // Try to retrieve the the job definition.
        jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, JOB_NAME);
    }

    /**
     * Tests the scenario by providing the wrong app name.
     */
    @Test
    public void testGetJobDefinitionByAltKeyNamespaceNoExists()
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        // Create and persist a new job definition entity.
        jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Call the API to query the newly added entity by providing an namespace code that doesn't exist and a job name that does exist.
        JobDefinitionEntity jobDefinitionEntityResult = jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE_2, JOB_NAME);

        // Validate the results.
        assertNull(jobDefinitionEntityResult);
    }

    /**
     * Tests all possible combinations of input parameters.
     */
    @Test
    public void testGetJobDefinitionsByFilter()
    {
        // Create a namespace database entity.
        List<NamespaceEntity> namespaceEntities =
            Arrays.asList(namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE), namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE_2));

        // Create and persist job definition entities.
        List<JobDefinitionEntity> jobDefinitionEntities = Arrays
            .asList(jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(0), JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(0), JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_2),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(1), JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_3),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(1), JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_4));

        List<JobDefinitionEntity> resultJobDefinitionEntities;

        // Retrieve job definition by specifying both namespace and job name.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE, JOB_NAME);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0)), resultJobDefinitionEntities);

        // Validate input parameters case insensitivity by specifying both namespace and job name in upper and lower case.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase());
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0)), resultJobDefinitionEntities);
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase());
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0)), resultJobDefinitionEntities);

        // Retrieve job definitions by specifying only namespace.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE, NO_JOB_NAME);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0), jobDefinitionEntities.get(1)), resultJobDefinitionEntities);

        // Retrieve job definitions by specifying only job name.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(NO_JOB_NAMESPACE, JOB_NAME);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0), jobDefinitionEntities.get(2)), resultJobDefinitionEntities);

        // Retrieve job definitions by not specifying any input parameters.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(NO_JOB_NAMESPACE, NO_JOB_NAME);
        assertEquals(jobDefinitionEntities, resultJobDefinitionEntities);

        // Try to retrieve job definitions by specifying non-existing namespace and job name.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE_3, JOB_NAME_3);
        assertEquals(new ArrayList<>(), resultJobDefinitionEntities);
    }

    /**
     * Tests all possible combinations of input parameters.
     */
    @Test
    public void testGetJobDefinitionsByFilterWithMultiNamespace()
    {
        // Create a namespace database entity.
        List<NamespaceEntity> namespaceEntities =
            Arrays.asList(namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE), namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE_2));

        // Create and persist job definition entities.
        List<JobDefinitionEntity> jobDefinitionEntities = Arrays
            .asList(jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(0), JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(0), JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_2),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(1), JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_3),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntities.get(1), JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_4));

        List<JobDefinitionEntity> resultJobDefinitionEntities;

        // Retrieve job definition by specifying both namespace and job name.
        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE, JOB_NAMESPACE_2), JOB_NAME);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0), jobDefinitionEntities.get(2)), resultJobDefinitionEntities);

        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(), JOB_NAME);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0), jobDefinitionEntities.get(2)), resultJobDefinitionEntities);

        resultJobDefinitionEntities = jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE), null);
        assertEquals(Arrays.asList(jobDefinitionEntities.get(0), jobDefinitionEntities.get(1)), resultJobDefinitionEntities);
    }
}
