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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.model.api.xml.JobDefinitionKey;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class JobDefinitionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetJobDefinitionByAltKey()
    {
        // Create a namespace database entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create and persist a job definition entity.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Retrieve the job definition by its key.
        assertEquals(jobDefinitionEntity, jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, JOB_NAME));

        // Retrieve the job definition by its key in uppercase.
        assertEquals(jobDefinitionEntity, jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase()));

        // Retrieve the job definition by its key in lowercase.
        assertEquals(jobDefinitionEntity, jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase()));

        // Try to retrieve a job definition for a non-existing namespace.
        assertNull(jobDefinitionDao.getJobDefinitionByAltKey("I_DO_NOT_EXIST", JOB_NAME));

        // Try to retrieve a job definition for a non-existing job name.
        assertNull(jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetJobDefinitionByAltKeyMultipleRecordsFound()
    {
        // Create a namespace database entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create duplicate job definitions. Please note that we need to pass unique activity ID value.
        for (String jobName : Arrays.asList(JOB_NAME.toUpperCase(), JOB_NAME.toLowerCase()))
        {
            jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, jobName, JOB_DESCRIPTION, jobName + ACTIVITI_ID);
        }

        // Try to retrieve a job definition.
        try
        {
            jobDefinitionDao.getJobDefinitionByAltKey(NAMESPACE, JOB_NAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one Activiti job definition with parameters {namespace=\"%s\", jobName=\"%s\"}.", NAMESPACE, JOB_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetJobDefinitionByProcessDefinitionId()
    {
        // Create a namespace database entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create and persist a job definition entity.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID);

        // Retrieve the job definition by its process definition id.
        assertEquals(jobDefinitionEntity, jobDefinitionDao.getJobDefinitionByProcessDefinitionId(ACTIVITI_ID));

        // Try to retrieve a job definition by its process definition id in uppercase.
        assertNull(jobDefinitionDao.getJobDefinitionByProcessDefinitionId(ACTIVITI_ID.toUpperCase()));

        // Try to retrieve a job definition by its process definition id in lowercase.
        assertNull(jobDefinitionDao.getJobDefinitionByProcessDefinitionId(ACTIVITI_ID.toLowerCase()));

        // Try to retrieve a job definition for a non-existing process definition id.
        assertNull(jobDefinitionDao.getJobDefinitionByProcessDefinitionId("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetJobDefinitionByProcessDefinitionIdMultipleRecordsFound()
    {
        // Create a namespace database entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create two job definitions with the same process definition id.
        for (String jobName : Arrays.asList(JOB_NAME, JOB_NAME_2))
        {
            jobDefinitionDaoTestHelper.createJobDefinitionEntity(namespaceEntity, jobName, JOB_DESCRIPTION, ACTIVITI_ID);
        }

        // Try to retrieve a job definition by its process definition id.
        try
        {
            jobDefinitionDao.getJobDefinitionByProcessDefinitionId(ACTIVITI_ID);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one Activiti job definition with processDefinitionId = \"%s\".", ACTIVITI_ID), e.getMessage());
        }
    }

    @Test
    public void testGetJobDefinitionsByFilterWithMultipleNamespaces()
    {
        // Create and persist job definition entities with namespaces and job names in reverse order to validate the order by clause.
        List<JobDefinitionEntity> jobDefinitionEntities = Arrays
            .asList(jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE_2, JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE_2, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_2),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE, JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_3),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_4));

        // Retrieve job definitions by specifying both namespaces and a job name.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(1)),
            jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE, JOB_NAMESPACE_2), JOB_NAME));

        // Retrieve job definitions by specifying a job name in uppercase.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(1)),
            jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE, JOB_NAMESPACE_2), JOB_NAME.toUpperCase()));

        // Try to retrieve job definitions by specifying namespaces in uppercase.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE.toUpperCase(), JOB_NAMESPACE_2.toUpperCase()), JOB_NAME).isEmpty());

        // Retrieve job definitions by specifying a job name in lowercase.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(1)),
            jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE, JOB_NAMESPACE_2), JOB_NAME.toLowerCase()));

        // Try to retrieve job definitions by specifying namespaces in lowercase.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE.toLowerCase(), JOB_NAMESPACE_2.toLowerCase()), JOB_NAME).isEmpty());

        // Retrieve job definitions by specifying a job name without a namespace.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(1)),
            jobDefinitionDao.getJobDefinitionsByFilter(new ArrayList<>(), JOB_NAME));

        // Retrieve job definitions by specifying a namespace without a job name.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(2)),
            jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE), NO_JOB_NAME));

        // Try to retrieve job definitions by specifying a non-existing namespace.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList("I_DO_NOT_EXIST"), JOB_NAME).isEmpty());

        // Try to retrieve job definitions by specifying a non-existing job name.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter(Arrays.asList(JOB_NAMESPACE), "I_DO_NOT_EXIST").isEmpty());
    }

    @Test
    public void testGetJobDefinitionsByFilterWithSingleNamespace()
    {
        // Create and persist job definition entities with namespaces and job names in reverse order to validate the order by clause.
        List<JobDefinitionEntity> jobDefinitionEntities = Arrays
            .asList(jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE_2, JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE_2, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_2),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE, JOB_NAME_2, JOB_DESCRIPTION, ACTIVITI_ID_3),
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE, JOB_NAME, JOB_DESCRIPTION, ACTIVITI_ID_4));

        // Retrieve job definitions by specifying both a namespace and a job name.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3)), jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE, JOB_NAME));

        // Retrieve job definitions by specifying both a namespace and a job name in uppercase.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3)),
            jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase()));

        // Retrieve job definitions by specifying both a namespace and a job name in lowercase.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3)),
            jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase()));

        // Retrieve job definitions by specifying a job name without a namespace.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(1)),
            jobDefinitionDao.getJobDefinitionsByFilter(NO_NAMESPACE, JOB_NAME));

        // Retrieve job definitions by specifying a namespace without a job name.
        assertEquals(Arrays.asList(jobDefinitionEntities.get(3), jobDefinitionEntities.get(2)),
            jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE, NO_JOB_NAME));

        // Try to retrieve job definitions by specifying a non-existing namespace.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter("I_DO_NOT_EXIST", JOB_NAME).isEmpty());

        // Try to retrieve job definitions by specifying a non-existing job name.
        assertTrue(jobDefinitionDao.getJobDefinitionsByFilter(JOB_NAMESPACE, "I_DO_NOT_EXIST").isEmpty());
    }

    @Test
    public void testGetJobDefinitionKeysByNamespaceEntity()
    {
        // Create two namespace entities.
        List<NamespaceEntity> namespaceEntities =
            Arrays.asList(namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE), namespaceDaoTestHelper.createNamespaceEntity(JOB_NAMESPACE_2));

        // Create two job definitions for the first namespace with job definition names sorted in descending order.
        for (String jobDefinitionName : Lists.newArrayList(JOB_NAME_3, JOB_NAME_2))
        {
            jobDefinitionDaoTestHelper.createJobDefinitionEntity(JOB_NAMESPACE, jobDefinitionName, JOB_DESCRIPTION, ACTIVITI_ID);
        }

        // Retrieve a list of job definition keys. This also validates the sorting order.
        assertEquals(Lists.newArrayList(new JobDefinitionKey(JOB_NAMESPACE, JOB_NAME_2), new JobDefinitionKey(JOB_NAMESPACE, JOB_NAME_3)),
            jobDefinitionDao.getJobDefinitionKeysByNamespaceEntity(namespaceEntities.get(0)));

        // Confirm that no job definition keys get selected when passing wrong namespace entity.
        assertTrue(jobDefinitionDao.getJobDefinitionKeysByNamespaceEntity(namespaceEntities.get(1)).isEmpty());
    }
}
