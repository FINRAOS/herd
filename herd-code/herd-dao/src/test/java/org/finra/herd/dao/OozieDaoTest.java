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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.dao.impl.MockOozieOperationsImpl;
import org.finra.herd.dao.impl.OozieDaoImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Parameter;

/**
 * This class tests the functionality of OozieDao.
 */
public class OozieDaoTest extends AbstractDaoTest
{
    /**
     * Tests the scenario where the job is run.
     */
    @Test
    public void testRunEmrOozieWorkflow() throws Exception
    {
        String masterIpAddress = "0.0.0.0";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter("param1", "value1"));

        String workflowJobId = oozieDao.runOozieWorkflow(masterIpAddress, "s3_workflow_location", parameters);
        Assert.assertNotNull("workflowJobId is null", workflowJobId);
    }

    /**
     * Tests the scenario where the job is run with no parameters.
     */
    @Test
    public void testRunEmrOozieWorkflowNoParams() throws Exception
    {
        String masterIpAddress = "0.0.0.0";

        String workflowJobId = oozieDao.runOozieWorkflow(masterIpAddress, "s3_workflow_location", null);
        Assert.assertNotNull("workflowJobId is null", workflowJobId);
    }

    /**
     * Tests the happy path scenario where workflow job is obtained.
     */
    @Test
    public void testGetEmrOozieWorkflow() throws Exception
    {
        String masterIpAddress = "0.0.0.0";
        String emrOozieWorkflowId = MockOozieOperationsImpl.CASE_1_JOB_ID;

        WorkflowJob workflowJob = oozieDao.getEmrOozieWorkflow(masterIpAddress, emrOozieWorkflowId);
        Assert.assertNotNull("workflowJob is null", workflowJob);
    }

    /**
     * Tests when the oozie error job does not exist is thrown.
     */
    @Test
    public void testGetEmrOozieWorkflowJobDoesNotExist()
    {
        String masterIpAddress = "0.0.0.0";
        String emrOozieWorkflowId = MockOozieOperationsImpl.CASE_2_JOB_ID;
        try
        {
            oozieDao.getEmrOozieWorkflow(masterIpAddress, emrOozieWorkflowId);
            Assert.fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());

            ObjectNotFoundException objectNotFoundException = (ObjectNotFoundException) e;

            Assert.assertNotNull("objectNotFoundException cause exception is not specified", objectNotFoundException.getCause());
            Assert.assertEquals("objectNotFoundException cause exception type", OozieClientException.class, objectNotFoundException.getCause().getClass());

            OozieClientException oozieClientException = (OozieClientException) objectNotFoundException.getCause();

            Assert.assertEquals("oozieClientException error code", OozieDaoImpl.OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST, oozieClientException.getErrorCode());
        }
    }

    /**
     * Tests the unexpected error is throws from the oozie client.
     */
    @Test
    public void testGetEmrOozieWorkflowUnexpectedError()
    {
        String masterIpAddress = "0.0.0.0";
        String emrOozieWorkflowId = MockOozieOperationsImpl.CASE_3_JOB_ID;
        try
        {
            oozieDao.getEmrOozieWorkflow(masterIpAddress, emrOozieWorkflowId);
            Assert.fail("expected OozieClientException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", OozieClientException.class, e.getClass());

            OozieClientException oozieClientException = (OozieClientException) e;

            Assert.assertNotEquals("oozieClientException error code", OozieDaoImpl.OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST, oozieClientException.getErrorCode());
        }
    }

    @Test
    public void testGetRunningEmrOozieJobsByName() throws Exception
    {
        String masterIpAddress = "0.0.0.0";

        List<WorkflowJob> workflowJobs = oozieDao.getRunningEmrOozieJobsByName(masterIpAddress, JOB_NAME, 1, 50);

        Assert.assertNotNull("workflowJob is null", workflowJobs);

        int clientRunningCount = 0;
        int clientNotRunningCount = 0;
        int nonHerdWrapperCount = 0;

        for (WorkflowJob workflowJob : workflowJobs)
        {
            if (workflowJob.getAppName().equals(OozieDaoImpl.HERD_OOZIE_WRAPPER_WORKFLOW_NAME))
            {
                if (workflowJob.getActions() != null && workflowJob.getActions().get(0).getExternalId().equals(MockOozieOperationsImpl.CASE_1_CLIENT_JOB_ID))
                {
                    clientRunningCount++;
                }
                else
                {
                    clientNotRunningCount++;
                }
            }
            else
            {
                nonHerdWrapperCount++;
            }
        }

        assertTrue(50 == clientRunningCount);
        assertTrue(40 == clientNotRunningCount);
        assertTrue(10 == nonHerdWrapperCount);
    }

    @Test
    public void testGetRunningEmrOozieJobsByNameAppNameNotSpecified() throws Exception
    {
        String masterIpAddress = "0.0.0.0";

        List<WorkflowJob> workflowJobs = oozieDao.getRunningEmrOozieJobsByName(masterIpAddress, null, 1, 50);

        Assert.assertNotNull("workflowJob is null", workflowJobs);

        int clientRunningCount = 0;
        int clientNotRunningCount = 0;
        int nonHerdWrapperCount = 0;

        for (WorkflowJob workflowJob : workflowJobs)
        {
            if (workflowJob.getAppName().equals(OozieDaoImpl.HERD_OOZIE_WRAPPER_WORKFLOW_NAME))
            {
                if (workflowJob.getActions() != null && workflowJob.getActions().get(0).getExternalId().equals(MockOozieOperationsImpl.CASE_1_CLIENT_JOB_ID))
                {
                    clientRunningCount++;
                }
                else
                {
                    clientNotRunningCount++;
                }
            }
            else
            {
                nonHerdWrapperCount++;
            }
        }

        assertTrue(50 == clientRunningCount);
        assertTrue(40 == clientNotRunningCount);
        assertTrue(10 == nonHerdWrapperCount);
    }
}
