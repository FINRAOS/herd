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
package org.finra.dm.dao;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.Test;

import org.finra.dm.dao.impl.MockOozieOperationsImpl;
import org.finra.dm.dao.impl.OozieDaoImpl;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.Parameter;

/**
 * This class tests the functionality of OozieDao.
 */
public class OozieDaoTest extends AbstractDaoTest
{
    /**
     * Tests the scenario where the job is run.
     */
    @Test
    public void testRunEmrOozieWorkflow()
    {
        String masterIpAddress = "0.0.0.0";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter("param1", "value1"));
        
        try
        {
            String workflowJobId = oozieDao.runOozieWorkflow(masterIpAddress, "s3_workflow_location", parameters);
            Assert.assertNotNull("workflowJobId is null", workflowJobId);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Tests the scenario where the job is run with no parameters.
     */
    @Test
    public void testRunEmrOozieWorkflowNoParams()
    {
        String masterIpAddress = "0.0.0.0";
        
        try
        {
            String workflowJobId = oozieDao.runOozieWorkflow(masterIpAddress, "s3_workflow_location", null);
            Assert.assertNotNull("workflowJobId is null", workflowJobId);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Tests the happy path scenario where workflow job is obtained.
     */
    @Test
    public void testGetEmrOozieWorkflow()
    {
        String masterIpAddress = "0.0.0.0";
        String emrOozieWorkflowId = MockOozieOperationsImpl.CASE_1_JOB_ID;
        try
        {
            WorkflowJob workflowJob = oozieDao.getEmrOozieWorkflow(masterIpAddress, emrOozieWorkflowId);
            Assert.assertNotNull("workflowJob is null", workflowJob);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
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
    public void testGetRunningEmrOozieJobs()
    {
        String masterIpAddress = "0.0.0.0";
        try
        {
            List<WorkflowJob> workflowJobs = oozieDao.getRunningEmrOozieJobsByName(masterIpAddress, null, 1, 50);

            Assert.assertNotNull("workflowJob is null", workflowJobs);
            
            int clientRunningCount = 0;
            int clientNotRunningCount = 0;
            int nonDmWrapperCount = 0;
            
            for(WorkflowJob workflowJob : workflowJobs)
            {
                if(workflowJob.getAppName().equals(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME))
                {
                    if(workflowJob.getActions() != null && workflowJob.getActions().get(0).getExternalId().equals(MockOozieOperationsImpl.CASE_1_CLIENT_JOB_ID))
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
                    nonDmWrapperCount++;
                }
            }
            
            assertTrue(50 == clientRunningCount);
            assertTrue(40 == clientNotRunningCount);
            assertTrue(10 == nonDmWrapperCount);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }
}
