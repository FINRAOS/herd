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
package org.finra.dm.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import org.finra.dm.dao.OozieOperations;

/**
 * Mock implementation of Oozie operations.
 */
public class MockOozieOperationsImpl implements OozieOperations
{
    public static final String CASE_1_JOB_ID = "case1";
    public static final String CASE_2_JOB_ID = "case2";
    public static final String CASE_3_JOB_ID = "case3";
    public static final String CASE_4_JOB_ID = "case4";
    public static final String CASE_5_JOB_ID = "case5";
    public static final String CASE_6_JOB_ID = "case6";
    public static final String CASE_1_CLIENT_JOB_ID = "case1_client";

    public static final String CASE_1_JOB_FILTER = "filter1";

    // Created clusters
    private Map<String, WorkflowJob> oozieJobs = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public String runOozieWorkflow(OozieClient oozieClient, Properties conf) throws OozieClientException
    {
        String oozieJobId = getNewOozieJobId();

        MockOozieWorkflowJob workflowJob = new MockOozieWorkflowJob();
        workflowJob.setId(oozieJobId);
        workflowJob.setConf(conf);
        workflowJob.setStatus(WorkflowJob.Status.RUNNING);

        oozieJobs.put(oozieJobId, workflowJob);

        return oozieJobId;
    }

    private String getNewOozieJobId()
    {
        return "UT_OozieId" + String.format("-%.3f", Math.random());
    }

    /**
     * Mock implementation of Oozie get job info.
     * The returned object is pre-constructed based on the jobId given.
     * The jobId are specified as public constants in this class.
     */
    @Override
    public WorkflowJob getJobInfo(OozieClient oozieClient, String jobId) throws OozieClientException
    {
        /*
         * Case1
         * Returns a valid wrapper workflow with a client workflow with job ID CASE_1_CLIENT_JOB_ID, which is also a valid workflow.
         */
        if (CASE_1_JOB_ID.equals(jobId))
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId(jobId);
            mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);

            List<WorkflowAction> workflowActions = new ArrayList<WorkflowAction>();
            {
                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setExternalId(CASE_1_CLIENT_JOB_ID);
                workflowAction.setName(OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW);
                workflowActions.add(workflowAction);
            }
            mockOozieWorkflowJob.setActions(workflowActions);

            return mockOozieWorkflowJob;
        }
        /*
         * Case1 Client
         * The client to case1 wrapper workflow.
         * This workflow is in a running state.
         * Contains 3 actions, each in DONE, RUNNING, and ERROR state
         */
        else if (CASE_1_CLIENT_JOB_ID.equals(jobId))
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId(jobId);
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.RUNNING);
            mockOozieWorkflowJob.setStartTime(new Date());

            List<WorkflowAction> workflowActions = new ArrayList<WorkflowAction>();
            {
                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setId("action1");
                workflowAction.setName("action1");
                workflowAction.setStartTime(new Date());
                workflowAction.setEndTime(new Date());
                workflowAction.setStatus(WorkflowAction.Status.DONE);
                workflowActions.add(workflowAction);
            }
            {
                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setId("action2");
                workflowAction.setName("action2");
                workflowAction.setStartTime(new Date());
                workflowAction.setStatus(WorkflowAction.Status.RUNNING);
                workflowActions.add(workflowAction);
            }
            {
                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setId("action3");
                workflowAction.setName("action3");
                workflowAction.setStartTime(new Date());
                workflowAction.setEndTime(new Date());
                workflowAction.setStatus(WorkflowAction.Status.ERROR);
                workflowAction.setErrorCode("testErrorCode");
                workflowAction.setErrorMessage("testErrorMessage");
                workflowActions.add(workflowAction);
            }
            mockOozieWorkflowJob.setActions(workflowActions);
            return mockOozieWorkflowJob;
        }
        /*
         * Case2
         * Throws an OozieClientException with error code OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST
         */
        else if (CASE_2_JOB_ID.equals(jobId))
        {
            throw new OozieClientException(OozieDaoImpl.OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST, "OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST");
        }
        /*
         * Case3
         * Throws a generic OozieClientException with a bogus error code
         */
        else if (CASE_3_JOB_ID.equals(jobId))
        {
            throw new OozieClientException("TEST_ERROR_CODE", "TEST_ERROR_CODE");
        }
        /*
         * Case4
         * Returns a wrapper workflow without any actions
         */
        else if (CASE_4_JOB_ID.equals(jobId))
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId(jobId);
            mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.RUNNING);
            mockOozieWorkflowJob.setActions(new ArrayList<WorkflowAction>());
            return mockOozieWorkflowJob;
        }
        /*
         * Case5
         * Returns a wrapper workflow with an action which does not correspond to DM wrapper workflow, that is, the client workflow doesn't exist.
         */
        else if (CASE_5_JOB_ID.equals(jobId))
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId(jobId);
            mockOozieWorkflowJob.setAppName("not_a_dm_wrapper_workflow");
            List<WorkflowAction> workflowActions = new ArrayList<WorkflowAction>();
            {
                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setName("TEST_ACTION_NAME");
                workflowActions.add(workflowAction);
            }
            mockOozieWorkflowJob.setActions(workflowActions);
            return mockOozieWorkflowJob;
        }
        /*
         * Case6
         * Returns a wrapper workflow in FAILED status
         */
        else if (CASE_6_JOB_ID.equals(jobId))
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId(jobId);
            mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.FAILED);

            return mockOozieWorkflowJob;
        }
        /*
         * An unknown case. Throws an exception with a hint to developer on how to proceed
         */
        else
        {
            throw new UnsupportedOperationException("jobId '" + jobId
                + "' is unrecognized. Either set up a pre-defined case for this jobId or try a different jobId.");
        }
    }

    @Override
    public List<WorkflowJob> getJobsInfo(OozieClient oozieClient, String filter, int start, int len) throws OozieClientException
    {
        List<WorkflowJob> jobs = new ArrayList<>();

        if (CASE_1_JOB_FILTER.equals(filter))
        {
            for (int i = 0; i < 5; i++)
            {
                MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
                mockOozieWorkflowJob.setId(filter + "_wrapper_" + i);
                mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);

                List<WorkflowAction> workflowActions = new ArrayList<WorkflowAction>();

                MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
                workflowAction.setExternalId(CASE_1_CLIENT_JOB_ID);
                workflowAction.setName(OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW);
                workflowActions.add(workflowAction);

                jobs.add(mockOozieWorkflowJob);
            }

            for (int i = 0; i < 5; i++)
            {
                MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
                mockOozieWorkflowJob.setId(filter + "_client_" + i);
                mockOozieWorkflowJob.setAppName("client_workflow");

                jobs.add(mockOozieWorkflowJob);
            }
        }
        else
        {
            // Fill 25 DM wrapper with client workflow running
            jobs.addAll(getDmWrapperJobWithClientRunning(50));

            // Fill 20 DM wrapper with client workflow not running yet
            jobs.addAll(getDmWrapperJobWithClientNotRunning(40));

            // Fill 5 client jobs
            jobs.addAll(getClientJob(10));
        }

        return jobs;
    }

    private List<WorkflowJob> getDmWrapperJobWithClientRunning(int num)
    {
        List<WorkflowJob> jobs = new ArrayList<>();

        for (int i = 0; i < num; i++)
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId("dm_wrapper_client_running_" + i);
            mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.RUNNING);

            List<WorkflowAction> workflowActions = new ArrayList<WorkflowAction>();

            MockOozieWorkflowAction workflowAction = new MockOozieWorkflowAction();
            workflowAction.setExternalId(CASE_1_CLIENT_JOB_ID);
            workflowAction.setName(OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW);
            workflowActions.add(workflowAction);

            mockOozieWorkflowJob.setActions(workflowActions);

            jobs.add(mockOozieWorkflowJob);
        }

        return jobs;
    }

    private List<WorkflowJob> getDmWrapperJobWithClientNotRunning(int num)
    {
        List<WorkflowJob> jobs = new ArrayList<>();

        for (int i = 0; i < num; i++)
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId("dm_wrapper_client_not_running_" + i);
            mockOozieWorkflowJob.setAppName(OozieDaoImpl.DM_OOZIE_WRAPPER_WORKFLOW_NAME);
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.SUCCEEDED);

            jobs.add(mockOozieWorkflowJob);
        }

        return jobs;
    }

    private List<WorkflowJob> getClientJob(int num)
    {
        List<WorkflowJob> jobs = new ArrayList<>();

        for (int i = 0; i < num; i++)
        {
            MockOozieWorkflowJob mockOozieWorkflowJob = new MockOozieWorkflowJob();
            mockOozieWorkflowJob.setId("client_" + i);
            mockOozieWorkflowJob.setAppName("client_workflow");
            mockOozieWorkflowJob.setStatus(WorkflowJob.Status.RUNNING);

            jobs.add(mockOozieWorkflowJob);
        }

        return jobs;
    }
}
