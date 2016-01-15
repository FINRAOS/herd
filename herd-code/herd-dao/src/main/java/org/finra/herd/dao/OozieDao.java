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

import java.util.List;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import org.finra.herd.model.api.xml.Parameter;

/**
 * A DAO for oozie operations.
 */
public interface OozieDao
{
    /**
     * @param masterIpAddress the IP address of oozie master server.
     * @param workflowLocation the location of workflow on S3.
     * @param parameters the configuration properties for the workflow.
     *
     * @return the oozie workflow job id.
     * @throws Exception
     */
    public String runOozieWorkflow(String masterIpAddress, String workflowLocation, List<Parameter> parameters) throws Exception;

    /**
     * Gets the oozie workflow job from oozie server. Returns null if client workflow has not started to run yet.
     *
     * @param masterIpAddress the IP address of oozie master server.
     * @param emrOozieWorkflowId the oozie workflow job id.
     *
     * @return the WorkflowJob object.
     * @throws OozieClientException
     */
    public WorkflowJob getEmrOozieWorkflow(String masterIpAddress, String emrOozieWorkflowId) throws OozieClientException;

    /**
     * Gets the oozie jobs in RUNNING status.
     *
     * @param masterIpAddress the IP address of oozie master server.
     * @param appName the workflow job application name, if no application name is provided returns all workflows.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     *
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException
     */
    public List<WorkflowJob> getRunningEmrOozieJobsByName(String masterIpAddress, String appName, int start, int len) throws OozieClientException;
}
