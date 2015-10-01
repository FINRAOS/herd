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

import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

/**
 * Oozie Operations Service.
 */
public interface OozieOperations
{
    /**
     * Runs the ozie workflow.
     * 
     * @param oozieClient the oozie client
     * @param conf the configuraton properties.

     * @return the oozie workflow job id.
     * @throws OozieClientException
     */
    public String runOozieWorkflow(OozieClient oozieClient, Properties conf) throws OozieClientException;

    /**
     * Gets the ozie workflow job info.
     * 
     * @param oozieClient the oozie client
     * @param jobId the oozie workflow job id.
     * @return the workflow job
     * @throws OozieClientException
     */
    public WorkflowJob getJobInfo(OozieClient oozieClient, String jobId) throws OozieClientException;
    
    /**
     * Gets the oozie jobs.

     * @param oozieClient the oozie client
     * @param statuses the statuses to look for, if null, all jobs will be returned. 
     *
     * @return
     * @throws OozieClientException
     */
    public List<WorkflowJob> getJobsInfo(OozieClient oozieClient, String filter, int start, int len) throws OozieClientException;
}
