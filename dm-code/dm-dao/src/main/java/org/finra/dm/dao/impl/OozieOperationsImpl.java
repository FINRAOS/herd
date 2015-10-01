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

import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import org.finra.dm.dao.OozieOperations;

/**
 * Implementation of the Oozie operations.
 */
public class OozieOperationsImpl implements OozieOperations
{
    /**
     * {@inheritDoc}
     */
    @Override
    public String runOozieWorkflow(OozieClient oozieClient, Properties conf) throws OozieClientException
    {
        return oozieClient.run(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkflowJob getJobInfo(OozieClient oozieClient, String jobId) throws OozieClientException
    {
        return oozieClient.getJobInfo(jobId);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<WorkflowJob> getJobsInfo(OozieClient oozieClient, String filter, int start, int len) throws OozieClientException
    {
        return oozieClient.getJobsInfo(filter, start, len);
    }
}
