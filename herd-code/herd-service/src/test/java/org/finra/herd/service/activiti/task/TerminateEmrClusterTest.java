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

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the TerminateEmrCluster Activiti task wrapper.
 */
public class TerminateEmrClusterTest extends AbstractServiceTest
{
    /**
     * This method tests the terminate cluster activiti task
     */
    @Test
    public void testTerminateCluster() throws Exception
    {
        List<Parameter> parameters = new ArrayList<>();

        Parameter parameter = new Parameter("clusterName", "testCluster1");
        parameters.add(parameter);

        // Run a job with Activiti XML that will start cluster and terminate.
        Job job = jobServiceTestHelper.createJobForCreateCluster(ACTIVITI_XML_TERMINATE_CLUSTER_WITH_CLASSPATH, parameters);
        assertNotNull(job);
    }
}