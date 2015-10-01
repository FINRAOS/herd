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
import java.util.List;

/**
 * Mock object to hold EMR job flow information.
 */
public class MockEmrJobFlow
{
    private String jobFlowId;
    private String jobFlowName;
    private String status;
    
    private String jarLocation;

    // Steps added to the clusters
    private List<MockEmrJobFlow> steps = new ArrayList<>();

    public String getJobFlowId()
    {
        return jobFlowId;
    }

    public void setJobFlowId(String jobFlowId)
    {
        this.jobFlowId = jobFlowId;
    }

    public String getJobFlowName()
    {
        return jobFlowName;
    }

    public void setJobFlowName(String jobFlowName)
    {
        this.jobFlowName = jobFlowName;
    }

    public String getStatus()
    {
        return status;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public List<MockEmrJobFlow> getSteps()
    {
        return steps;
    }

    public void setSteps(List<MockEmrJobFlow> steps)
    {
        this.steps = steps;
    }

    public String getJarLocation()
    {
        return jarLocation;
    }

    public void setJarLocation(String jarLocation)
    {
        this.jarLocation = jarLocation;
    }

}
