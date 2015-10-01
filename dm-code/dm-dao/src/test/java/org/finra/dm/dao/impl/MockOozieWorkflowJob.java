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

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

public class MockOozieWorkflowJob implements WorkflowJob
{
    private String id;
    private Status status;
    private Properties conf;
    private String appPath;
    private String appName;
    private Date createdTime;
    private Date startTime;
    private Date endTime;
    private String user;
    private String group;
    private String acl;
    private int run;
    private String consoleUrl;
    private String parentId;
    private List<WorkflowAction> actions;
    private String externalId;

    @Override
    public String getAcl()
    {
        return acl;
    }

    @Override
    public List<WorkflowAction> getActions()
    {
        return actions;
    }

    @Override
    public String getAppName()
    {
        return appName;
    }

    @Override
    public String getAppPath()
    {
        return appPath;
    }

    @Override
    public String getConf()
    {
        return conf.toString();
    }

    @Override
    public String getConsoleUrl()
    {
        return consoleUrl;
    }

    @Override
    public Date getCreatedTime()
    {
        return createdTime;
    }

    @Override
    public Date getEndTime()
    {
        return endTime;
    }

    @Override
    public String getExternalId()
    {
        return externalId;
    }

    @Override
    @SuppressWarnings("deprecation")
    public String getGroup()
    {
        return group;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public Date getLastModifiedTime()
    {
        return null;
    }

    @Override
    public String getParentId()
    {
        return parentId;
    }

    @Override
    public int getRun()
    {
        return run;
    }

    @Override
    public Date getStartTime()
    {
        return startTime;
    }

    @Override
    public Status getStatus()
    {
        return status;
    }

    @Override
    public String getUser()
    {
        return user;
    }

    public void setConf(Properties conf)
    {
        this.conf = conf;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }

    public void setAppPath(String appPath)
    {
        this.appPath = appPath;
    }

    public void setAppName(String appName)
    {
        this.appName = appName;
    }

    public void setCreatedTime(Date createdTime)
    {
        this.createdTime = createdTime;
    }

    public void setStartTime(Date startTime)
    {
        this.startTime = startTime;
    }

    public void setEndTime(Date endTime)
    {
        this.endTime = endTime;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setGroup(String group)
    {
        this.group = group;
    }

    public void setAcl(String acl)
    {
        this.acl = acl;
    }

    public void setRun(int run)
    {
        this.run = run;
    }

    public void setConsoleUrl(String consoleUrl)
    {
        this.consoleUrl = consoleUrl;
    }

    public void setParentId(String parentId)
    {
        this.parentId = parentId;
    }

    public void setActions(List<WorkflowAction> actions)
    {
        this.actions = actions;
    }

    public void setExternalId(String externalId)
    {
        this.externalId = externalId;
    }
}
