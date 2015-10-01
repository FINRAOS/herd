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

import org.apache.oozie.client.WorkflowAction;

/**
 * A POJO implementation of {@link WorkflowAction}. Exposes setters to modify the state of the object.
 */
public class MockOozieWorkflowAction implements WorkflowAction
{
    private String id;
    private String name;
    private String cred;
    private String type;
    private String conf;
    private Status status;
    private int retries;
    private int userRetryCount;
    private int userRetryMax;
    private int userRetryInterval;
    private Date startTime;
    private Date endTime;
    private String transition;
    private String data;
    private String stats;
    private String externalChildIDs;
    private String externalId;
    private String externalStatus;
    private String trackerUri;
    private String consoleUrl;
    private String errorCode;
    private String errorMessage;

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getCred()
    {
        return cred;
    }

    @Override
    public String getType()
    {
        return type;
    }

    @Override
    public String getConf()
    {
        return conf;
    }

    @Override
    public Status getStatus()
    {
        return status;
    }

    @Override
    public int getRetries()
    {
        return retries;
    }

    @Override
    public int getUserRetryCount()
    {
        return userRetryCount;
    }

    @Override
    public int getUserRetryMax()
    {
        return userRetryMax;
    }

    @Override
    public int getUserRetryInterval()
    {
        return userRetryInterval;
    }

    @Override
    public Date getStartTime()
    {
        return startTime;
    }

    @Override
    public Date getEndTime()
    {
        return endTime;
    }

    @Override
    public String getTransition()
    {
        return transition;
    }

    @Override
    public String getData()
    {
        return data;
    }

    @Override
    public String getStats()
    {
        return stats;
    }

    @Override
    public String getExternalChildIDs()
    {
        return externalChildIDs;
    }

    @Override
    public String getExternalId()
    {
        return externalId;
    }

    @Override
    public String getExternalStatus()
    {
        return externalStatus;
    }

    @Override
    public String getTrackerUri()
    {
        return trackerUri;
    }

    @Override
    public String getConsoleUrl()
    {
        return consoleUrl;
    }

    @Override
    public String getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String getErrorMessage()
    {
        return errorMessage;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setCred(String cred)
    {
        this.cred = cred;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public void setConf(String conf)
    {
        this.conf = conf;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }

    public void setRetries(int retries)
    {
        this.retries = retries;
    }

    public void setUserRetryCount(int userRetryCount)
    {
        this.userRetryCount = userRetryCount;
    }

    public void setUserRetryMax(int userRetryMax)
    {
        this.userRetryMax = userRetryMax;
    }

    public void setUserRetryInterval(int userRetryInterval)
    {
        this.userRetryInterval = userRetryInterval;
    }

    public void setStartTime(Date startTime)
    {
        this.startTime = startTime;
    }

    public void setEndTime(Date endTime)
    {
        this.endTime = endTime;
    }

    public void setTransition(String transition)
    {
        this.transition = transition;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public void setStats(String stats)
    {
        this.stats = stats;
    }

    public void setExternalChildIDs(String externalChildIDs)
    {
        this.externalChildIDs = externalChildIDs;
    }

    public void setExternalId(String externalId)
    {
        this.externalId = externalId;
    }

    public void setExternalStatus(String externalStatus)
    {
        this.externalStatus = externalStatus;
    }

    public void setTrackerUri(String trackerUri)
    {
        this.trackerUri = trackerUri;
    }

    public void setConsoleUrl(String consoleUrl)
    {
        this.consoleUrl = consoleUrl;
    }

    public void setErrorCode(String errorCode)
    {
        this.errorCode = errorCode;
    }

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }
}
