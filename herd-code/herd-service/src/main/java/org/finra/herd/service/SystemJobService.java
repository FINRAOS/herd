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
package org.finra.herd.service;

import org.quartz.SchedulerException;

import org.finra.herd.model.api.xml.SystemJobRunRequest;
import org.finra.herd.model.api.xml.SystemJobRunResponse;

/**
 * The system job service.
 */
public interface SystemJobService
{
    /**
     * Starts a system job asynchronously.
     *
     * @param request the information needed to run the system job
     *
     * @return the system job information
     * @throws SchedulerException if fails to schedule the system job
     */
    public SystemJobRunResponse runSystemJob(SystemJobRunRequest request) throws SchedulerException;
}
