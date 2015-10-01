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
package org.finra.dm.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.SystemJobRunRequest;
import org.finra.dm.model.api.xml.SystemJobRunResponse;
import org.finra.dm.service.SystemJobService;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles system job REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class SystemJobRestController extends DmBaseController
{
    @Autowired
    private SystemJobService systemJobService;

    /**
     * Starts a system job asynchronously.
     *
     * @param request the information needed to run the system job
     *
     * @return the system job information
     */
    @RequestMapping(value = "/systemJobs", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SYSTEM_JOBS_POST)
    public SystemJobRunResponse runSystemJob(@RequestBody SystemJobRunRequest request) throws Exception
    {
        // Run the system job.
        return systemJobService.runSystemJob(request);
    }
}
