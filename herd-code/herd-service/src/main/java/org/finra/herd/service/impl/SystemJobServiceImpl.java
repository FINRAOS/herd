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
package org.finra.herd.service.impl;

import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SystemJobRunRequest;
import org.finra.herd.model.api.xml.SystemJobRunResponse;
import org.finra.herd.service.SystemJobService;
import org.finra.herd.service.helper.ParameterHelper;
import org.finra.herd.service.helper.SystemJobHelper;

/**
 * The system job service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SystemJobServiceImpl implements SystemJobService
{
    @Autowired
    private ParameterHelper parameterHelper;

    @Autowired
    private SystemJobHelper systemJobHelper;

    @Override
    public SystemJobRunResponse runSystemJob(SystemJobRunRequest request) throws SchedulerException
    {
        // Validate and trim the request.
        validateSystemJobRunRequest(request);

        // Schedule the system job to run once right away.
        systemJobHelper.runSystemJob(request.getJobName(), request.getParameters());

        // Create and populate the response.
        SystemJobRunResponse response = new SystemJobRunResponse();
        response.setJobName(request.getJobName());
        response.setParameters(request.getParameters());

        return response;
    }

    /**
     * Validate a system job run request. This method also trims request parameters.
     *
     * @param request the system job run request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateSystemJobRunRequest(SystemJobRunRequest request)
    {
        // Validate and trim the required elements.
        Assert.hasText(request.getJobName(), "A job name must be specified.");
        request.setJobName(request.getJobName().trim());

        // Validate that parameter names are there and that there are no duplicate names.
        parameterHelper.validateParameters(request.getParameters());
    }
}
