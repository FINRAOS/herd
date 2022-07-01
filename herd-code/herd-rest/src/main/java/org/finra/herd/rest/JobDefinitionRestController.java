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
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDefinitionKeys;
import org.finra.herd.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.JobDefinitionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles job definition REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Job Definition")
public class JobDefinitionRestController extends HerdBaseController
{
    @Autowired
    private JobDefinitionService jobDefinitionService;

    /**
     * Creates a new job definition.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param request the information needed to create the job definition.
     *
     * @return the created job definition.
     */
    @RequestMapping(value = "/jobDefinitions", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_JOB_DEFINITIONS_POST)
    public JobDefinition createJobDefinition(@RequestBody JobDefinitionCreateRequest request) throws Exception
    {
        return jobDefinitionService.createJobDefinition(request, true);
    }

    /**
     * Updates an existing job definition.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace of the job definition.
     * @param jobName the job name of the job definition.
     * @param request the information needed to update the job definition.
     *
     * @return the updated job definition.
     */
    @RequestMapping(value = "/jobDefinitions/namespaces/{namespace}/jobNames/{jobName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_JOB_DEFINITIONS_PUT)
    public JobDefinition updateJobDefinition(@PathVariable("namespace") String namespace, @PathVariable("jobName") String jobName,
        @RequestBody JobDefinitionUpdateRequest request) throws Exception
    {
        return jobDefinitionService.updateJobDefinition(namespace, jobName, request, true);
    }

    /**
     * Gets an existing job definition.
     * <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace of the job definition.
     * @param jobName the job name of the job definition.
     *
     * @return the job definition.
     */
    @RequestMapping(value = "/jobDefinitions/namespaces/{namespace}/jobNames/{jobName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_JOB_DEFINITIONS_GET)
    public JobDefinition getJobDefinition(@PathVariable("namespace") String namespace, @PathVariable("jobName") String jobName) throws Exception
    {
        return jobDefinitionService.getJobDefinition(namespace, jobName);
    }

    /**
     * Gets a list of keys for all job definitions defined in the system for the specified namespace.
     * <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace of the job definition
     *
     * @return the job definition keys
     */
    @RequestMapping(value = "/jobDefinitions/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_JOB_DEFINITIONS_ALL_GET)
    public JobDefinitionKeys getJobDefinitionKeys(@PathVariable("namespace") String namespace)
    {
        return jobDefinitionService.getJobDefinitionKeys(namespace);
    }
}
