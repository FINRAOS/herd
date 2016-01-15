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

import org.finra.herd.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.EmrClusterDefinitionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles EMR cluster definition REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "EMR Cluster Definition")
public class EmrClusterDefinitionRestController extends HerdBaseController
{
    public static final String EMR_CLUSTER_DEFINITIONS_URI_PREFIX = "/emrClusterDefinitions";

    @Autowired
    private EmrClusterDefinitionService emrClusterDefinitionService;

    /**
     * Creates a new EMR cluster definition.
     *
     * @param request the information needed to create an EMR cluster definition
     *
     * @return the newly created EMR cluster definition
     */
    @RequestMapping(value = EMR_CLUSTER_DEFINITIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_CLUSTER_DEFINITIONS_POST)
    public EmrClusterDefinitionInformation createEmrClusterDefinition(@RequestBody EmrClusterDefinitionCreateRequest request) throws Exception
    {
        return emrClusterDefinitionService.createEmrClusterDefinition(request);
    }

    /**
     * Gets an existing EMR cluster definition by namespace and name.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     *
     * @return the EMR cluster definition
     */
    @RequestMapping(value = EMR_CLUSTER_DEFINITIONS_URI_PREFIX + "/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EMR_CLUSTER_DEFINITIONS_GET)
    public EmrClusterDefinitionInformation getEmrClusterDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName) throws Exception
    {
        return emrClusterDefinitionService.getEmrClusterDefinition(new EmrClusterDefinitionKey(namespace, emrClusterDefinitionName));
    }

    /**
     * Updates an existing EMR cluster definition by namespace and name.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     * @param request the information needed to update the EMR cluster definition
     *
     * @return the updated EMR cluster definition
     */
    @RequestMapping(
        value = EMR_CLUSTER_DEFINITIONS_URI_PREFIX + "/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_CLUSTER_DEFINITIONS_PUT)
    public EmrClusterDefinitionInformation updateEmrClusterDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName, @RequestBody EmrClusterDefinitionUpdateRequest request) throws Exception
    {
        return emrClusterDefinitionService.updateEmrClusterDefinition(new EmrClusterDefinitionKey(namespace, emrClusterDefinitionName), request);
    }

    /**
     * Deletes an existing EMR cluster definition by namespace and name.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     *
     * @return the EMR cluster definition that got deleted
     */
    @RequestMapping(value = EMR_CLUSTER_DEFINITIONS_URI_PREFIX + "/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_EMR_CLUSTER_DEFINITIONS_DELETE)
    public EmrClusterDefinitionInformation deleteEmrClusterDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName) throws Exception
    {
        return emrClusterDefinitionService.deleteEmrClusterDefinition(new EmrClusterDefinitionKey(namespace, emrClusterDefinitionName));
    }
}
