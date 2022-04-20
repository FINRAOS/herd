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

import java.util.Set;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespaceUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceSearchRequest;
import org.finra.herd.model.api.xml.NamespaceSearchResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.NamespaceService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles namespace REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Namespace")
public class NamespaceRestController
{
    @Autowired
    private NamespaceService namespaceService;

    /**
     * Creates a new namespace.
     *
     * @param request the information needed to create the namespace
     *
     * @return the created namespace
     */
    @RequestMapping(value = "/namespaces", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_NAMESPACES_POST)
    public Namespace createNamespace(@RequestBody NamespaceCreateRequest request)
    {
        return namespaceService.createNamespace(request);
    }

    /**
     * Gets an existing namespace by namespace code.
     *
     * @param namespaceCode the namespace code
     *
     * @return the retrieved namespace
     */
    @RequestMapping(value = "/namespaces/{namespaceCode}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_NAMESPACES_GET)
    public Namespace getNamespace(@PathVariable("namespaceCode") String namespaceCode)
    {
        return namespaceService.getNamespace(new NamespaceKey(namespaceCode));
    }

    /**
     * Deletes an existing namespace by namespace code.
     *
     * @param namespaceCode the namespace code
     *
     * @return the namespace that got deleted
     */
    @RequestMapping(value = "/namespaces/{namespaceCode}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_NAMESPACES_DELETE)
    public Namespace deleteNamespace(@PathVariable("namespaceCode") String namespaceCode)
    {
        return namespaceService.deleteNamespace(new NamespaceKey(namespaceCode));
    }

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the list of namespace keys
     */
    @RequestMapping(value = "/namespaces", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_NAMESPACES_ALL_GET)
    public NamespaceKeys getNamespaces()
    {
        return namespaceService.getNamespaces();
    }

    /**
     * Updates an existing namespace.
     *
     * @return the namespace that got updated
     */
    @RequestMapping(value = "/namespaces/{namespaceCode}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_NAMESPACES_PUT)
    public Namespace updateNamespaces(@PathVariable("namespaceCode") String namespaceCode, @RequestBody NamespaceUpdateRequest namespaceUpdateRequest)
    {
        return namespaceService.updateNamespaces(new NamespaceKey(namespaceCode), namespaceUpdateRequest);
    }

    /**
     * Retrieves all namespaces existing in the system per specified search filters and keys.
     *
     * @param namespaceSearchRequest the namespace search request. The request can only accept a single search filter and a single search key
     * @param fields                 the field options for the namespace search response. The valid field options are: chargeCode, s3KeyPrefix
     *
     * @return the namespace search response
     */
    @RequestMapping(value = "/namespaces/search", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_NAMESPACES_SEARCH_POST)
    public NamespaceSearchResponse searchNamespaces(@RequestBody NamespaceSearchRequest namespaceSearchRequest,
        @RequestParam(value = "fields", required = false, defaultValue = "") Set<String> fields)
    {
        return namespaceService.searchNamespaces(namespaceSearchRequest, fields);
    }
}
