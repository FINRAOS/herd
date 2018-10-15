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

import org.finra.herd.model.api.xml.ExternalInterface;
import org.finra.herd.model.api.xml.ExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.ExternalInterfaceKey;
import org.finra.herd.model.api.xml.ExternalInterfaceKeys;
import org.finra.herd.model.api.xml.ExternalInterfaceUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.ExternalInterfaceService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles external interface REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "External Interface")
public class ExternalInterfaceRestController
{
    private static final String EXTERNAL_INTERFACE_URI_PREFIX = "/externalInterfaces";

    @Autowired
    private ExternalInterfaceService externalInterfaceService;

    /**
     * Creates a new external interface.
     *
     * @param request the information needed to create an external interface
     *
     * @return the created external interface
     */
    @RequestMapping(value = EXTERNAL_INTERFACE_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EXTERNAL_INTERFACES_POST)
    public ExternalInterface createExternalInterface(@RequestBody ExternalInterfaceCreateRequest request)
    {
        return externalInterfaceService.createExternalInterface(request);
    }

    /**
     * Deletes an existing external interface by external interface name.
     *
     * @param externalInterfaceName the external interface name
     *
     * @return the deleted external interface
     */
    @RequestMapping(value = EXTERNAL_INTERFACE_URI_PREFIX + "/{externalInterfaceName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_EXTERNAL_INTERFACES_DELETE)
    public ExternalInterface deleteExternalInterface(@PathVariable("externalInterfaceName") String externalInterfaceName)
    {
        return externalInterfaceService.deleteExternalInterface(new ExternalInterfaceKey(externalInterfaceName));
    }

    /**
     * Retrieves an existing external interface by external interface name.
     *
     * @param externalInterfaceName the external interface name
     *
     * @return the retrieved external interface
     */
    @RequestMapping(value = EXTERNAL_INTERFACE_URI_PREFIX + "/{externalInterfaceName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EXTERNAL_INTERFACES_GET)
    public ExternalInterface getExternalInterface(@PathVariable("externalInterfaceName") String externalInterfaceName)
    {
        return externalInterfaceService.getExternalInterface(new ExternalInterfaceKey(externalInterfaceName));
    }

    /**
     * Gets a list of external interface keys for all external interfaces defined in the system. The result list is sorted by external interface name in
     * ascending order.
     *
     * @return the list of external interface keys
     */
    @RequestMapping(value = EXTERNAL_INTERFACE_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EXTERNAL_INTERFACES_ALL_GET)
    public ExternalInterfaceKeys getExternalInterfaces()
    {
        return externalInterfaceService.getExternalInterfaces();
    }

    /**
     * Updates an existing external interface.
     *
     * @param request the information needed to update an external interface
     *
     * @return the updated external interface
     */
    @RequestMapping(value = EXTERNAL_INTERFACE_URI_PREFIX + "/{externalInterfaceName}", method = RequestMethod.PUT, consumes = {"application/xml",
        "application/json"})
    @Secured(SecurityFunctions.FN_EXTERNAL_INTERFACES_PUT)
    public ExternalInterface updateExternalInterface(@PathVariable("externalInterfaceName") String externalInterfaceName,
        @RequestBody ExternalInterfaceUpdateRequest request)
    {
        return externalInterfaceService.updateExternalInterface(new ExternalInterfaceKey(externalInterfaceName), request);
    }
}
