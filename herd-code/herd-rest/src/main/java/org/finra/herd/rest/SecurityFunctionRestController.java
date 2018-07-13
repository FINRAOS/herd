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

import org.finra.herd.model.api.xml.SecurityFunction;
import org.finra.herd.model.api.xml.SecurityFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityFunctionKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SecurityFunctionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles security function REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Security Function")
public class SecurityFunctionRestController
{
    private static final String SECURITY_FUNCTIONS_URI_PREFIX = "/securityFunctions";

    @Autowired
    private SecurityFunctionService securityFunctionService;

    /**
     * Creates a new security function.
     *
     * @param request the information needed to create a security function
     *
     * @return the created security function
     */
    @RequestMapping(value = SECURITY_FUNCTIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SECURITY_FUNCTIONS_POST)
    public SecurityFunction createSecurityFunction(@RequestBody SecurityFunctionCreateRequest request)
    {
        return securityFunctionService.createSecurityFunction(request);
    }

    /**
     * Deletes an existing security function by security function name.
     *
     * @param securityFunctionName the security function name
     *
     * @return the deleted security function
     */
    @RequestMapping(value = SECURITY_FUNCTIONS_URI_PREFIX + "/{securityFunctionName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_SECURITY_FUNCTIONS_DELETE)
    public SecurityFunction deleteSecurityFunction(@PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityFunctionService.deleteSecurityFunction(new SecurityFunctionKey(securityFunctionName));
    }

    /**
     * Retrieves an existing security function by security function name.
     *
     * @param securityFunctionName the security function name
     *
     * @return the retrieved security function
     */
    @RequestMapping(value = SECURITY_FUNCTIONS_URI_PREFIX + "/{securityFunctionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_FUNCTIONS_GET)
    public SecurityFunction getSecurityFunction(@PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityFunctionService.getSecurityFunction(new SecurityFunctionKey(securityFunctionName));
    }

    /**
     * Gets a list of security function keys for all security functions defined in the system. The result list is sorted by security function name in ascending
     * order.
     *
     * @return the list of security function keys
     */
    @RequestMapping(value = SECURITY_FUNCTIONS_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_FUNCTIONS_ALL_GET)
    public SecurityFunctionKeys getSecurityFunctions()
    {
        return securityFunctionService.getSecurityFunctions();
    }
}
