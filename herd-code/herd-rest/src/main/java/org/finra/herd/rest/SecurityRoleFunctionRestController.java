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

import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunction;
import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SecurityRoleFunctionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles security role function mapping REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "SecurityRole")
public class SecurityRoleFunctionRestController
{
    @Autowired
    SecurityRoleFunctionService securityRoleFunctionService;

    /**
     * Creates a new security role function mapping
     *
     * @param securityRoleFunctionCreateRequest the information needed to create the security role function mapping
     *
     * @return the created security role function
     */
    @RequestMapping(value = "/securityRoleFunctions", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_POST)
    public SecurityRoleFunction createSecurityRoleFunction(@RequestBody SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest)
    {
        return securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest);
    }

    /**
     * Gets an existing security role function
     *
     * @param securityRoleName required to get the security role information
     * @param securityFunctionName required to get the security function information
     *
     * @return the retrieved security role function
     */
    @RequestMapping(value = "/securityRoleFunctions/securityRoleNames/{securityRoleName}/securityFunctionNames/{securityFunctionName}}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_GET)
    public SecurityRoleFunction getSecurityRoleFunction(@PathVariable("securityRoleName") String securityRoleName,
        @PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.getSecurityRoleFunction(new SecurityRoleKey(securityRoleName), new SecurityFunctionKey(securityFunctionName));
    }

    /**
     * Deletes an existing security role function
     *
     * @param securityRoleName required to get the security role
     * @param securityFunctionName required to get the security function information
     *
     * @return the deleted security role function
     */
    @RequestMapping(value = "/securityRoleFunctions/securityRoleNames/{securityRoleName}/securityFunctionNames/{securityFunctionName}}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_DELETE)
    public SecurityRoleFunction deleteSecurityRoleFunction(@PathVariable("securityRoleName") String securityRoleName,
        @PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.deleteSecurityRoleFunction(new SecurityRoleKey(securityRoleName), new SecurityFunctionKey(securityFunctionName));
    }

    /**
     * Gets a list of security role function keys for all security role function mappings
     *
     * @return the security role function keys
     */
    @RequestMapping(value = "/securityRoleFunctions", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_ALL_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctions()
    {
        return securityRoleFunctionService.getSecurityRoleFunctions();
    }

    /**
     * Get a list of security role function mapping keys based on the security role provided
     *
     * @return the security role function keys
     */
    @RequestMapping(value = "/securityRoleFunctions/securityRoleNames/{securityRoleName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_BY_SECURITY_ROLE_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityRole(@PathVariable("securityRoleName")String securityRoleName)
    {
        return securityRoleFunctionService.getSecurityRoleFunctionsBySecurityRole(new SecurityRoleKey(securityRoleName));
    }

    /**
     * Get a list of security role function mapping keys based on the security function provided
     *
     * @return the security role function keys
     */
    @RequestMapping(value = "/securityRoleFunctions/securityFunctionNames/{securityFunctionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_BY_SECURITY_FUNCTION_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityFunction(@PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.getSecurityRoleFunctionsBySecurityFunction(new SecurityFunctionKey(securityFunctionName));
    }
}
