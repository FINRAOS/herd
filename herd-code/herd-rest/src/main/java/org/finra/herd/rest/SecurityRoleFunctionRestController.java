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
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SecurityRoleFunctionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles security role to function mapping REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Security Role To Function Mapping")
public class SecurityRoleFunctionRestController
{
    private static final String SECURITY_ROLE_FUNCTIONS_URI_PREFIX = "/securityRoleFunctions";

    @Autowired
    private SecurityRoleFunctionService securityRoleFunctionService;

    /**
     * Creates a new security role to function mapping that is identified by security role name and security function name.
     *
     * @param securityRoleFunctionCreateRequest the information needed to create a security role to function mapping
     *
     * @return the created security role to function mapping
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_POST)
    public SecurityRoleFunction createSecurityRoleFunction(@RequestBody SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest)
    {
        return securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest);
    }

    /**
     * Deletes an existing security role to function mapping based on the specified parameters.
     *
     * @param securityRoleName the security role name
     * @param securityFunctionName the security function name
     *
     * @return the deleted security role to function mapping
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX +
        "/securityRoleNames/{securityRoleName}/securityFunctionNames/{securityFunctionName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_DELETE)
    public SecurityRoleFunction deleteSecurityRoleFunction(@PathVariable("securityRoleName") String securityRoleName,
        @PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.deleteSecurityRoleFunction(new SecurityRoleFunctionKey(securityRoleName, securityFunctionName));
    }

    /**
     * Retrieves an existing security role to function mapping based on the specified parameters.
     *
     * @param securityRoleName the security role name
     * @param securityFunctionName the security function name
     *
     * @return the retrieved security role to function mapping
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX +
        "/securityRoleNames/{securityRoleName}/securityFunctionNames/{securityFunctionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_GET)
    public SecurityRoleFunction getSecurityRoleFunction(@PathVariable("securityRoleName") String securityRoleName,
        @PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.getSecurityRoleFunction(new SecurityRoleFunctionKey(securityRoleName, securityFunctionName));
    }

    /**
     * Retrieves a list of security role to function mapping keys for all security role to function mappings registered in the system. The result list is sorted
     * by security role name and security function name in ascending order.
     *
     * @return the list of security role to function mapping keys
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_ALL_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctions()
    {
        return securityRoleFunctionService.getSecurityRoleFunctions();
    }

    /**
     * Retrieves a list of security role to function mapping keys for the specified security function. The result list is sorted by security role name in
     * ascending order.
     *
     * @param securityFunctionName the security function name
     *
     * @return the list of security role to function mapping keys
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX + "/securityFunctionNames/{securityFunctionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_BY_SECURITY_FUNCTION_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityFunction(@PathVariable("securityFunctionName") String securityFunctionName)
    {
        return securityRoleFunctionService.getSecurityRoleFunctionsBySecurityFunction(new SecurityFunctionKey(securityFunctionName));
    }

    /**
     * Retrieves a list of security role to function mapping keys for the specified security role. The result list is sorted by security function name in
     * ascending order.
     *
     * @param securityRoleName the security role name
     *
     * @return the list of security role to function mapping keys
     */
    @RequestMapping(value = SECURITY_ROLE_FUNCTIONS_URI_PREFIX + "/securityRoleNames/{securityRoleName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLE_FUNCTIONS_BY_SECURITY_ROLE_GET)
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityRole(@PathVariable("securityRoleName") String securityRoleName)
    {
        return securityRoleFunctionService.getSecurityRoleFunctionsBySecurityRole(new SecurityRoleKey(securityRoleName));
    }
}
