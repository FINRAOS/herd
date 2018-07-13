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

import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SecurityRoleService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles security role REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Security Role")
public class SecurityRoleRestController
{
    private static final String SECURITY_ROLES_URI_PREFIX = "/securityRoles";

    @Autowired
    private SecurityRoleService securityRoleService;

    /**
     * Creates a new security role.
     *
     * @param securityRoleCreateRequest the information needed to create a security role
     *
     * @return the created security role
     */
    @RequestMapping(value = SECURITY_ROLES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SECURITY_ROLES_POST)
    public SecurityRole createSecurityRole(@RequestBody SecurityRoleCreateRequest securityRoleCreateRequest)
    {
        return securityRoleService.createSecurityRole(securityRoleCreateRequest);
    }

    /**
     * Deletes an existing security role.
     *
     * @param securityRoleName the security role name
     *
     * @return the deleted security role
     */
    @RequestMapping(value = SECURITY_ROLES_URI_PREFIX + "/{securityRoleName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_SECURITY_ROLES_DELETE)
    public SecurityRole deleteSecurityRole(@PathVariable("securityRoleName") String securityRoleName)
    {
        return securityRoleService.deleteSecurityRole(new SecurityRoleKey(securityRoleName));
    }

    /**
     * Retrieves an existing security role.
     *
     * @param securityRoleName the security role name
     *
     * @return the retrieved security role
     */
    @RequestMapping(value = SECURITY_ROLES_URI_PREFIX + "/{securityRoleName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLES_GET)
    public SecurityRole getSecurityRole(@PathVariable("securityRoleName") String securityRoleName)
    {
        return securityRoleService.getSecurityRole(new SecurityRoleKey(securityRoleName));
    }

    /**
     * Gets a list of security role keys for all security roles registered in the system. The result list is sorted by security role name in ascending order.
     *
     * @return the list of security role keys
     */
    @RequestMapping(value = SECURITY_ROLES_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SECURITY_ROLES_ALL_GET)
    public SecurityRoleKeys getSecurityRoles()
    {
        return securityRoleService.getSecurityRoles();
    }

    /**
     * Updates an existing security role.
     *
     * @param securityRoleName the security role name
     * @param securityRoleUpdateRequest the information required to update a security role
     *
     * @return the updated security role
     */
    @RequestMapping(value = SECURITY_ROLES_URI_PREFIX + "/{securityRoleName}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_SECURITY_ROLES_PUT)
    public SecurityRole updateSecurityRole(@PathVariable("securityRoleName") String securityRoleName,
        @RequestBody SecurityRoleUpdateRequest securityRoleUpdateRequest)
    {
        return securityRoleService.updateSecurityRole(new SecurityRoleKey(securityRoleName), securityRoleUpdateRequest);
    }
}
