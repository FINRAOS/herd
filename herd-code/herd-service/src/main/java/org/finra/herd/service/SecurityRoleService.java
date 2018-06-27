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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;

/**
 * The security Role service.
 */
public interface SecurityRoleService
{
    /**
     * Creates a new security role
     *
     * @param securityRoleCreateRequest the security role create request
     *
     * @return the created security role
     */
    SecurityRole createSecurityRole(SecurityRoleCreateRequest securityRoleCreateRequest);

    /**
     * Gets a security role
     *
     * @param securityRoleKey the security role key
     *
     * @return the retrieved security role
     */
    SecurityRole getSecurityRole(SecurityRoleKey securityRoleKey);

    /**
     * Deletes a security role
     *
     * @param securityRoleName the security role name
     *
     * @return the security role that was deleted
     */
    SecurityRole deleteSecurityRole(SecurityRoleKey securityRoleName);

    /**
     * Updates a security role
     *
     * @param securityRoleKey the security role key
     * @param securityRoleUpdateRequest the security role update request
     *
     * @return the security role that was updated
     */
    SecurityRole updateSecurityRole(SecurityRoleKey securityRoleKey, SecurityRoleUpdateRequest securityRoleUpdateRequest);

    /**
     * Gets a list of security role keys for all security roles defined in the system
     *
     * @return the security role keys
     */
    SecurityRoleKeys getSecurityRoles();
}
