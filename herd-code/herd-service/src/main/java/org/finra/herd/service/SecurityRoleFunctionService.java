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

import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunction;
import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;

/**
 * The security role function service.
 */
public interface SecurityRoleFunctionService
{
    /**
     * Creates a new security role function.
     *
     * @param securityRoleFunctionCreateRequest the security role function create request
     *
     * @return the created security role function
     */
    SecurityRoleFunction createSecurityRoleFunction(SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest);

    /**
     * Gets an existing security role function for the specified key parameters.
     *
     * @param securityRoleKey the security role key
     * @param securityFunctionKey the security function key
     *
     * @return the security role function
     */
    SecurityRoleFunction getSecurityRoleFunction(SecurityRoleKey securityRoleKey, SecurityFunctionKey securityFunctionKey);

    /**
     * Deletes a security role function for the specified key parameters.
     *
     * @param securityRoleKey the security role key
     * @param securityFunctionKey the security function key
     *
     * @return the security role function that was deleted
     */
    SecurityRoleFunction deleteSecurityRoleFunction(SecurityRoleKey securityRoleKey, SecurityFunctionKey securityFunctionKey);

    /**
     * Gets a list of all security role function keys.
     *
     * @return the security role function keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctions();

    /**
     * Gets a list of all security role function keys for the specified security role key.
     *
     * @param securityRoleKey the security role key
     *
     * @return the security role function keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityRole(SecurityRoleKey securityRoleKey);

    /**
     * Gets a list of all security role function keys for the specified security function key.
     *
     * @param securityFunctionKey the security function key
     *
     * @return the security role function keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityFunction(SecurityFunctionKey securityFunctionKey);
}
