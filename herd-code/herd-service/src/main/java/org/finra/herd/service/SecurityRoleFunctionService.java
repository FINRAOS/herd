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
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;

/**
 * The security role to function mapping service.
 */
public interface SecurityRoleFunctionService
{
    /**
     * Creates a new security role to function mapping.
     *
     * @param securityRoleFunctionCreateRequest the information needed to create a security role to function mapping
     *
     * @return the created security role to function mapping
     */
    SecurityRoleFunction createSecurityRoleFunction(SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest);

    /**
     * Deletes an existing security role to function mapping by its key.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     *
     * @return the deleted security role to function mapping
     */
    SecurityRoleFunction deleteSecurityRoleFunction(SecurityRoleFunctionKey securityRoleFunctionKey);

    /**
     * Retrieves an existing security role to function mapping by its key.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     *
     * @return the retrieved security role to function mapping
     */
    SecurityRoleFunction getSecurityRoleFunction(SecurityRoleFunctionKey securityRoleFunctionKey);

    /**
     * Retrieves a list of security role to function mapping keys for all security role to function mappings registered in the system.
     *
     * @return the list of security role to function mapping keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctions();

    /**
     * Retrieves a list of security role to function mapping keys for the specified security function.
     *
     * @param securityFunctionKey the security function key
     *
     * @return the list of security role to function mapping keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityFunction(SecurityFunctionKey securityFunctionKey);

    /**
     * Retrieves a list of security role to function mapping keys for the specified security role.
     *
     * @param securityRoleKey the security role key
     *
     * @return the list of security role to function mapping keys
     */
    SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityRole(SecurityRoleKey securityRoleKey);
}
