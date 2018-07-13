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
package org.finra.herd.dao;

import java.util.List;

import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

public interface SecurityRoleFunctionDao extends BaseJpaDao
{
    /**
     * Gets a security role to function mapping by its key.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     *
     * @return the security role to function mapping entity
     */
    SecurityRoleFunctionEntity getSecurityRoleFunctionByKey(SecurityRoleFunctionKey securityRoleFunctionKey);

    /**
     * Retrieves a list of security role to function mapping keys for all security role to function mappings registered in the system. The result list is sorted
     * by security role name and security function name in ascending order.
     *
     * @return the list of security role to function mapping keys
     */
    List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeys();

    /**
     * Retrieves a list of security role to function mapping keys for the specified security function. The result list is sorted by security role name in
     * ascending order.
     *
     * @param securityFunctionName the security function name
     *
     * @return the list of security role to function mapping keys
     */
    List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeysBySecurityFunction(String securityFunctionName);

    /**
     * Retrieves a list of security role to function mapping keys for the specified security role. The result list is sorted by security function name in
     * ascending order.
     *
     * @param securityRoleName the security role name
     *
     * @return the list of security role to function mapping keys
     */
    List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeysBySecurityRole(String securityRoleName);
}
