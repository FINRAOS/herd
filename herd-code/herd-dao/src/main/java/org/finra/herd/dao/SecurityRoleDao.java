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

import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.jpa.SecurityRoleEntity;

public interface SecurityRoleDao extends BaseJpaDao
{
    /**
     * Gets the list of all security roles entities.
     *
     * @return the list of security role entities
     */
    List<SecurityRoleEntity> getAllSecurityRoles();

    /**
     * Gets a security role by it's name.
     *
     * @param securityRoleName the security role name (case-insensitive)
     *
     * @return the security role for the specified role
     */
    SecurityRoleEntity getSecurityRoleByName(String securityRoleName);

    /**
     * Gets all the security roles in ascending order of security role names.
     *
     * @return the security role for the specified key
     */
    List<SecurityRoleKey> getSecurityRoleKeys();
}
