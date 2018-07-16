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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SecurityRoleDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.SecurityRoleEntity;

/**
 * Helper for security role related operations which require DAO.
 */
@Component
public class SecurityRoleDaoHelper
{
    @Autowired
    private SecurityRoleDao securityRoleDao;

    /**
     * Gets a security role entity and ensure it exists.
     *
     * @param securityRoleName the security role name (case insensitive)
     *
     * @return the security role entity
     */
    public SecurityRoleEntity getSecurityRoleEntity(String securityRoleName)
    {
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleName);

        if (securityRoleEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Security role with name \"%s\" doesn't exist.", securityRoleName));
        }

        return securityRoleEntity;
    }
}
