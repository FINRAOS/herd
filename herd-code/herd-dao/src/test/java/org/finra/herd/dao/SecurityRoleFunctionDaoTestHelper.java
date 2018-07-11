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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

@Component
public class SecurityRoleFunctionDaoTestHelper
{
    @Autowired
    private SecurityFunctionDao securityFunctionDao;

    @Autowired
    private SecurityFunctionDaoTestHelper securityFunctionDaoTestHelper;

    @Autowired
    private SecurityRoleDao securityRoleDao;

    @Autowired
    private SecurityRoleDaoTestHelper securityRoleDaoTestHelper;

    @Autowired
    private SecurityRoleFunctionDao securityRoleFunctionDao;

    /**
     * Creates and persists a security role to function mapping entity.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     *
     * @return the security role to function mapping entity
     */
    public SecurityRoleFunctionEntity createSecurityRoleFunctionEntity(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        // Create a security role entity if needed.
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleFunctionKey.getSecurityRoleName());
        if (securityRoleEntity == null)
        {
            securityRoleEntity = securityRoleDaoTestHelper.createSecurityRoleEntity(securityRoleFunctionKey.getSecurityRoleName());
        }

        // Create a security role entity if needed.
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDao.getSecurityFunctionByName(securityRoleFunctionKey.getSecurityFunctionName());
        if (securityFunctionEntity == null)
        {
            securityFunctionEntity = securityFunctionDaoTestHelper.createSecurityFunctionEntity(securityRoleFunctionKey.getSecurityFunctionName());
        }

        return createSecurityRoleFunctionEntity(securityRoleEntity, securityFunctionEntity);
    }

    /**
     * Creates and persists a security role to function mapping entity.
     *
     * @param securityRoleEntity the security role entity
     * @param securityFunctionEntity the security function entity
     *
     * @return the security role to function mapping entity
     */
    public SecurityRoleFunctionEntity createSecurityRoleFunctionEntity(SecurityRoleEntity securityRoleEntity, SecurityFunctionEntity securityFunctionEntity)
    {
        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);

        // Persist and return the entity.
        return securityRoleFunctionDao.saveAndRefresh(securityRoleFunctionEntity);
    }
}
