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

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE_2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.SecurityRoleEntity;

@Component
public class SecurityRoleDaoTestHelper
{
    @Autowired
    private SecurityRoleDao securityRoleDao;

    /**
     * Creates and persists a security role entity.
     *
     * @param code the name of the security role
     *
     * @return the security role entity
     */
    public SecurityRoleEntity createSecurityRoleEntity(String code)
    {
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(code);
        return securityRoleDao.saveAndRefresh(securityRoleEntity);
    }

    /**
     * Returns a list of test security role entities
     *
     * @return list of security role entities
     */
    private List<SecurityRoleEntity> getTestSecurityRoleEntities()
    {
        SecurityRoleEntity securityRoleEntityOne = new SecurityRoleEntity();
        securityRoleEntityOne.setCode(SECURITY_ROLE);

        SecurityRoleEntity securityRoleEntityTwo = new SecurityRoleEntity();
        securityRoleEntityTwo.setCode(SECURITY_ROLE_2);

        return Arrays.asList(securityRoleEntityOne, securityRoleEntityTwo);
    }

    /**
     * Creates test security role entities.
     */
    public List<SecurityRoleEntity> createTestSecurityRoles()
    {
        List<SecurityRoleEntity> securityRoleEntities = new ArrayList<>();
        for (SecurityRoleEntity securityRoleEntity : getTestSecurityRoleEntities())
        {
            securityRoleEntities.add(createSecurityRoleEntity(securityRoleEntity.getCode()));
        }
        return securityRoleEntities;
    }

    /**
     * Creates and persists a security role entity
     *
     * @param code the name of the security role
     *
     * @param description the description of the security role
     *
     * @return the security role entity
     */
    public SecurityRoleEntity createSecurityRoleEntity(String code,String description)
    {
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(code);
        securityRoleEntity.setDescription(description);
        return securityRoleDao.saveAndRefresh(securityRoleEntity);
    }
}
