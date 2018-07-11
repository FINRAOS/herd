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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.jpa.SecurityRoleEntity;

public class SecurityRoleDaoTest extends AbstractDaoTest
{
    @Autowired
    private CacheManager cacheManager;

    @Test
    public void testGetAllSecurityRoles()
    {
        // Get all security roles
        List<SecurityRoleEntity> securityRoleEntities = securityRoleDao.getAllSecurityRoles();

        // Add new security roles
        securityRoleDaoTestHelper.createTestSecurityRoles();

        List<SecurityRoleEntity> securityRoleEntitiesNew = securityRoleDao.getAllSecurityRoles();

        // Since the roles are cached, the test roles added will not be returned
        assertEquals(securityRoleEntities, securityRoleEntitiesNew);

        // Clear the cache and fetch the roles again
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        securityRoleEntitiesNew = securityRoleDao.getAllSecurityRoles();

        assertNotEquals(securityRoleEntities, securityRoleEntitiesNew);
    }

    @Test
    public void testGetAllSecurityRolesValidateOrder()
    {
        // Add new security roles
        List<SecurityRoleEntity> testSecurityRoleEntities = securityRoleDaoTestHelper.createTestSecurityRoles();

        // Clear the cache and fetch the roles
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Get all security roles
        List<SecurityRoleEntity> securityRoleEntities = securityRoleDao.getAllSecurityRoles();

        // validate that new roles have been added
        assertTrue(securityRoleEntities.containsAll(testSecurityRoleEntities));

        // validate that security roles are returned in a sorted order
        assertTrue(securityRoleEntities.indexOf(testSecurityRoleEntities.get(1)) > securityRoleEntities.indexOf(testSecurityRoleEntities.get(0)));
    }

    @Test
    public void testGetSecurityRoleByName()
    {
        // Create a security role entity.
        SecurityRoleEntity securityRoleEntity = securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Retrieve the security role entity.
        assertEquals(securityRoleEntity, securityRoleDao.getSecurityRoleByName(SECURITY_ROLE));

        // Test case insensitivity of security role name.
        assertEquals(securityRoleEntity, securityRoleDao.getSecurityRoleByName(SECURITY_ROLE.toUpperCase()));
        assertEquals(securityRoleEntity, securityRoleDao.getSecurityRoleByName(SECURITY_ROLE.toLowerCase()));
    }

    @Test
    public void testGetSecurityRoleKeys()
    {
        // Create a list of security role keys.
        final List<SecurityRoleKey> securityRoleKeys = ImmutableList.of(new SecurityRoleKey(SECURITY_ROLE), new SecurityRoleKey(SECURITY_ROLE_2));

        // Create and persist security role entities in reverse order.
        for (SecurityRoleKey securityRoleKey : Lists.reverse(securityRoleKeys))
        {
            securityRoleDaoTestHelper.createSecurityRoleEntity(securityRoleKey.getSecurityRoleName());
        }

        // Get all security roles registered in the system.
        List<SecurityRoleKey> result = securityRoleDao.getSecurityRoleKeys();

        // Validate the returned object.
        assertEquals(securityRoleKeys, result);
    }
}
