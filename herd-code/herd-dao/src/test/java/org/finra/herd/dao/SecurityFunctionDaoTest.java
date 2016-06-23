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

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

public class SecurityFunctionDaoTest extends AbstractDaoTest
{
    @Autowired
    private CacheManager cacheManager;

    @Test
    public void testGetSecurityFunctionsByRole() throws Exception
    {
        // Create role and function.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode("TEST_ROLE");
        herdDao.saveAndRefresh(securityRoleEntity);

        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");
        herdDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions = securityFunctionDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Add new role to functions mapping.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);
        herdDao.saveAndRefresh(securityRoleFunctionEntity);

        List<String> functions2 = securityFunctionDao.getSecurityFunctionsForRole("TEST_ROLE");

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        functions2 = securityFunctionDao.getSecurityFunctionsForRole("TEST_ROLE");

        assertNotEquals(functions, functions2);
    }

    @Test
    public void testGetSecurityFunctions() throws Exception
    {
        List<String> functions = securityFunctionDao.getSecurityFunctions();

        // Add a function in functions.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode("FN_UT_SECURITY_FUNCTION");

        herdDao.saveAndRefresh(securityFunctionEntity);

        List<String> functions2 = securityFunctionDao.getSecurityFunctions();

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        functions2 = securityFunctionDao.getSecurityFunctions();

        assertNotEquals(functions, functions2);
    }
}
