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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
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
    public void testGetSecurityFunctionByName()
    {
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION);
        SecurityFunctionEntity securityFunction2Entity = securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION_2.toUpperCase());

        // test the exact match
        SecurityFunctionEntity searchResult = securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION);
        assertEquals(securityFunctionEntity, searchResult);

        // test that the security function name is case insensitive
        searchResult = securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION_2.toLowerCase());
        assertNotEquals(SECURITY_FUNCTION_2.toUpperCase(), SECURITY_FUNCTION_2.toLowerCase());
        assertEquals(SECURITY_FUNCTION_2.toUpperCase(), searchResult.getCode());
        assertEquals(securityFunction2Entity, searchResult);
    }

    @Test
    public void testGetgetSecurityFunctionByNameNotExist()
    {
        securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION);

        assertNull(securityFunctionDao.getSecurityFunctionByName(INVALID_VALUE));
    }

    @Test
    public void testGetSecurityFunctions() throws Exception
    {
        List<String> functions = securityFunctionDao.getSecurityFunctions();

        // Add a function in functions.
        securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION);

        List<String> functions2 = securityFunctionDao.getSecurityFunctions();

        // Since the functions method is cached, the test function will not be retrieved.
        assertEquals(functions, functions2);

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        functions2 = securityFunctionDao.getSecurityFunctions();

        assertNotEquals(functions, functions2);
    }

    @Test
    public void testGetSecurityFunctionsByRole() throws Exception
    {
        // Create role and function.
        SecurityRoleEntity securityRoleEntity = securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE);
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION);

        // Validate that no security functions are returned for the role.
        assertTrue(securityFunctionDao.getSecurityFunctionsForRole(SECURITY_ROLE).isEmpty());

        // Add new role to functions mapping.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);
        herdDao.saveAndRefresh(securityRoleFunctionEntity);

        // Since the functions method is cached, the test function still will not be retrieved.
        assertTrue(securityFunctionDao.getSecurityFunctionsForRole(SECURITY_ROLE).isEmpty());

        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Validate that test security function mapped to the role is now retrieved.
        assertEquals(Arrays.asList(SECURITY_FUNCTION), securityFunctionDao.getSecurityFunctionsForRole(SECURITY_ROLE));
    }

    @Test
    public void testGetUnrestrictedSecurityFunctions() throws Exception
    {
        // Create a role and two functions.
        SecurityRoleEntity securityRoleEntity = securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE);
        List<SecurityFunctionEntity> securityFunctionEntities = Arrays.asList(securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION_3),
            securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION_2),
            securityFunctionDaoTestHelper.createSecurityFunctionEntity(SECURITY_FUNCTION));

        // Retrieve a list of unrestricted functions.
        List<String> resultSecurityFunctions = securityFunctionDao.getUnrestrictedSecurityFunctions();

        // Since none of the security functions is mapped to a security role, the list will contain all three security functions.
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION));
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION_2));
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION_3));

        // Validate the order of retrieved security functions.
        assertTrue(resultSecurityFunctions.indexOf(SECURITY_FUNCTION) < resultSecurityFunctions.indexOf(SECURITY_FUNCTION_2));
        assertTrue(resultSecurityFunctions.indexOf(SECURITY_FUNCTION_2) < resultSecurityFunctions.indexOf(SECURITY_FUNCTION_3));

        // Map the role to the first security function.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntities.get(0));
        herdDao.saveAndRefresh(securityRoleFunctionEntity);

        // Retrieve a list of unrestricted functions.
        resultSecurityFunctions = securityFunctionDao.getUnrestrictedSecurityFunctions();

        // Since the method is cached, all three security functions will be retrieved.
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION));
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION_2));
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION_3));

        // Clear the cache.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Retrieve a list of unrestricted functions.
        resultSecurityFunctions = securityFunctionDao.getUnrestrictedSecurityFunctions();

        // Since the first security function is mapped to a role, only two security functions will be retrieved.
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION));
        assertTrue(resultSecurityFunctions.contains(SECURITY_FUNCTION_2));
        assertFalse(resultSecurityFunctions.contains(SECURITY_FUNCTION_3));
    }
}
