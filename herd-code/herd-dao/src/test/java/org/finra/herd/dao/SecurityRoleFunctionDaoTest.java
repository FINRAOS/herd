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
import static org.junit.Assert.assertNull;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

public class SecurityRoleFunctionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSecurityRoleFunctionByKey()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = securityRoleFunctionDaoTestHelper.createSecurityRoleFunctionEntity(securityRoleFunctionKey);

        // Get the security role to function mapping entity by its key.
        assertEquals(securityRoleFunctionEntity, securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey));

        // Test case insensitivity of the security role to function mapping key parameters.
        assertEquals(securityRoleFunctionEntity,
            securityRoleFunctionDao.getSecurityRoleFunctionByKey(new SecurityRoleFunctionKey(SECURITY_ROLE.toUpperCase(), SECURITY_FUNCTION.toUpperCase())));
        assertEquals(securityRoleFunctionEntity,
            securityRoleFunctionDao.getSecurityRoleFunctionByKey(new SecurityRoleFunctionKey(SECURITY_ROLE.toLowerCase(), SECURITY_FUNCTION.toLowerCase())));

        // Confirm negative results when using non-existing key parameters.
        assertNull(securityRoleFunctionDao.getSecurityRoleFunctionByKey(new SecurityRoleFunctionKey(I_DO_NOT_EXIST, SECURITY_FUNCTION)));
        assertNull(securityRoleFunctionDao.getSecurityRoleFunctionByKey(new SecurityRoleFunctionKey(SECURITY_ROLE, I_DO_NOT_EXIST)));
    }

    @Test
    public void testGetSecurityRoleFunctionKeys()
    {
        // Create a list of security role to function mapping keys.
        final List<SecurityRoleFunctionKey> securityRoleFunctionKeys = ImmutableList
            .of(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION), new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION_2),
                new SecurityRoleFunctionKey(SECURITY_ROLE_2, SECURITY_FUNCTION), new SecurityRoleFunctionKey(SECURITY_ROLE_2, SECURITY_FUNCTION_2));

        // Create and persist security role to function mapping entities in reverse order.
        for (SecurityRoleFunctionKey securityRoleFunctionKey : Lists.reverse(securityRoleFunctionKeys))
        {
            securityRoleFunctionDaoTestHelper.createSecurityRoleFunctionEntity(securityRoleFunctionKey);
        }

        // Get a list of keys for all security role to function mappings registered in the system.
        assertEquals(securityRoleFunctionKeys, securityRoleFunctionDao.getSecurityRoleFunctionKeys());
    }

    @Test
    public void testGetSecurityRoleFunctionKeysBySecurityFunction()
    {
        // Create a list of security role to function mapping keys.
        final List<SecurityRoleFunctionKey> securityRoleFunctionKeys =
            ImmutableList.of(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION), new SecurityRoleFunctionKey(SECURITY_ROLE_2, SECURITY_FUNCTION));

        // Create and persist security role to function mapping entities in reverse order.
        for (SecurityRoleFunctionKey securityRoleFunctionKey : Lists.reverse(securityRoleFunctionKeys))
        {
            securityRoleFunctionDaoTestHelper.createSecurityRoleFunctionEntity(securityRoleFunctionKey);
        }

        // Get a list of keys for all security role to function mappings for the security function.
        assertEquals(securityRoleFunctionKeys, securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityFunction(SECURITY_FUNCTION));

        // Confirm negative results when using a non-existing security function.
        assertEquals(0, securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityFunction(I_DO_NOT_EXIST).size());
    }

    @Test
    public void testGetSecurityRoleFunctionKeysBySecurityRole()
    {
        // Create a list of security role to function mapping keys.
        final List<SecurityRoleFunctionKey> securityRoleFunctionKeys =
            ImmutableList.of(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION), new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION_2));

        // Create and persist security role to function mapping entities in reverse order.
        for (SecurityRoleFunctionKey securityRoleFunctionKey : Lists.reverse(securityRoleFunctionKeys))
        {
            securityRoleFunctionDaoTestHelper.createSecurityRoleFunctionEntity(securityRoleFunctionKey);
        }

        // Get a list of keys for all security role to function mappings for the security role.
        assertEquals(securityRoleFunctionKeys, securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityRole(SECURITY_ROLE));

        // Confirm negative results when using a non-existing security function.
        assertEquals(0, securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityRole(I_DO_NOT_EXIST).size());
    }
}
