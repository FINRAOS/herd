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
package org.finra.herd.rest;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.finra.herd.service.AbstractServiceTest.ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunction;
import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.service.SecurityRoleFunctionService;

public class SecurityRoleFunctionRestControllerTest
{
    @InjectMocks
    private SecurityRoleFunctionRestController securityRoleFunctionRestController;

    @Mock
    private SecurityRoleFunctionService securityRoleFunctionService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityRoleFunction()
    {
        // Create a security role function create request.
        SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest =
            new SecurityRoleFunctionCreateRequest(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION));

        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest)).thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionRestController.createSecurityRoleFunction(securityRoleFunctionCreateRequest);

        // Validate the result.
        assertEquals(securityRoleFunction, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).createSecurityRoleFunction(securityRoleFunctionCreateRequest);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testDeleteSecurityRoleFunction()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, securityRoleFunctionKey);

        // Mock the external calls.
        when(securityRoleFunctionService.deleteSecurityRoleFunction(securityRoleFunctionKey)).thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionRestController.deleteSecurityRoleFunction(SECURITY_ROLE, SECURITY_FUNCTION);

        // Validate the result.
        assertEquals(securityRoleFunction, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).deleteSecurityRoleFunction(securityRoleFunctionKey);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunction()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, securityRoleFunctionKey);

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunction(securityRoleFunctionKey)).thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionRestController.getSecurityRoleFunction(SECURITY_ROLE, SECURITY_FUNCTION);

        // Validate the result.
        assertEquals(securityRoleFunction, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunction(securityRoleFunctionKey);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctions()
    {
        // Create a list of security role to function mapping keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctions()).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionRestController.getSecurityRoleFunctions();

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctions();
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityFunction()
    {
        // Create a security function key.
        SecurityFunctionKey securityFunctionKey = new SecurityFunctionKey(SECURITY_FUNCTION);

        // Create a list of security role to function mapping keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctionsBySecurityFunction(securityFunctionKey)).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionRestController.getSecurityRoleFunctionsBySecurityFunction(SECURITY_FUNCTION);

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctionsBySecurityFunction(securityFunctionKey);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityRole()
    {
        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a list of security role to function mapping keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctionsBySecurityRole(securityRoleKey)).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionRestController.getSecurityRoleFunctionsBySecurityRole(SECURITY_ROLE);

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, result);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctionsBySecurityRole(securityRoleKey);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }
}
