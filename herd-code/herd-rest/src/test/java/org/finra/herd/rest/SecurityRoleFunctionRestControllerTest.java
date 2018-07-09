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
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE_1;
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
            new SecurityRoleFunctionCreateRequest(new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION));

        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest)).thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction response = securityRoleFunctionRestController.createSecurityRoleFunction(securityRoleFunctionCreateRequest);

        // Validate the result.
        assertEquals(securityRoleFunction, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).createSecurityRoleFunction(securityRoleFunctionCreateRequest);
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testDeleteSecurityRoleFunction()
    {
        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionService.deleteSecurityRoleFunction(new SecurityRoleKey(SECURITY_ROLE_1), new SecurityFunctionKey(SECURITY_FUNCTION)))
            .thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction response = securityRoleFunctionRestController.deleteSecurityRoleFunction(SECURITY_ROLE_1, SECURITY_FUNCTION);

        // Validate the result.
        assertEquals(securityRoleFunction, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).deleteSecurityRoleFunction(new SecurityRoleKey(SECURITY_ROLE_1), new SecurityFunctionKey(SECURITY_FUNCTION));
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunction()
    {
        // Create a security role function.
        SecurityRoleFunction securityRoleFunction = new SecurityRoleFunction(ID, new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunction(new SecurityRoleKey(SECURITY_ROLE_1), new SecurityFunctionKey(SECURITY_FUNCTION)))
            .thenReturn(securityRoleFunction);

        // Call the method under test.
        SecurityRoleFunction response = securityRoleFunctionRestController.getSecurityRoleFunction(SECURITY_ROLE_1, SECURITY_FUNCTION);

        // Validate the response.
        assertEquals(securityRoleFunction, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunction(new SecurityRoleKey(SECURITY_ROLE_1), new SecurityFunctionKey(SECURITY_FUNCTION));
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctions()
    {
        // Create security role function keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctions()).thenReturn(securityRoleFunctionKeys);

        // Retrieve a list of security role function keys.
        SecurityRoleFunctionKeys response = securityRoleFunctionRestController.getSecurityRoleFunctions();

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctions();
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityRole()
    {
        // Create security role function keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctionsBySecurityRole(new SecurityRoleKey(SECURITY_ROLE_1))).thenReturn(securityRoleFunctionKeys);

        // Retrieve a list of security role function keys.
        SecurityRoleFunctionKeys response = securityRoleFunctionRestController.getSecurityRoleFunctionsBySecurityRole(SECURITY_ROLE_1);

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctionsBySecurityRole(new SecurityRoleKey(SECURITY_ROLE_1));
        verifyNoMoreInteractions(securityRoleFunctionService);
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityFunction()
    {
        // Create security role function keys.
        SecurityRoleFunctionKeys securityRoleFunctionKeys =
            new SecurityRoleFunctionKeys(Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE_1, SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityRoleFunctionService.getSecurityRoleFunctionsBySecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION)))
            .thenReturn(securityRoleFunctionKeys);

        // Retrieve a list of security role function keys.
        SecurityRoleFunctionKeys response = securityRoleFunctionRestController.getSecurityRoleFunctionsBySecurityFunction(SECURITY_FUNCTION);

        // Validate the response.
        assertEquals(securityRoleFunctionKeys, response);

        // Verify the external calls.
        verify(securityRoleFunctionService).getSecurityRoleFunctionsBySecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION));
        verifyNoMoreInteractions(securityRoleFunctionService);
    }
}