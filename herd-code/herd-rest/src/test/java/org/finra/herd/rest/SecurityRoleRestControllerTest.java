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

import static org.finra.herd.dao.AbstractDaoTest.DESCRIPTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;
import org.finra.herd.service.SecurityRoleService;

public class SecurityRoleRestControllerTest
{
    @InjectMocks
    private SecurityRoleRestController securityRoleRestController;

    @Mock
    private SecurityRoleService securityRoleService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityRole()
    {
        // Create a security role create request.
        SecurityRoleCreateRequest securityRoleCreateRequest = new SecurityRoleCreateRequest(SECURITY_ROLE, DESCRIPTION);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.createSecurityRole(securityRoleCreateRequest)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.createSecurityRole(securityRoleCreateRequest);

        // Validate the result.
        assertEquals(securityRole, result);

        // Verify the external calls.
        verify(securityRoleService).createSecurityRole(securityRoleCreateRequest);
        verifyNoMoreInteractions(securityRoleService);
    }

    @Test
    public void testDeleteSecurityRole()
    {
        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE))).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.deleteSecurityRole(SECURITY_ROLE);

        // Validate the result.
        assertEquals(securityRole, result);

        // Verify the external calls.
        verify(securityRoleService).deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
        verifyNoMoreInteractions(securityRoleService);
    }

    @Test
    public void testGetSecurityRole()
    {
        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.getSecurityRole(securityRoleKey)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.getSecurityRole(SECURITY_ROLE);

        // Validate the result.
        assertEquals(securityRole, result);

        // Verify the external calls.
        verify(securityRoleService).getSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
        verifyNoMoreInteractions(securityRoleService);
    }

    @Test
    public void testGetSecurityRoles()
    {
        // Create security role keys.
        SecurityRoleKeys securityRoleKeys = new SecurityRoleKeys(Lists.newArrayList(new SecurityRoleKey(SECURITY_ROLE)));

        // Mock the external calls.
        when(securityRoleService.getSecurityRoles()).thenReturn(securityRoleKeys);

        // Call the method under test.
        SecurityRoleKeys result = securityRoleRestController.getSecurityRoles();

        // Validate the result.
        assertEquals(securityRoleKeys, result);

        // Verify the external calls.
        verify(securityRoleService).getSecurityRoles();
        verifyNoMoreInteractions(securityRoleService);
    }

    @Test
    public void testUpdateSecurityRole()
    {
        // Create a security role update request.
        SecurityRoleUpdateRequest securityRoleUpdateRequest = new SecurityRoleUpdateRequest(DESCRIPTION);

        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.updateSecurityRole(securityRoleKey, securityRoleUpdateRequest)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.updateSecurityRole(SECURITY_ROLE, securityRoleUpdateRequest);

        // Validate the result.
        assertEquals(securityRole, result);

        // Verify the external calls.
        verify(securityRoleService).updateSecurityRole(securityRoleKey, securityRoleUpdateRequest);
        verifyNoMoreInteractions(securityRoleService);
    }
}
