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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;
import org.finra.herd.model.jpa.SecurityRoleEntity;

/**
 * This class tests security role functionality within the security role service.
 */
public class SecurityRoleServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateSecurityRole() throws Exception
    {
        // Create a security role.
        SecurityRole securityRole = securityRoleService.createSecurityRole(new SecurityRoleCreateRequest(SECURITY_ROLE, DESCRIPTION));

        // Validate the returned object.
        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION), securityRole);
    }

    @Test
    public void testCreateSecurityRoleMissingRequiredParameters()
    {
        // Try to create a security role when security role name is not specified.
        try
        {
            securityRoleService.createSecurityRole(new SecurityRoleCreateRequest(BLANK_TEXT, DESCRIPTION));
            fail("Should throw an IllegalArgumentException when security role name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A security role name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateSecurityRoleMissingOptionalParameters()
    {
        // Try to create a security role when security role description is not provided.
        SecurityRole securityRole = securityRoleService.createSecurityRole(new SecurityRoleCreateRequest(SECURITY_ROLE, null));

        // Validate the response.
        assertEquals(new SecurityRole(SECURITY_ROLE, null), securityRole);
    }

    @Test
    public void testCreateSecurityRoleTrimParameters()
    {
        // Create a security role using input parameters with leading and trailing empty spaces.
        SecurityRole securityRole =
            securityRoleService.createSecurityRole(new SecurityRoleCreateRequest(addWhitespace(SECURITY_ROLE), addWhitespace(DESCRIPTION)));

        // Validate the returned object.
        assertEquals(new SecurityRole(SECURITY_ROLE, addWhitespace(DESCRIPTION)), securityRole);
    }

    @Test
    public void testCreateSecurityRoleAlreadyExists()
    {
        // Create a security role.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        try
        {
            securityRoleService.createSecurityRole(new SecurityRoleCreateRequest(SECURITY_ROLE, DESCRIPTION));
            fail("Should throw an AlreadyExistsException when security role already exists");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create security role \"%s\" because it already exists.", SECURITY_ROLE), e.getMessage());
        }
    }

    @Test
    public void testGetSecurityRole() throws Exception
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Retrieve the security role.
        SecurityRole securityRole = securityRoleService.getSecurityRole(new SecurityRoleKey(SECURITY_ROLE));

        // Validate the returned object.
        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION), securityRole);
    }

    @Test
    public void testGetSecurityRoleThatDoesNotExist()
    {
        try
        {
            // Retrieve the security role that does not exist.
            securityRoleService.getSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
            fail("Should throw ObjectNotFoundException when security role does not exist");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Security role with name \"%s\" doesn't exist.", SECURITY_ROLE), e.getMessage());
        }
    }

    @Test
    public void testGetSecurityRoleTrimParameters()
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Get a security role using input parameters with leading and trailing empty spaces.
        SecurityRole securityRole = securityRoleService.getSecurityRole(new SecurityRoleKey(addWhitespace(SECURITY_ROLE)));

        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION), securityRole);
    }

    @Test
    public void testUpdateSecurityRole() throws Exception
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Update the security role.
        SecurityRole updatedSecurityRole =
            securityRoleService.updateSecurityRole(new SecurityRoleKey(SECURITY_ROLE), new SecurityRoleUpdateRequest(DESCRIPTION_2));

        // Validate the returned object
        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION_2), updatedSecurityRole);
    }

    @Test
    public void testUpdateSecurityRoleThatDoesNotExists()
    {
        try
        {
            securityRoleService.updateSecurityRole(new SecurityRoleKey(SECURITY_ROLE), new SecurityRoleUpdateRequest(DESCRIPTION));
            fail("Should throw ObjectNotFoundException when security role does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Security role with name \"%s\" doesn't exist.", SECURITY_ROLE), e.getMessage());
        }
    }

    @Test
    public void testUpdateSecurityRoleTrimParameters()
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Get a security role using input parameters with leading and trailing empty spaces.
        SecurityRole securityRole = securityRoleService
            .updateSecurityRole(new SecurityRoleKey(addWhitespace(SECURITY_ROLE)), new SecurityRoleUpdateRequest(addWhitespace(DESCRIPTION)));

        assertEquals(new SecurityRole(SECURITY_ROLE, addWhitespace(DESCRIPTION)), securityRole);
    }

    @Test
    public void testDeleteSecurityRole() throws Exception
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Delete the security role.
        SecurityRole securityRole = securityRoleService.deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE));

        // Validate the response.
        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION), securityRole);
    }

    @Test
    public void testDeleteSecurityRoleTrimParameters()
    {
        // Create and persist a security role entity.
        securityRoleDaoTestHelper.createSecurityRoleEntity(SECURITY_ROLE, DESCRIPTION);

        // Get a security role using input parameters with leading and trailing empty spaces.
        SecurityRole securityRole = securityRoleService.deleteSecurityRole(new SecurityRoleKey(addWhitespace(SECURITY_ROLE)));

        assertEquals(new SecurityRole(SECURITY_ROLE, DESCRIPTION), securityRole);
    }

    @Test
    public void testDeleteNonExistingSecurityRole()
    {
        // Try to delete a non-existing security role.
        try
        {
            securityRoleService.deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
            fail("Should throw an ObjectNotFoundException when security role doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Security role with name \"%s\" doesn't exist.", SECURITY_ROLE), e.getMessage());
        }
    }

    @Test
    public void testGetAllSecurityRoles()
    {
        // Create and persist security role entities.
        List<SecurityRoleEntity> testSecurityRoleEntities = securityRoleDaoTestHelper.createTestSecurityRoles();

        // Populate the security role keys from the entities.
        List<SecurityRoleKey> keys =
            testSecurityRoleEntities.stream().map(securityRoleEntity -> new SecurityRoleKey(securityRoleEntity.getCode())).collect(Collectors.toList());
        SecurityRoleKeys securityRoleKeys = new SecurityRoleKeys(keys);

        // Get all the security roles.
        SecurityRoleKeys securityRoles = securityRoleService.getSecurityRoles();

        // Validate the result.
        assertEquals(securityRoleKeys, securityRoles);
    }
}
