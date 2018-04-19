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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

/**
 * This class tests various functionality within the user namespace authorization REST controller.
 */
public class UserNamespaceAuthorizationServiceTest extends AbstractServiceTest
{
    // Unit tests for createUserNamespaceAuthorization().

    @Test
    public void testCreateUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());

        // Create a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.createUserNamespaceAuthorization(
            new UserNamespaceAuthorizationCreateRequest(key, SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(resultUserNamespaceAuthorization.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testCreateUserNamespaceAuthorizationMissingRequiredParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Try to create a user namespace authorization when user namespace authorization key is not specified.
        try
        {
            userNamespaceAuthorizationService
                .createUserNamespaceAuthorization(new UserNamespaceAuthorizationCreateRequest(null, SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when user namespace authorization key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user namespace authorization key must be specified.", e.getMessage());
        }

        // Try to create a user namespace authorization when user id is not specified.
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(
                new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(BLANK_TEXT, NAMESPACE), SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when user id is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }

        // Try to create a user namespace authorization when namespace is not specified.
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(
                new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(USER_ID, BLANK_TEXT), SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a user namespace authorization when permissions are not specified (passed as null).
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(new UserNamespaceAuthorizationCreateRequest(key, null));
            fail("Should throw an IllegalArgumentException when permissions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace permissions must be specified.", e.getMessage());
        }

        // Try to create a user namespace authorization when permissions are not specified (passed as an empty list).
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(new UserNamespaceAuthorizationCreateRequest(key, Arrays.asList()));
            fail("Should throw an IllegalArgumentException when permissions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace permissions must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateUserNamespaceAuthorizationTrimParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());

        // Create a user namespace authorization using input parameters with leading and trailing empty spaces.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.createUserNamespaceAuthorization(
            new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(addWhitespace(key.getUserId()), addWhitespace(key.getNamespace())),
                SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(resultUserNamespaceAuthorization.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testCreateUserNamespaceAuthorizationUpperCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());

        // Create a user namespace authorization using uppercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.createUserNamespaceAuthorization(
            new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(key.getUserId().toUpperCase(), key.getNamespace().toUpperCase()),
                SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(resultUserNamespaceAuthorization.getId(),
            new UserNamespaceAuthorizationKey(key.getUserId().toUpperCase(), key.getNamespace()), SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testCreateUserNamespaceAuthorizationLowerCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());

        // Create a user namespace authorization using lowercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.createUserNamespaceAuthorization(
            new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(key.getUserId().toLowerCase(), key.getNamespace().toLowerCase()),
                SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(resultUserNamespaceAuthorization.getId(),
            new UserNamespaceAuthorizationKey(key.getUserId().toLowerCase(), key.getNamespace()), SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testCreateUserNamespaceAuthorizationInvalidParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        UserNamespaceAuthorizationCreateRequest request;

        // Try to create a user namespace authorization when user id contains a forward slash character.
        request = new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(addSlash(key.getUserId()), key.getNamespace()),
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(request);
            fail("Should throw an IllegalArgumentException when user id contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("User id can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a user namespace authorization using non-existing namespace.
        request = new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(key.getUserId(), "I_DO_NOT_EXIST"),
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(request);
            fail("Should throw an ObjectNotFoundException when using non-existing user namespace authorization namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getUserNamespaceAuthorizationKey().getNamespace()), e.getMessage());
        }

        // Try to create a user namespace authorization when namespace contains a forward slash character.
        request = new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(key.getUserId(), addSlash(key.getNamespace())),
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(request);
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateUserNamespaceAuthorizationDuplicatePermissions()
    {
        // Try to create a user namespace authorization using duplicate permission values.
        UserNamespaceAuthorizationCreateRequest request = new UserNamespaceAuthorizationCreateRequest(new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE),
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.READ));
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(request);
            fail("Should throw an IllegalArgumentException when using duplicate permission values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate namespace permission \"%s\" is found.", NamespacePermissionEnum.READ.value()), e.getMessage());
        }
    }

    @Test
    public void testCreateUserNamespaceAuthorizationAlreadyExists()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Try to create a user namespace authorization when it already exists.
        try
        {
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(new UserNamespaceAuthorizationCreateRequest(key,
                Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));
            fail("Should throw an AlreadyExistsException when user namespace authorization already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Unable to create user namespace authorization with user id \"%s\" and namespace \"%s\" because it already exists.", key.getUserId(),
                    key.getNamespace()), e.getMessage());
        }
    }

    // Unit tests for updateUserNamespaceAuthorization().

    @Test
    public void testUpdateUserNamespaceAuthorization()
    {
        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = overrideSecurityContext();

        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.GRANT));

        // Update a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key,
            new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key,
            Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)), resultUserNamespaceAuthorization);

        // Revert the update. This is done for the branch unit test coverage.
        resultUserNamespaceAuthorization = userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key,
            new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.GRANT)));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key,
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.GRANT)), resultUserNamespaceAuthorization);

        // Restore the original authentication.
        restoreSecurityContext(originalAuthentication);
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationGrantException()
    {
        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = overrideSecurityContext();

        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Update a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key,
            new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key,
            Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)), resultUserNamespaceAuthorization);

        // Revert the update. This should fail because we are removing our own GRANT permission
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key,
                new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE)));
            fail("Should throw an Exception when user attempts to remove their own GRANT namespace permission.");
        }
        catch (final IllegalArgumentException illegalArgumentException)
        {
            assertEquals("Users are not allowed to remove their own GRANT namespace permission."
                + " Please include the GRANT namespace permission in this request, or have another user remove the GRANT permission.",
                illegalArgumentException.getMessage());
        }

        // Restore the original authentication.
        restoreSecurityContext(originalAuthentication);
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationMissingRequiredParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Try to update a user namespace authorization when user namespace authorization key is not specified.
        try
        {
            userNamespaceAuthorizationService
                .updateUserNamespaceAuthorization(null, new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when user namespace authorization key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user namespace authorization key must be specified.", e.getMessage());
        }

        // Try to update a user namespace authorization when user id is not specified.
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(BLANK_TEXT, NAMESPACE),
                new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when user id is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }

        // Try to update a user namespace authorization when namespace is not specified.
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(USER_ID, BLANK_TEXT),
                new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a user namespace authorization when permissions are not specified (passed as null).
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key, new UserNamespaceAuthorizationUpdateRequest(null));
            fail("Should throw an IllegalArgumentException when permissions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace permissions must be specified.", e.getMessage());
        }

        // Try to update a user namespace authorization when permissions are not specified (passed as an empty list).
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key, new UserNamespaceAuthorizationUpdateRequest(new ArrayList<>()));
            fail("Should throw an IllegalArgumentException when permissions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace permissions must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationTrimParameters()
    {
        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = overrideSecurityContext();

        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Update a user namespace authorization using input parameters with leading and trailing empty spaces.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(addWhitespace(key.getUserId()), addWhitespace(key.getNamespace())),
                new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Restore the original authentication.
        restoreSecurityContext(originalAuthentication);
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationUpperCaseParameters()
    {
        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = overrideSecurityContext();

        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Update a user namespace authorization using uppercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toUpperCase(), key.getNamespace().toUpperCase()),
                new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Restore the original authentication.
        restoreSecurityContext(originalAuthentication);
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationLowerCaseParameters()
    {
        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = overrideSecurityContext();

        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Update a user namespace authorization using lowercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toLowerCase(), key.getNamespace().toLowerCase()),
                new UserNamespaceAuthorizationUpdateRequest(SUPPORTED_NAMESPACE_PERMISSIONS));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Restore the original authentication.
        restoreSecurityContext(originalAuthentication);
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationDuplicatePermissions()
    {
        // Try to update a user namespace authorization using duplicate permission values.
        UserNamespaceAuthorizationUpdateRequest request =
            new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.READ));
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE), request);
            fail("Should throw an IllegalArgumentException when using duplicate permission values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate namespace permission \"%s\" is found.", NamespacePermissionEnum.READ.value()), e.getMessage());
        }
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationNoExists()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Try to update a user namespace authorization when it does not exist.
        try
        {
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key, new UserNamespaceAuthorizationUpdateRequest(
                Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));
            fail("Should throw an ObjectNotFoundException when user namespace authorization does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("User namespace authorization with user id \"%s\" and namespace \"%s\" doesn't exist.", key.getUserId(), key.getNamespace()),
                e.getMessage());
        }
    }

    // Unit tests for getUserNamespaceAuthorization().

    @Test
    public void testGetUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService.getUserNamespaceAuthorization(key);

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorizationMissingRequiredParameters()
    {
        // Try to get a user namespace authorization when user namespace authorization key is not specified.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorization(null);
            fail("Should throw an IllegalArgumentException when user namespace authorization key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user namespace authorization key must be specified.", e.getMessage());
        }

        // Try to get a user namespace authorization when user id is not specified.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(BLANK_TEXT, NAMESPACE));
            fail("Should throw an IllegalArgumentException when user id is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }

        // Try to get a user namespace authorization when namespace is not specified.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(USER_ID, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetUserNamespaceAuthorizationTrimParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get a user namespace authorization using input parameters with leading and trailing empty spaces.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(addWhitespace(key.getUserId()), addWhitespace(key.getNamespace())));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorizationUpperCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get a user namespace authorization using uppercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toUpperCase(), key.getNamespace().toUpperCase()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorizationLowerCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get a user namespace authorization using lowercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toLowerCase(), key.getNamespace().toLowerCase()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorizationNoExists()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Try to get a user namespace authorization when it does not exist.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorization(key);
            fail("Should throw an ObjectNotFoundException when user namespace authorization does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("User namespace authorization with user id \"%s\" and namespace \"%s\" doesn't exist.", key.getUserId(), key.getNamespace()),
                e.getMessage());
        }
    }

    // Unit tests for deleteUserNamespaceAuthorization().

    @Test
    public void testDeleteUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Validate that this user namespace authorization exists.
        assertNotNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));

        // Delete this user namespace authorization.
        UserNamespaceAuthorization deletedUserNamespaceAuthorization = userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(key);

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            deletedUserNamespaceAuthorization);

        // Ensure that this user namespace authorization is no longer there.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationMissingRequiredParameters()
    {
        // Try to delete a user namespace authorization when user namespace authorization key is not specified.
        try
        {
            userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(null);
            fail("Should throw an IllegalArgumentException when user namespace authorization key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user namespace authorization key must be specified.", e.getMessage());
        }

        // Try to delete a user namespace authorization when user id is not specified.
        try
        {
            userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(BLANK_TEXT, NAMESPACE));
            fail("Should throw an IllegalArgumentException when user id is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }

        // Try to delete a user namespace authorization when namespace is not specified.
        try
        {
            userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(USER_ID, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationTrimParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Validate that this user namespace authorization exists.
        assertNotNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));

        // Delete a user namespace authorization using input parameters with leading and trailing empty spaces.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(addWhitespace(key.getUserId()), addWhitespace(key.getNamespace())));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Ensure that this user namespace authorization is no longer there.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationUpperCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Validate that this user namespace authorization exists.
        assertNotNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));

        // Delete a user namespace authorization using uppercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toUpperCase(), key.getNamespace().toUpperCase()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Ensure that this user namespace authorization is no longer there.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationLowerCaseParameters()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key,
            SUPPORTED_NAMESPACE_PERMISSIONS);

        // Validate that this user namespace authorization exists.
        assertNotNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));

        // Delete a user namespace authorization using lowercase input parameters.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationService
            .deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(key.getUserId().toLowerCase(), key.getNamespace().toLowerCase()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);

        // Ensure that this user namespace authorization is no longer there.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationNoExists()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Try to delete a user namespace authorization when it does not exist.
        try
        {
            userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(key);
            fail("Should throw an ObjectNotFoundException when user namespace authorization does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("User namespace authorization with user id \"%s\" and namespace \"%s\" doesn't exist.", key.getUserId(), key.getNamespace()),
                e.getMessage());
        }
    }

    // Unit tests for getUserNamespaceAuthorizationsByUserId().

    @Test
    public void testGetUserNamespaceAuthorizationsByUserId() throws Exception
    {
        // Create user namespace authorization keys. The keys are listed out of order to validate the order by logic.
        List<UserNamespaceAuthorizationKey> keys = Arrays
            .asList(new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE),
                new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE));

        // Create and persist the relative database entities.
        for (UserNamespaceAuthorizationKey key : keys)
        {
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);
        }

        // Get user namespace authorizations for the specified user id.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations = userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(USER_ID);

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(Arrays.asList(
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(0).getId(), keys.get(3),
                SUPPORTED_NAMESPACE_PERMISSIONS),
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(1).getId(), keys.get(2),
                SUPPORTED_NAMESPACE_PERMISSIONS))), resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdMissingRequiredParameters()
    {
        // Try to get a user namespace authorizations when user id is not specified.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(BLANK_TEXT);
            fail("Should throw an IllegalArgumentException when user id is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdTrimParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified user id using user id value with leading and trailing empty spaces.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(addWhitespace(key.getUserId()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdUpperCaseParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified user id using uppercase user id value.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(key.getUserId().toUpperCase());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdLowerCaseParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified user id using lowercase user id value.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(key.getUserId().toLowerCase());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdEmptyList() throws Exception
    {
        // Retrieve an empty list of user namespace authorizations.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId("I_DO_NOT_EXIST");

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(), resultUserNamespaceAuthorizations);
    }

    // Unit tests for getUserNamespaceAuthorizationsByNamespace().

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespace() throws Exception
    {
        // Create user namespace authorization keys. The keys are listed out of order to validate the order by logic.
        List<UserNamespaceAuthorizationKey> keys = Arrays
            .asList(new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE),
                new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE));

        // Create and persist the relative database entities.
        for (UserNamespaceAuthorizationKey key : keys)
        {
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);
        }

        // Get user namespace authorizations for the specified namespace.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations = userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(Arrays.asList(
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(0).getId(), keys.get(3),
                SUPPORTED_NAMESPACE_PERMISSIONS),
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(1).getId(), keys.get(1),
                SUPPORTED_NAMESPACE_PERMISSIONS))), resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceMissingRequiredParameters()
    {
        // Try to get a user namespace authorizations when namespace is not specified.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(BLANK_TEXT);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceTrimParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified namespace using namespace code with leading and trailing empty spaces.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(addWhitespace(key.getNamespace()));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceUpperCaseParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified namespace using uppercase namespace code.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(key.getNamespace().toUpperCase());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceLowerCaseParameters() throws Exception
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get user namespace authorizations for the specified namespace using lowercase namespace code.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(key.getNamespace().toLowerCase());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(
            Arrays.asList(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS))),
            resultUserNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceNamespaceNoExists() throws Exception
    {
        // Try to retrieve user namespace authorizations for a non-existing namespace.
        try
        {
            userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace("I_DO_NOT_EXIST");
            fail("Should throw an ObjectNotFoundException when namespace does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Namespace \"I_DO_NOT_EXIST\" doesn't exist.", e.getMessage());
        }
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespaceEmptyList() throws Exception
    {
        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Retrieve an empty list of user namespace authorizations.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations = userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorizations(), resultUserNamespaceAuthorizations);
    }

    private void restoreSecurityContext(final Authentication originalAuthentication)
    {
        // Restore the original authentication.
        SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
    }

    private Authentication overrideSecurityContext()
    {
        // Create a set of test namespace authorizations.
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        // Create test roles
        List<SecurityRoleEntity> securityRoleEntities = securityRoleDaoTestHelper.createTestSecurityRoles();

        // Fetch the security role codes to add to the application user.
        Set<String> roles = securityRoleEntities.stream().map(SecurityRoleEntity::getCode).collect(Collectors.toSet());

        // Override the security context to return an application user populated with test values.
        final Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();

        SecurityContextHolder.getContext().setAuthentication(new Authentication()
        {
            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException
            {
            }

            @Override
            public boolean isAuthenticated()
            {
                return false;
            }

            @Override
            public Object getPrincipal()
            {
                List<SimpleGrantedAuthority> authorities = Lists.newArrayList(new SimpleGrantedAuthority(SECURITY_FUNCTION));

                ApplicationUser applicationUser = new ApplicationUser(this.getClass());
                applicationUser.setUserId(USER_ID);
                applicationUser.setRoles(roles);
                applicationUser.setNamespaceAuthorizations(namespaceAuthorizations);

                return new SecurityUserWrapper(USER_ID, STRING_VALUE, true, true, true, true, authorities, applicationUser);
            }

            @Override
            public Object getDetails()
            {
                return null;
            }

            @Override
            public Object getCredentials()
            {
                return null;
            }

            @Override
            public Collection<? extends GrantedAuthority> getAuthorities()
            {
                return null;
            }
        });

        return originalAuthentication;
    }
}
