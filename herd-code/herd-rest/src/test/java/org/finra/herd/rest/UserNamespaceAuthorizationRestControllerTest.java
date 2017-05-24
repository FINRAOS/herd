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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;
import org.finra.herd.service.UserNamespaceAuthorizationService;

/**
 * This class tests various functionality within the user namespace authorization REST controller.
 */
public class UserNamespaceAuthorizationRestControllerTest extends AbstractRestTest
{
    @Mock
    private UserNamespaceAuthorizationService userNamespaceAuthorizationService;

    @InjectMocks
    private UserNamespaceAuthorizationRestController userNamespaceAuthorizationRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        UserNamespaceAuthorizationCreateRequest request = new UserNamespaceAuthorizationCreateRequest(key,
            Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));

        UserNamespaceAuthorization userNamespaceAuthorization = new UserNamespaceAuthorization(100, key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Mock calls to external method.
        when(userNamespaceAuthorizationService.createUserNamespaceAuthorization(request)).thenReturn(userNamespaceAuthorization);

        UserNamespaceAuthorization response = userNamespaceAuthorizationRestController.createUserNamespaceAuthorization(request);

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).createUserNamespaceAuthorization(request);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(userNamespaceAuthorization, response);
    }

    @Test
    public void testUpdateUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        UserNamespaceAuthorization userNamespaceAuthorization =
            new UserNamespaceAuthorization(100, key, Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));

        UserNamespaceAuthorizationUpdateRequest request =
            new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT));
        // Mock calls to external method.
        when(userNamespaceAuthorizationService.updateUserNamespaceAuthorization(key, request)).thenReturn(userNamespaceAuthorization);

        // Update a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization =
            userNamespaceAuthorizationRestController.updateUserNamespaceAuthorization(key.getUserId(), key.getNamespace(), request);

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).updateUserNamespaceAuthorization(key, request);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(userNamespaceAuthorization, resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        UserNamespaceAuthorization userNamespaceAuthorization = new UserNamespaceAuthorization(100, key, SUPPORTED_NAMESPACE_PERMISSIONS);

        when(userNamespaceAuthorizationService.getUserNamespaceAuthorization(key)).thenReturn(userNamespaceAuthorization);

        // Get a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorization(key.getUserId(), key.getNamespace());

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).getUserNamespaceAuthorization(key);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(userNamespaceAuthorization, resultUserNamespaceAuthorization);
    }

    @Test
    public void testDeleteUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        UserNamespaceAuthorization userNamespaceAuthorization = new UserNamespaceAuthorization(100, key, SUPPORTED_NAMESPACE_PERMISSIONS);

        when(userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(key)).thenReturn(userNamespaceAuthorization);

        // Delete this user namespace authorization.
        UserNamespaceAuthorization deletedUserNamespaceAuthorization =
            userNamespaceAuthorizationRestController.deleteUserNamespaceAuthorization(key.getUserId(), key.getNamespace());

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).deleteUserNamespaceAuthorization(key);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(userNamespaceAuthorization, deletedUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserId() throws Exception
    {
        UserNamespaceAuthorizations userNamespaceAuthorizations = new UserNamespaceAuthorizations();

        when(userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(USER_ID)).thenReturn(userNamespaceAuthorizations);

        // Get user namespace authorizations for the specified user id.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorizationsByUserId(USER_ID);

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).getUserNamespaceAuthorizationsByUserId(USER_ID);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(resultUserNamespaceAuthorizations, userNamespaceAuthorizations);
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespace() throws Exception
    {
        UserNamespaceAuthorizations userNamespaceAuthorizations = new UserNamespaceAuthorizations();

        when(userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(USER_ID)).thenReturn(userNamespaceAuthorizations);

        // Get user namespace authorizations for the specified user id.
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorizationsByNamespace(USER_ID);

        // Verify the external calls.
        verify(userNamespaceAuthorizationService).getUserNamespaceAuthorizationsByNamespace(USER_ID);
        verifyNoMoreInteractions(userNamespaceAuthorizationService);

        // Validate the returned object.
        assertEquals(resultUserNamespaceAuthorizations, userNamespaceAuthorizations);
    }
}
