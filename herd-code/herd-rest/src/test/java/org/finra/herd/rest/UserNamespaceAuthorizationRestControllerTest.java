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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

/**
 * This class tests various functionality within the user namespace authorization REST controller.
 */
public class UserNamespaceAuthorizationRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key.
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(key.getNamespace());

        // Create a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationRestController.createUserNamespaceAuthorization(
            new UserNamespaceAuthorizationCreateRequest(key,
                Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(resultUserNamespaceAuthorization.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testUpdateUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(key, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Update a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization = userNamespaceAuthorizationRestController
            .updateUserNamespaceAuthorization(key.getUserId(), key.getNamespace(),
                new UserNamespaceAuthorizationUpdateRequest(Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)));

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key,
            Arrays.asList(NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT)), resultUserNamespaceAuthorization);
    }

    @Test
    public void testGetUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Get a user namespace authorization.
        UserNamespaceAuthorization resultUserNamespaceAuthorization =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorization(key.getUserId(), key.getNamespace());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            resultUserNamespaceAuthorization);
    }

    @Test
    public void testDeleteUserNamespaceAuthorization()
    {
        // Create a user namespace authorization key
        UserNamespaceAuthorizationKey key = new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE);

        // Create and persist the relative database entities.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity =
            userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS);

        // Validate that this user namespace authorization exists.
        assertNotNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));

        // Delete this user namespace authorization.
        UserNamespaceAuthorization deletedUserNamespaceAuthorization =
            userNamespaceAuthorizationRestController.deleteUserNamespaceAuthorization(key.getUserId(), key.getNamespace());

        // Validate the returned object.
        assertEquals(new UserNamespaceAuthorization(userNamespaceAuthorizationEntity.getId(), key, SUPPORTED_NAMESPACE_PERMISSIONS),
            deletedUserNamespaceAuthorization);

        // Ensure that this user namespace authorization is no longer there.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key));
    }

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
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorizationsByUserId(USER_ID);

        // Validate the returned object.
        assertNotNull(resultUserNamespaceAuthorizations);
        assertNotNull(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations());
        assertEquals(2, resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().size());
        assertEquals(new UserNamespaceAuthorizations(Arrays.asList(
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(0).getId(), keys.get(3),
                SUPPORTED_NAMESPACE_PERMISSIONS),
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(1).getId(), keys.get(2),
                SUPPORTED_NAMESPACE_PERMISSIONS))), resultUserNamespaceAuthorizations);
    }

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
        UserNamespaceAuthorizations resultUserNamespaceAuthorizations =
            userNamespaceAuthorizationRestController.getUserNamespaceAuthorizationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertNotNull(resultUserNamespaceAuthorizations);
        assertNotNull(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations());
        assertEquals(2, resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().size());
        assertEquals(new UserNamespaceAuthorizations(Arrays.asList(
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(0).getId(), keys.get(3),
                SUPPORTED_NAMESPACE_PERMISSIONS),
            new UserNamespaceAuthorization(resultUserNamespaceAuthorizations.getUserNamespaceAuthorizations().get(1).getId(), keys.get(1),
                SUPPORTED_NAMESPACE_PERMISSIONS))), resultUserNamespaceAuthorizations);
    }
}
