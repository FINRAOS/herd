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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

public class UserNamespaceAuthorizationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace()
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create user namespace authorisations with user ids in reverse order and with all possible
        // permutations of WRITE and WRITE_DESCRIPTIVE_CONTENT namespace permissions.
        userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(USER_ID_4, namespaceEntity,
            Lists.newArrayList(NamespacePermissionEnum.WRITE, NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_3, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID_2, namespaceEntity, Lists.newArrayList(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));
        userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(USER_ID, namespaceEntity, Lists.newArrayList());

        // Gets a list of user ids for all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT namespace permissions for the test namespace.
        assertEquals(Lists.newArrayList(USER_ID_2, USER_ID_3, USER_ID_4),
            userNamespaceAuthorizationDao.getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(namespaceEntity));

        // Try to retrieve user ids for a namespace that has no user namespace authorisations.
        assertEquals(0, userNamespaceAuthorizationDao
            .getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2)).size());
    }

    @Test
    public void testGetUserNamespaceAuthorizationByKey()
    {
        // Create and persist the relative database entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID, namespaceEntity, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE));

        // Get a user namespace authorization.
        assertEquals(userNamespaceAuthorizationEntity,
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE)));

        // Test case insensitivity of user namespace authorization key.
        assertEquals(userNamespaceAuthorizationEntity, userNamespaceAuthorizationDao
            .getUserNamespaceAuthorizationByKey(new UserNamespaceAuthorizationKey(USER_ID.toUpperCase(), NAMESPACE.toUpperCase())));
        assertEquals(userNamespaceAuthorizationEntity, userNamespaceAuthorizationDao
            .getUserNamespaceAuthorizationByKey(new UserNamespaceAuthorizationKey(USER_ID.toLowerCase(), NAMESPACE.toLowerCase())));

        // Try to retrieve user namespace authorization using invalid input parameters.
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(new UserNamespaceAuthorizationKey("I_DO_NOT_EXIST", NAMESPACE)));
        assertNull(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(new UserNamespaceAuthorizationKey(USER_ID, "I_DO_NOT_EXIST")));
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserId()
    {
        // Create user namespace authorization keys. The keys are listed out of order to validate the order by logic.
        List<UserNamespaceAuthorizationKey> keys = Arrays
            .asList(new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE),
                new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE));

        // Create and persist the relative database entities.
        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities = new ArrayList<>();
        for (UserNamespaceAuthorizationKey key : keys)
        {
            userNamespaceAuthorizationEntities
                .add(userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS));
        }

        // Get user namespace authorizations by user id.
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(2)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(USER_ID));

        // Test case insensitivity of the user id input parameter.
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(2)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(USER_ID.toUpperCase()));
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(2)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(USER_ID.toLowerCase()));

        // Try to retrieve user namespace authorization using a non-existing user id.
        assertEquals(new ArrayList<>(), userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(USER_ID_3));
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByNamespace()
    {
        // Create user namespace authorization keys. The keys are listed out of order to validate the order by logic.
        List<UserNamespaceAuthorizationKey> keys = Arrays
            .asList(new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID_2, NAMESPACE),
                new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE_2), new UserNamespaceAuthorizationKey(USER_ID, NAMESPACE));

        // Create and persist the relative database entities.
        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities = new ArrayList<>();
        for (UserNamespaceAuthorizationKey key : keys)
        {
            userNamespaceAuthorizationEntities
                .add(userNamespaceAuthorizationDaoTestHelper.createUserNamespaceAuthorizationEntity(key, SUPPORTED_NAMESPACE_PERMISSIONS));
        }

        // Get user namespace authorizations by namespace.
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(1)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByNamespace(NAMESPACE));

        // Test case insensitivity of the namespace input parameter.
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(1)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByNamespace(NAMESPACE.toUpperCase()));
        assertEquals(Arrays.asList(userNamespaceAuthorizationEntities.get(3), userNamespaceAuthorizationEntities.get(1)),
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByNamespace(NAMESPACE.toLowerCase()));

        // Try to retrieve user namespace authorization using a non-existing namespace.
        assertEquals(new ArrayList<>(), userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByNamespace(NAMESPACE_3));
    }

    @Test
    public void testGetUserNamespaceAuthorizationsByUserIdStartsWith()
    {
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);
        namespaceDao.saveAndRefresh(namespaceEntity);
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity1 = new UserNamespaceAuthorizationEntity();
        userNamespaceAuthorizationEntity1.setUserId("ab");
        userNamespaceAuthorizationEntity1.setNamespace(namespaceEntity);
        userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity1);
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity2 = new UserNamespaceAuthorizationEntity();
        userNamespaceAuthorizationEntity2.setUserId("ac");
        userNamespaceAuthorizationEntity2.setNamespace(namespaceEntity);
        userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity2);
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity3 = new UserNamespaceAuthorizationEntity();
        userNamespaceAuthorizationEntity3.setUserId("bc");
        userNamespaceAuthorizationEntity3.setNamespace(namespaceEntity);
        userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity3);
        {
            List<UserNamespaceAuthorizationEntity> result = userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserIdStartsWith("a");
            assertEquals(2, result.size());
            assertEquals("ab", result.get(0).getUserId());
            assertEquals("ac", result.get(1).getUserId());
        }
        {
            List<UserNamespaceAuthorizationEntity> result = userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserIdStartsWith("b");
            assertEquals(1, result.size());
            assertEquals("bc", result.get(0).getUserId());
        }
        {
            List<UserNamespaceAuthorizationEntity> result = userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserIdStartsWith("c");
            assertEquals(0, result.size());
        }
    }
}
