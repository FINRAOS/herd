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

import org.junit.Test;

import org.finra.herd.model.jpa.UserEntity;

public class UserDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetUserByUserId()
    {
        // Create and persist the relative database entities.
        UserEntity userEntity = createUserEntity(USER_ID, true);

        // Get a user.
        assertEquals(userEntity, userDao.getUserByUserId(USER_ID));

        // Test case insensitivity of user id.
        assertEquals(userEntity, userDao.getUserByUserId(USER_ID.toUpperCase()));
        assertEquals(userEntity, userDao.getUserByUserId(USER_ID.toLowerCase()));

        // Try to retrieve user using invalid user id.
        assertNull(userDao.getUserByUserId("I_DO_NOT_EXIST"));
    }
}
