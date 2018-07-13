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

import java.util.List;

import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

public interface UserNamespaceAuthorizationDao extends BaseJpaDao
{
    /**
     * Gets a list of user ids for all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT namespace permissions for the specified namespace.
     *
     * @param namespaceEntity the namespace entity
     *
     * @return the list of user ids
     */
    List<String> getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(NamespaceEntity namespaceEntity);

    /**
     * Gets a user namespace authorization by key.
     *
     * @param userNamespaceAuthorizationKey the user namespace authorization key (case-insensitive)
     *
     * @return the user namespace authorization
     */
    UserNamespaceAuthorizationEntity getUserNamespaceAuthorizationByKey(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey);

    /**
     * Gets a list of user namespace authorizations for the specified user.
     *
     * @param userId the user id (case-insensitive)
     *
     * @return the list of user namespace authorizations
     */
    List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserId(String userId);

    /**
     * Gets a list of user namespace authorization entities where the user ID starts with the given string
     *
     * @param userIdStartsWith String to search
     *
     * @return the list of user namespace authorizations
     */
    List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserIdStartsWith(String userIdStartsWith);

    /**
     * Gets a list of user namespace authorizations for the specified namespace.
     *
     * @param namespace the namespace (case-insensitive)
     *
     * @return the list of user namespace authorizations
     */
    List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByNamespace(String namespace);
}
