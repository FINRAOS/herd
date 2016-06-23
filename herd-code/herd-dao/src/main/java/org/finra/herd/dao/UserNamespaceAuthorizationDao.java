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
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

public interface UserNamespaceAuthorizationDao extends BaseJpaDao
{
    /**
     * Gets a user namespace authorization by key.
     *
     * @param key the user namespace authorization key (case-insensitive)
     *
     * @return the user namespace authorization for the specified key
     */
    public UserNamespaceAuthorizationEntity getUserNamespaceAuthorizationByKey(UserNamespaceAuthorizationKey key);

    /**
     * Gets a list of user namespace authorizations for the specified user.
     *
     * @param userId the user id (case-insensitive)
     *
     * @return the list of user namespace authorizations
     */
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserId(String userId);

    /**
     * Gets a list of user namespace authorization entities where the user ID starts with the given string
     * 
     * @param string String to search
     * @return List of namespace authorization entities
     */
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserIdStartsWith(String string);

    /**
     * Gets a list of user namespace authorizations for the specified namespace.
     *
     * @param namespace the namespace (case-insensitive)
     *
     * @return the list of user namespace authorizations
     */
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByNamespace(String namespace);
}
