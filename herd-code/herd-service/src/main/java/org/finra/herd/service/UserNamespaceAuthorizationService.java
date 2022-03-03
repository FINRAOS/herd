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

import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;

/**
 * The business object definition service.
 */
public interface UserNamespaceAuthorizationService
{
    /**
     * Creates a new user namespace authorization.
     *
     * @param request the information needed to create the user namespace authorization
     *
     * @return the created user namespace authorization
     */
    UserNamespaceAuthorization createUserNamespaceAuthorization(UserNamespaceAuthorizationCreateRequest request);

    /**
     * Updates an existing user namespace authorization by key.
     *
     * @param key the user namespace authorization key
     * @param request the information needed to update the user namespace authorization
     *
     * @return the updated user namespace authorization
     */
    UserNamespaceAuthorization updateUserNamespaceAuthorization(UserNamespaceAuthorizationKey key, UserNamespaceAuthorizationUpdateRequest request);

    /**
     * Gets an existing user namespace authorization by key.
     *
     * @param key the user namespace authorization key
     *
     * @return the retrieved user namespace authorization
     */
    UserNamespaceAuthorization getUserNamespaceAuthorization(UserNamespaceAuthorizationKey key);

    /**
     * Deletes an existing user namespace authorization by key.
     *
     * @param key the user namespace authorization key
     *
     * @return the deleted user namespace authorization
     */
    UserNamespaceAuthorization deleteUserNamespaceAuthorization(UserNamespaceAuthorizationKey key);

    /**
     * Deletes all existing user namespace authorizations for the specified user.
     *
     * @param userId the user id
     *
     * @return the list of deleted user namespace authorizations
     */
    UserNamespaceAuthorizations deleteUserNamespaceAuthorizationsByUserId(String userId);

    /**
     * Gets a list of user namespace authorizations for the specified user.
     *
     * @param userId the user id
     *
     * @return the list of user namespace authorizations
     */
    UserNamespaceAuthorizations getUserNamespaceAuthorizationsByUserId(String userId);

    /**
     * Gets a list of user namespace authorizations for the specified namespace.
     *
     * @param namespace the namespace
     *
     * @return the list of user namespace authorizations
     */
    UserNamespaceAuthorizations getUserNamespaceAuthorizationsByNamespace(String namespace);
}
