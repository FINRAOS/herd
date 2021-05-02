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

import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKeys;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;

/**
 * The service for namespace IAM role authorizations.
 */
public interface NamespaceIamRoleAuthorizationService
{
    /**
     * Authorizes a namespace to use IAM roles.
     *
     * @param request The namespace IAM role create request
     *
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationCreateRequest request);

    /**
     * Removes IAM roles a namespace has authorizations to use.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key consisting of the namespace code and the IAM role name
     *
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey);

    /**
     * Get the IAM roles that a namespace is authorized to use.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key consisting of the namespace code and the IAM role name
     *
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey);

    /**
     * Get a list of namespace IAM role authorization keys.
     *
     * @return The list of namespace IAM role authorization keys.
     */
    NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizations();

    /**
     * Get a list of namespace IAM role authorization keys.
     *
     * @param iamRoleName The IAM role name
     *
     * @return The list of namespace IAM role authorization keys.
     */
    NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByIamRoleName(String iamRoleName);

    /**
     * Get a list of namespace IAM role authorization keys.
     *
     * @param namespace The namespace
     *
     * @return The list of namespace IAM role authorization keys.
     */
    NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByNamespace(String namespace);

    /**
     * Sets the authorizations a namespace has to use IAM roles.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key consisting of the namespace code and the IAM role name
     * @param request The namespace IAM role update request
     *
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey,
        NamespaceIamRoleAuthorizationUpdateRequest request);
}
