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
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizations;

/**
 * The service for namespace IAM role authorizations.
 */
public interface NamespaceIamRoleAuthorizationService
{
    /**
     * Authorizes a namespace to use IAM roles.
     * 
     * @param request The namespace IAM role create request
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationCreateRequest request);

    /**
     * Get the IAM roles that a namespace is authorized to use.
     * 
     * @param namespace The namespace
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(String namespace);

    /**
     * Get a list of namespace IAM role authorizations.
     * 
     * @return The list of namespace IAM role authorizations.
     */
    NamespaceIamRoleAuthorizations getNamespaceIamRoleAuthorizations();

    /**
     * Sets the authorizations a namespace has to use IAM roles.
     * 
     * @param namespace The namespace to update authorizations
     * @param request The namespace IAM role update request
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(String namespace, NamespaceIamRoleAuthorizationUpdateRequest request);

    /**
     * Removes IAM roles a namespace has authorizations to use.
     * 
     * @param namespace The namespace of the authorizations to remove
     * @return The namespace IAM role authorization
     */
    NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(String namespace);
}
