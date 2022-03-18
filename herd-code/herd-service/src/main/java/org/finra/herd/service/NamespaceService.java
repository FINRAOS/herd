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

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespaceUpdateRequest;

/**
 * The namespace service.
 */
public interface NamespaceService
{
    /**
     * Creates a new namespace.
     *
     * @param namespaceCreateRequest the namespace create request
     *
     * @return the created namespace
     */
    public Namespace createNamespace(NamespaceCreateRequest namespaceCreateRequest);

    /**
     * Gets a namespace for the specified key.
     *
     * @param namespaceKey the namespace key
     *
     * @return the namespace
     */
    public Namespace getNamespace(NamespaceKey namespaceKey);

    /**
     * Deletes a namespace for the specified key.
     *
     * @param namespaceKey the namespace key
     *
     * @return the namespace that was deleted
     */
    public Namespace deleteNamespace(NamespaceKey namespaceKey);

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the namespace keys
     */
    public NamespaceKeys getNamespaces();

    /**
     * Updates an existing namespace based on the specified name
     *
     * @return the namespace that was updated
     */
    public Namespace updateNamespaces(NamespaceKey namespaceKey, NamespaceUpdateRequest namespaceUpdateRequest);
}
