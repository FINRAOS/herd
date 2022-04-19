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

import java.util.Set;

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespaceSearchRequest;
import org.finra.herd.model.api.xml.NamespaceSearchResponse;
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
    Namespace createNamespace(NamespaceCreateRequest namespaceCreateRequest);

    /**
     * Gets a namespace for the specified key.
     *
     * @param namespaceKey the namespace key
     *
     * @return the namespace
     */
    Namespace getNamespace(NamespaceKey namespaceKey);

    /**
     * Deletes a namespace for the specified key.
     *
     * @param namespaceKey the namespace key
     *
     * @return the namespace that was deleted
     */
    Namespace deleteNamespace(NamespaceKey namespaceKey);

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the namespace keys
     */
    NamespaceKeys getNamespaces();

    /**
     * Updates an existing namespace based on the specified name
     *
     * @param namespaceKey           the namespace key
     * @param namespaceUpdateRequest the namespace update request
     *
     * @return the updated namespace
     */
    Namespace updateNamespaces(NamespaceKey namespaceKey, NamespaceUpdateRequest namespaceUpdateRequest);

    /**
     * Retrieves all namespaces existing in the system per specified search filters and keys.
     *
     * @param namespaceSearchRequest namespace search request
     * @param fields                 the field options for the namespace search response. The valid field options are: chargeCode, s3KeyPrefix
     *
     * @return the namespace search response
     */
    NamespaceSearchResponse searchNamespaces(NamespaceSearchRequest namespaceSearchRequest, Set<String> fields);
}
