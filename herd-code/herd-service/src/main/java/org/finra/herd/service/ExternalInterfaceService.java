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

import org.finra.herd.model.api.xml.ExternalInterface;
import org.finra.herd.model.api.xml.ExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.ExternalInterfaceKey;
import org.finra.herd.model.api.xml.ExternalInterfaceKeys;
import org.finra.herd.model.api.xml.ExternalInterfaceUpdateRequest;

/**
 * The external interface service.
 */
public interface ExternalInterfaceService
{
    /**
     * Creates a new external interface.
     *
     * @param externalInterfaceCreateRequest the external interface create request
     *
     * @return the created external interface
     */
    ExternalInterface createExternalInterface(ExternalInterfaceCreateRequest externalInterfaceCreateRequest);

    /**
     * Gets an external interface for the specified key.
     *
     * @param externalInterfaceKey the external interface key
     *
     * @return the external interface
     */
    ExternalInterface getExternalInterface(ExternalInterfaceKey externalInterfaceKey);

    /**
     * Deletes an external interface for the specified name.
     *
     * @param externalInterfaceKey the external interface key
     *
     * @return the external interface that was deleted
     */
    ExternalInterface deleteExternalInterface(ExternalInterfaceKey externalInterfaceKey);

    /**
     * Gets a list of external interface keys for all external interfaces defined in the system.
     *
     * @return the external interface keys
     */
    ExternalInterfaceKeys getExternalInterfaces();

    /**
     * Updates an existing external interface.
     *
     * @param externalInterfaceKey the external interface key
     * @param externalInterfaceUpdateRequest the external interface update request
     *
     * @return the created external interface
     */
    ExternalInterface updateExternalInterface(ExternalInterfaceKey externalInterfaceKey, ExternalInterfaceUpdateRequest externalInterfaceUpdateRequest);
}
