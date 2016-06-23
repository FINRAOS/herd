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

import org.finra.herd.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKeys;
import org.finra.herd.model.api.xml.EmrClusterDefinitionUpdateRequest;

/**
 * The EMR cluster definition service.
 */
public interface EmrClusterDefinitionService
{
    /**
     * Creates a new EMR cluster definition.
     *
     * @param emrClusterDefinitionCreateRequest the information needed to create an EMR cluster definition
     *
     * @return the newly created EMR cluster definition
     */
    public EmrClusterDefinitionInformation createEmrClusterDefinition(EmrClusterDefinitionCreateRequest emrClusterDefinitionCreateRequest) throws Exception;

    /**
     * Gets an existing EMR cluster definition by key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition
     */
    public EmrClusterDefinitionInformation getEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception;

    /**
     * Updates an existing EMR cluster definition.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     * @param emrClusterDefinitionUpdateRequest the information needed to update the EMR cluster definition
     *
     * @return the updated EMR cluster definition
     */
    public EmrClusterDefinitionInformation updateEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey,
        EmrClusterDefinitionUpdateRequest emrClusterDefinitionUpdateRequest) throws Exception;

    /**
     * Deletes an existing EMR cluster definition by key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition that got deleted
     */
    public EmrClusterDefinitionInformation deleteEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception;

    /**
     * Gets a list of keys for all EMR cluster definitions defined in the system for the specified namespace.
     *
     * @param namespace the namespace
     *
     * @return the EMR cluster definition keys
     */
    public EmrClusterDefinitionKeys getEmrClusterDefinitions(String namespace) throws Exception;
}
