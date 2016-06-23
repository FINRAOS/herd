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

import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;

public interface EmrClusterDefinitionDao extends BaseJpaDao
{
    /**
     * Retrieves EMR cluster definition entity by alternate key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(EmrClusterDefinitionKey emrClusterDefinitionKey);

    /**
     * Retrieves EMR cluster definition entity by alternate key.
     *
     * @param namespace the namespace (case-insensitive)
     * @param definitionName the EMR cluster definition name (case-insensitive)
     *
     * @return the EMR cluster definition entity
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(String namespace, String definitionName);

    /**
     * Gets a list of keys for all EMR cluster definitions defined in the system for the specified namespace.
     *
     * @param namespace the namespace (case-insensitive)
     *
     * @return the list of EMR cluster definition keys
     */
    public List<EmrClusterDefinitionKey> getEmrClusterDefinitionsByNamespace(String namespace);
}
