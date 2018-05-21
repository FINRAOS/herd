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
import org.finra.herd.model.jpa.NamespaceEntity;

public interface EmrClusterDefinitionDao extends BaseJpaDao
{
    /**
     * Retrieves an EMR cluster definition entity by namespace entity and EMR cluster definition name.
     *
     * @param namespaceEntity the namespace entity
     * @param emrClusterDefinitionName the name of the EMR cluster definition (case-insensitive)
     *
     * @return the EMR cluster definition entity
     */
    EmrClusterDefinitionEntity getEmrClusterDefinitionByNamespaceAndName(NamespaceEntity namespaceEntity, String emrClusterDefinitionName);

    /**
     * Gets a list of keys for all EMR cluster definitions defined in the system for the specified namespace. The result list is sorted by EMR cluster
     * definition name in ascending order.
     *
     * @param namespaceEntity the namespace entity
     *
     * @return the list of EMR cluster definition keys
     */
    List<EmrClusterDefinitionKey> getEmrClusterDefinitionKeysByNamespace(NamespaceEntity namespaceEntity);
}
