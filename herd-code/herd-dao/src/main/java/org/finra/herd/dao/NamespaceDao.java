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

import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.jpa.NamespaceEntity;

public interface NamespaceDao extends BaseJpaDao
{
    /**
     * Gets a namespace by it's key.
     *
     * @param namespaceKey the namespace key (case-insensitive)
     *
     * @return the namespace entity for the specified key
     */
    NamespaceEntity getNamespaceByKey(NamespaceKey namespaceKey);

    /**
     * Gets a namespace by it's code.
     *
     * @param namespaceCode the namespace code (case-insensitive)
     *
     * @return the namespace entity for the specified code
     */
    NamespaceEntity getNamespaceByCd(String namespaceCode);

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the list of namespace keys
     */
    List<NamespaceKey> getNamespaceKeys();

    /**
     * Gets a list of all namespaces registered in the system.
     *
     * @return the list of namespaces
     */
    List<NamespaceEntity> getNamespaces();

    /**
     * Gets a list of namespaces by chargeCode.
     *
     * @param chargeCode the charge code
     *
     * @return the list namespace entities for the specified charge code
     */
    List<NamespaceEntity> getNamespacesByChargeCode(String chargeCode);
}
