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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.TagEntity;

public interface BusinessObjectDefinitionDao extends BaseJpaDao
{
    /**
     * Gets a business object definition by key.
     *
     * @param businessObjectDefinitionKey the business object definition key (case-insensitive)
     *
     * @return the business object definition for the specified key
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * A shortcut for {@link #getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey)}
     *
     * @param namespace The business object definition namespace
     * @param name The business object definition name
     *
     * @return Business object definition entity or null
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(String namespace, String name);

    /**
     * Gets all business object definition keys.
     *
     * @return the list of all business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitionKeys();

    /**
     * Gets a list of all business object definition keys for a specified namespace, or, if, namespace is not specified, for all namespaces in the system.
     *
     * @param namespaceCode the optional namespace code (case-insensitive)
     *
     * @return the list of all business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitionKeys(String namespaceCode);

    /**
     * Gets a list of all business object definition entities for the specified namespace.
     *
     * @return the list of all business object definition entities.
     */
    public List<BusinessObjectDefinitionEntity> getBusinessObjectDefinitions(List<TagEntity> tagEntities);
}
