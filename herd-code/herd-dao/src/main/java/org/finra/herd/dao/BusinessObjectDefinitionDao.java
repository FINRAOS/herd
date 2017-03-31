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
     * Gets a list of all business object definition entities defined in the system.
     *
     * @return the list of business object definition entities
     */
    public List<BusinessObjectDefinitionEntity> getAllBusinessObjectDefinitions();

    /**
     * Gets a chunk of business object definition entities defined in the system per specified parameters.
     *
     * @param startPosition the position of the first result, numbered from 0
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of business object definition entities
     */
    public List<BusinessObjectDefinitionEntity> getAllBusinessObjectDefinitions(Integer startPosition, Integer maxResult);

    /**
     * Gets a list of business object definition entities by a list of ids
     *
     * @param ids a list of business object definition ids
     *
     * @return the list of all business object definition entities.
     */
    public List<BusinessObjectDefinitionEntity> getAllBusinessObjectDefinitionsByIds(List<Integer> ids);

    /**
     * Gets a business object definition by key.
     *
     * @param businessObjectDefinitionKey the business object definition key (case-insensitive)
     *
     * @return the business object definition for the specified key
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey businessObjectDefinitionKey);

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
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitionKeysByNamespace(String namespaceCode);

    /**
     * Gets a list of all business object definition entities for the specified list of tag entities.
     *
     * @return the list of all business object definition entities.
     */
    public List<BusinessObjectDefinitionEntity> getBusinessObjectDefinitions(List<TagEntity> tagEntities);

    /**
     * Gets a percentage of all business object definition entities. The percentage is randomly selected from all the business object definitions. The random
     * selection is done by assigning a random number between 0 and 1 for each business object definition and if that number is below the percentage value
     * passed in as an argument, also between 0 and 1, then the business object definition is selected.
     *
     * @param percentage the percentage of all business object definitions to return. Value between 0 and 1 (inclusive).
     *
     * @return the percentage of all business object definition entities
     */
    public List<BusinessObjectDefinitionEntity> getPercentageOfAllBusinessObjectDefinitions(double percentage);

    /**
     * Gets the most recent of all business object definition entities
     *
     * @param numberOfResults the number of results to return
     *
     * @return the most recent of all business object definition entities.
     */
    public List<BusinessObjectDefinitionEntity> getMostRecentBusinessObjectDefinitions(int numberOfResults);

    /**
     * Gets a count of all business object definition entities
     *
     * @return the count of all business object definition entities.
     */
    public long getCountOfAllBusinessObjectDefinitions();
}
