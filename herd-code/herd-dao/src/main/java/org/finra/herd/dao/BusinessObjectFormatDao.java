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
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

public interface BusinessObjectFormatDao extends BaseJpaDao
{
    /**
     * Gets a business object format based on it's key. If a format version isn't specified, the latest available format version will be used.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive)
     *
     * @return the business object format
     */
    BusinessObjectFormatEntity getBusinessObjectFormatByAltKey(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Returns a number of business object format instances that reference a specified partition key group.
     *
     * @param partitionKeyGroupEntity the partition key group entity
     *
     * @return the number of business object format instances that reference this partition key group
     */
    Long getBusinessObjectFormatCount(PartitionKeyGroupEntity partitionKeyGroupEntity);

    /**
     * Gets a list of ids for all business object formats registered under the specified business object definition. The list of business object format ids
     * returned by this method is sorted by business object format usage ascending, file type ascending, and business object format version descending.
     *
     * @param businessObjectDefinitionEntity the business object definition
     *
     * @return the list of business object format ids
     */
    List<Integer> getBusinessObjectFormatIdsByBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinitionEntity);

    /**
     * Gets the maximum available version of the specified business object format.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive)
     *
     * @return the maximum available version of the specified business object format
     */
    Integer getBusinessObjectFormatMaxVersion(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Gets a list of business object format keys for the specified business object definition key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param latestBusinessObjectFormatVersion specifies if only the latest (maximum) versions of the relative business object formats are returned
     *
     * @return the list of business object format keys
     */
    List<BusinessObjectFormatKey> getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey, boolean latestBusinessObjectFormatVersion);

    /**
     * Gets a list of business object format keys for the specified business object definition key and business object format usage.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param businessObjectFormatUsage the business object format usage
     * @param latestBusinessObjectFormatVersion specifies if only the latest (maximum) versions of the relative business object formats are returned
     *
     * @return the list of business object format keys
     */
    List<BusinessObjectFormatKey> getBusinessObjectFormatsWithFilters(BusinessObjectDefinitionKey businessObjectDefinitionKey, String businessObjectFormatUsage,
        boolean latestBusinessObjectFormatVersion);

    /**
     * Gets a list of latest version business object format entities for the specified business object definition key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the list of business object format entities
     */
    List<BusinessObjectFormatEntity> getLatestVersionBusinessObjectFormatsByBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);
}
