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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

public interface BusinessObjectDefinitionColumnDao extends BaseJpaDao
{
    /**
     * Gets a business object definition column by business object definition entity and business object definition column name.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectDefinitionColumnName the name of the business object definition column (case-insensitive)
     *
     * @return the business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnByBusinessObjectDefinitionColumnName(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String businessObjectDefinitionColumnName);

    /**
     * Gets a business object definition column by it's key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key (case-insensitive)
     *
     * @return the business object definition column entity
     */
    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnByKey(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey);

    /**
     * Gets a list of business object definition columns by business object definition entity. Returns the business object definition column keys by default
     * along with any other top-level elements as specified by the fields String array.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     *
     * @return the business object definition column entity
     */
    public List<BusinessObjectDefinitionColumnEntity> getBusinessObjectDefinitionColumnsByBusinessObjectDefinition(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity);
}
