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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;

/**
 * The business object definition column service.
 */
public interface BusinessObjectDefinitionColumnService
{
    /**
     * Creates a new business object definition column.
     *
     * @param businessObjectDefinitionColumnCreateRequest the information needed to create a business object definition column
     *
     * @return the newly created business object definition column
     */
    public BusinessObjectDefinitionColumn createBusinessObjectDefinitionColumn(
        BusinessObjectDefinitionColumnCreateRequest businessObjectDefinitionColumnCreateRequest);

    /**
     * Gets an existing business object definition column by key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     *
     * @return the business object definition column information
     */
    public BusinessObjectDefinitionColumn getBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey);

    /**
     * Updates an existing business object definition column by key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     * @param businessObjectDefinitionColumnUpdateRequest the information needed to update a business object definition column
     *
     * @return the business object definition column information
     */
    public BusinessObjectDefinitionColumn updateBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey,
        BusinessObjectDefinitionColumnUpdateRequest businessObjectDefinitionColumnUpdateRequest);

    /**
     * Deletes an existing business object definition column by key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     *
     * @return the business object definition column information for the attribute that got deleted
     */
    public BusinessObjectDefinitionColumn deleteBusinessObjectDefinitionColumn(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey);

    /**
     * Gets a list of keys for all existing business object definition columns.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the list of business object definition column keys
     */
    public BusinessObjectDefinitionColumnKeys getBusinessObjectDefinitionColumns(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Search business object definition columns based on the request
     *
     * @param request search request
     * @param fields business object data columns optional fields to return
     *
     * @return business object definition column search response
     */
    public BusinessObjectDefinitionColumnSearchResponse searchBusinessObjectDefinitionColumns(BusinessObjectDefinitionColumnSearchRequest request,
        Set<String> fields);
}
