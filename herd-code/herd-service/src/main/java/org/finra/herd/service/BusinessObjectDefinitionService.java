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

import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.dto.BusinessObjectDefinitionSampleFileUpdateDto;
import org.finra.herd.model.dto.SearchIndexUpdateDto;

/**
 * The business object definition service.
 */
public interface BusinessObjectDefinitionService
{
    /**
     * Creates a new business object definition.
     *
     * @param businessObjectDefinitionCreateRequest the business object definition create request.
     *
     * @return the created business object definition.
     */
    BusinessObjectDefinition createBusinessObjectDefinition(BusinessObjectDefinitionCreateRequest businessObjectDefinitionCreateRequest);

    /**
     * Updates a business object definition.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param businessObjectDefinitionUpdateRequest the business object definition update request
     *
     * @return the updated business object definition
     */
    BusinessObjectDefinition updateBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionUpdateRequest businessObjectDefinitionUpdateRequest);

    /**
     * Updates a business object definition descriptive information.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param businessObjectDefinitionDescriptiveInformationUpdateRequest the business object definition descriptive information update request
     *
     * @return the updated business object definition
     */
    BusinessObjectDefinition updateBusinessObjectDefinitionDescriptiveInformation(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest businessObjectDefinitionDescriptiveInformationUpdateRequest);

    /**
     * Gets a business object definition for the specified key. This method starts a new transaction.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param includeBusinessObjectDefinitionUpdateHistory a flag to indicate if change events are to be included or not
     *
     * @return the business object definition.
     */
    BusinessObjectDefinition getBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        Boolean includeBusinessObjectDefinitionUpdateHistory);

    /**
     * Deletes a business object definition for the specified name.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the business object definition that was deleted.
     */
    BusinessObjectDefinition deleteBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Gets the list of all business object definitions defined in the system.
     *
     * @return the business object definition keys.
     */
    BusinessObjectDefinitionKeys getBusinessObjectDefinitions();

    /**
     * Gets a list of all business object definitions defined in the system for a specified namespace.
     *
     * @param namespaceCode the namespace code
     *
     * @return the business object definition keys
     */
    BusinessObjectDefinitionKeys getBusinessObjectDefinitions(String namespaceCode);

    /**
     * Searches across all business object definitions that are defined in the system per specified search filters and keys
     *
     * @return the retrieved business object definition list
     */
    BusinessObjectDefinitionSearchResponse searchBusinessObjectDefinitions(BusinessObjectDefinitionSearchRequest businessObjectDefinitionSearchRequest,
        Set<String> fields);

    /**
     * Update business object definition sample file
     *
     * @param businessObjectDefinitionKey business object definition key
     * @param businessObjectDefinitionSampleFileUpdateDto update dto
     */
    void updateBusinessObjectDefinitionEntitySampleFile(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionSampleFileUpdateDto businessObjectDefinitionSampleFileUpdateDto);

    /**
     * Updates the search index document representation of the business object definition.
     *
     * @param searchIndexUpdateDto the SearchIndexUpdateDto object
     */
    void updateSearchIndexDocumentBusinessObjectDefinition(SearchIndexUpdateDto searchIndexUpdateDto);
}
