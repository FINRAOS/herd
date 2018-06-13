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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;

/**
 * The business object definition description suggestion service.
 */
public interface BusinessObjectDefinitionDescriptionSuggestionService
{
    /**
     * Creates a new business object definition description suggestion.
     *
     * @param request the information needed to create the business object definition description suggestion
     *
     * @return the created user namespace authorization
     */
    BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request);

    /**
     * Deletes an existing business object definition description suggestion by key.
     *
     * @param key the business object definition description suggestion key
     *
     * @return the deleted business object definition description suggestion
     */
    BusinessObjectDefinitionDescriptionSuggestion deleteBusinessObjectDefinitionDescriptionSuggestion(BusinessObjectDefinitionDescriptionSuggestionKey key);

    /**
     * Gets an existing business object definition description suggestion by key.
     *
     * @param key the business object definition description suggestion key
     *
     * @return the retrieved business object definition description suggestion
     */
    BusinessObjectDefinitionDescriptionSuggestion getBusinessObjectDefinitionDescriptionSuggestionByKey(BusinessObjectDefinitionDescriptionSuggestionKey key);

    /**
     * Gets all existing business object definition description suggestion keys.
     *
     * @param businessObjectDefinitionKey the business object definition key used to get the business object definition suggestion keys
     *
     * @return the list of retrieved business object definition description suggestion keys
     */
    BusinessObjectDefinitionDescriptionSuggestionKeys getBusinessObjectDefinitionDescriptionSuggestions(
        BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Search business object definition description suggestions based on the request
     *
     * @param request search request
     * @param fields business object definition description suggestions optional fields to return
     *
     * @return business object definition description suggestion search response
     */
    BusinessObjectDefinitionDescriptionSuggestionSearchResponse searchBusinessObjectDefinitionDescriptionSuggestions(
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request, Set<String> fields);

    /**
     * Updates an existing business object definition description suggestion by key.
     *
     * @param key the business object definition description suggestion key
     * @param request the information needed to update the business object definition description suggestion
     *
     * @return the updated business object definition description suggestion
     */
    BusinessObjectDefinitionDescriptionSuggestion updateBusinessObjectDefinitionDescriptionSuggestion(BusinessObjectDefinitionDescriptionSuggestionKey key,
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request);

    /**
     * Accepts the business object definition description suggestion and updates business object definition description.
     *
     * @param request the information needed to accept the business object definition description suggestion.
     *
     * @return the accepted business object definition description suggestion.
     */
    BusinessObjectDefinitionDescriptionSuggestion acceptBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request);
}