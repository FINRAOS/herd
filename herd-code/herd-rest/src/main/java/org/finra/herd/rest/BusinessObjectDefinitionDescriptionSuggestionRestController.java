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
package org.finra.herd.rest;

import java.util.Set;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDefinitionDescriptionSuggestionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition description suggestion REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Definition Description Suggestion")
public class BusinessObjectDefinitionDescriptionSuggestionRestController extends HerdBaseController
{
    private static final String BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX = "/businessObjectDefinitionDescriptionSuggestions";

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionService businessObjectDefinitionDescriptionSuggestionService;

    /**
     * Creates a new business object definition description suggestion.
     *
     * @param request the information needed to create the business object definition description suggestion
     *
     * @return the created business object definition description suggestion
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml",
        "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_POST)
    public BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestion(
        @RequestBody BusinessObjectDefinitionDescriptionSuggestionCreateRequest request)
    {
        return businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request);
    }

    /**
     * Deletes an existing business object definition description suggestion by key.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param userId the user id
     *
     * @return the deleted business object definition description suggestion
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX +
        "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}/userIds/{userId}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_DELETE)
    public BusinessObjectDefinitionDescriptionSuggestion deleteBusinessObjectDefinitionDescriptionSuggestion(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @PathVariable("userId") String userId)
    {
        return businessObjectDefinitionDescriptionSuggestionService.deleteBusinessObjectDefinitionDescriptionSuggestion(
            new BusinessObjectDefinitionDescriptionSuggestionKey(namespace, businessObjectDefinitionName, userId));
    }

    /**
     * Retrieves an existing business object definition description suggestion by key.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param userId the user id
     *
     * @return the retrieved business object definition description suggestion
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX +
        "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}/userIds/{userId}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_GET)
    public BusinessObjectDefinitionDescriptionSuggestion getBusinessObjectDefinitionDescriptionSuggestionByKey(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @PathVariable("userId") String userId)
    {
        return businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestionByKey(
            new BusinessObjectDefinitionDescriptionSuggestionKey(namespace, businessObjectDefinitionName, userId));
    }

    /**
     * Retrieves a list of business object definition description suggestion keys for all suggestions registered for a specific business object definition.
     * The result list is sorted by userId in ascending order.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the list of retrieved business object definition description suggestion keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX +
        "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_ALL_GET)
    public BusinessObjectDefinitionDescriptionSuggestionKeys getBusinessObjectDefinitionDescriptionSuggestions(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        return businessObjectDefinitionDescriptionSuggestionService
            .getBusinessObjectDefinitionDescriptionSuggestions(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName));
    }

    /**
     * Retrieve a list of business object definition description suggestions meeting the search criteria filters and fields request.
     *
     * @param request the search criteria needed to find a list of business object definition description suggestions
     * @param fields the field options for the business object definition description suggestions search response.
     * The valid field options are: status, descriptionSuggestion, createdByUserId, createdOn
     *
     * @return the list of business object definition description suggestions
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX + "/search", method = RequestMethod.POST, consumes = {
        "application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_SEARCH_POST)
    public BusinessObjectDefinitionDescriptionSuggestionSearchResponse searchBusinessObjectDefinitionDescriptionSuggestions(
        @RequestParam(value = "fields", required = false, defaultValue = "") Set<String> fields,
        @RequestBody BusinessObjectDefinitionDescriptionSuggestionSearchRequest request)
    {
        return businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, fields);
    }

    /**
     * Updates an existing business object definition description suggestion by key.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param userId the user id
     * @param request the information needed to update the business object definition description suggestion
     *
     * @return the updated business object definition description suggestion
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX +
        "/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}/userIds/{userId}", method = RequestMethod.PUT, consumes = {
        "application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_PUT)
    public BusinessObjectDefinitionDescriptionSuggestion updateBusinessObjectDefinitionDescriptionSuggestion(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @PathVariable("userId") String userId,
        @RequestBody BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request)
    {
        return businessObjectDefinitionDescriptionSuggestionService.updateBusinessObjectDefinitionDescriptionSuggestion(
            new BusinessObjectDefinitionDescriptionSuggestionKey(namespace, businessObjectDefinitionName, userId), request);
    }

    /**
     * Accepts suggested business object definition description suggestion by key and updates the corresponding business object definition description.
     *
     * @param request the information needed to accept the business object definition description suggestion.
     *
     * @return the accepted business object definition description suggestion.
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_URI_PREFIX + "/acceptance", method = RequestMethod.POST, consumes = {
        "application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTIONS_ACCEPTANCE_POST)
    public BusinessObjectDefinitionDescriptionSuggestion acceptBusinessObjectDefinitionDescriptionSuggestion(
        @RequestBody BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request)
    {
        return businessObjectDefinitionDescriptionSuggestionService.acceptBusinessObjectDefinitionDescriptionSuggestion(request);
    }
}