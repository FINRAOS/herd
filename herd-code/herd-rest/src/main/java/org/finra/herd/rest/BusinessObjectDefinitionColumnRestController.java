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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDefinitionColumnService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition column requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Definition Column")
public class BusinessObjectDefinitionColumnRestController extends HerdBaseController
{
    public static final String BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX = "/businessObjectDefinitionColumns";

    @Autowired
    private BusinessObjectDefinitionColumnService businessObjectDefinitionColumnService;

    /**
     * Creates a new business object definition column.
     *
     * @param request the information needed to create a business object definition column
     *
     * @return the newly created business object definition column
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_POST)
    public BusinessObjectDefinitionColumn createBusinessObjectDefinitionColumn(@RequestBody BusinessObjectDefinitionColumnCreateRequest request)
    {
        return businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(request);
    }

    /**
     * Gets an existing column for the business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectDefinitionColumnName the name of the business object definition column
     *
     * @return the business object definition column that got updated
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectDefinitionColumnNames/{businessObjectDefinitionColumnName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_GET)
    public BusinessObjectDefinitionColumn getBusinessObjectDefinitionColumn(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectDefinitionColumnName") String businessObjectDefinitionColumnName)
    {
        return businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(namespace, businessObjectDefinitionName, businessObjectDefinitionColumnName));
    }

    /**
     * Updates an existing column for the business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectDefinitionColumnName the name of the business object definition column
     * @param request the request information needed to update the business object definition column
     *
     * @return the business object definition column
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectDefinitionColumnNames/{businessObjectDefinitionColumnName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_PUT)
    public BusinessObjectDefinitionColumn updateBusinessObjectDefinitionColumn(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectDefinitionColumnName") String businessObjectDefinitionColumnName,
        @RequestBody BusinessObjectDefinitionColumnUpdateRequest request)
    {
        return businessObjectDefinitionColumnService.updateBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(namespace, businessObjectDefinitionName, businessObjectDefinitionColumnName), request);
    }

    /**
     * Deletes an existing column for the business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectDefinitionColumnName the name of the business object definition column
     *
     * @return the business object definition column that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectDefinitionColumnNames/{businessObjectDefinitionColumnName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_DELETE)
    public BusinessObjectDefinitionColumn deleteBusinessObjectDefinitionColumn(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectDefinitionColumnName") String businessObjectDefinitionColumnName)
    {
        return businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(namespace, businessObjectDefinitionName, businessObjectDefinitionColumnName));
    }

    /**
     * Gets a list of keys for all existing business object definition columns for a specific business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the list of business object definition column keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_ALL_GET)
    public BusinessObjectDefinitionColumnKeys getBusinessObjectDefinitionColumns(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        return businessObjectDefinitionColumnService
            .getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName));
    }

    /**
     * Retrieve a list of business object definition columns meeting the search criteria filters and fields request.
     *
     * @param request the search criteria needed to find a list of business object definition columns
     * @param fields the field options for the business object definition columns search response. The valid field options are: description, schemaColumnName
     *
     * @return the list of business object definition columns
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_COLUMNS_URI_PREFIX + "/search", method = RequestMethod.POST, consumes = {"application/xml",
        "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_COLUMNS_SEARCH_POST)
    public BusinessObjectDefinitionColumnSearchResponse searchBusinessObjectDefinitionColumns(
        @RequestParam(value = "fields", required = false, defaultValue = "") Set<String> fields,
        @RequestBody BusinessObjectDefinitionColumnSearchRequest request)
    {
        return businessObjectDefinitionColumnService.searchBusinessObjectDefinitionColumns(request, fields);
    }
}
