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
package org.finra.dm.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.BusinessObjectDefinition;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.dm.service.BusinessObjectDefinitionService;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class BusinessObjectDefinitionRestController extends DmBaseController
{
    @Autowired
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    /**
     * Creates a new business object definition.
     *
     * @param request the information needed to create the business object definition.
     *
     * @return the created business object definition.
     */
    @RequestMapping(value = "/businessObjectDefinitions", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_POST)
    public BusinessObjectDefinition createBusinessObjectDefinition(@RequestBody BusinessObjectDefinitionCreateRequest request)
    {
        return businessObjectDefinitionService.createBusinessObjectDefinition(request);
    }

    /**
     * Updates an existing legacy business object definition by name.
     *
     * @param businessObjectDefinitionName the name of the business object definition to update
     * @param request the information needed to update the business object definition
     *
     * @return the updated business object definition
     */
    @RequestMapping(value = "/businessObjectDefinitions/{businessObjectDefinitionName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_PUT)
    public BusinessObjectDefinition updateBusinessObjectDefinition(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @RequestBody BusinessObjectDefinitionUpdateRequest request)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(null, businessObjectDefinitionName);
        return businessObjectDefinitionService.updateBusinessObjectDefinition(businessObjectDefinitionKey, request);
    }

    /**
     * Updates an existing business object definition by key.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the name of the business object definition to update
     * @param request the information needed to update the business object definition
     *
     * @return the updated business object definition
     */
    @RequestMapping(value = "/businessObjectDefinitions/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_PUT)
    public BusinessObjectDefinition updateBusinessObjectDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @RequestBody BusinessObjectDefinitionUpdateRequest request)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName);
        return businessObjectDefinitionService.updateBusinessObjectDefinition(businessObjectDefinitionKey, request);
    }

    /**
     * Gets an existing legacy business object definition by name.
     *
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the retrieved business object definition.
     */
    @RequestMapping(value = "/businessObjectDefinitions/{businessObjectDefinitionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_GET)
    public BusinessObjectDefinition getBusinessObjectDefinition(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(null, businessObjectDefinitionName);
        return businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey);
    }

    /**
     * Gets an existing business object definition by key.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the retrieved business object definition.
     */
    @RequestMapping(value = "/businessObjectDefinitions/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_GET)
    public BusinessObjectDefinition getBusinessObjectDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName);
        return businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey);
    }

    /**
     * Deletes an existing legacy business object definition by name.
     *
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the business object definition information of the entity that got deleted
     */
    @RequestMapping(value = "/businessObjectDefinitions/{businessObjectDefinitionName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_DELETE)
    public BusinessObjectDefinition deleteBusinessObjectDefinition(@PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(null, businessObjectDefinitionName);
        return businessObjectDefinitionService.deleteBusinessObjectDefinition(businessObjectDefinitionKey);
    }

    /**
     * Deletes an existing legacy business object definition by key.
     *
     * @param namespace the namespace code
     * @param businessObjectDefinitionName the business object definition name
     *
     * @return the business object definition information of the entity that got deleted
     */
    @RequestMapping(value = "/businessObjectDefinitions/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_DELETE)
    public BusinessObjectDefinition deleteBusinessObjectDefinition(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName);
        return businessObjectDefinitionService.deleteBusinessObjectDefinition(businessObjectDefinitionKey);
    }

    /**
     * Gets the list of business object definitions that are defined in the system.
     *
     * @return the retrieved business object definition list.
     */
    // TODO: The below endpoint doesn't take a namespace and it should ultimately be removed once we have an API where a user can get
    // a list of all namespaces in the system. Then there would be no need to get all business object definitions in the system.
    @RequestMapping(value = "/businessObjectDefinitions", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_ALL_GET)
    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions()
    {
        return businessObjectDefinitionService.getBusinessObjectDefinitions();
    }

    /**
     * Gets the list of business object definitions that are defined in the system.
     *
     * @param namespace the namespace code
     *
     * @return the retrieved business object definition list
     */
    @RequestMapping(value = "/businessObjectDefinitions/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITIONS_ALL_GET)
    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions(@PathVariable("namespace") String namespace)
    {
        return businessObjectDefinitionService.getBusinessObjectDefinitions(namespace);
    }
}
