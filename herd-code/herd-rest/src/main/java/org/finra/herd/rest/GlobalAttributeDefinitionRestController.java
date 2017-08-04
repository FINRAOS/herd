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

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.GlobalAttributeDefinitionService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles global attribute definition REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Global Attribute Definition")
public class GlobalAttributeDefinitionRestController
{
    public final static String GLOBAL_ATTRIBUTE_DEFINITIONS_URI_PREFIX = "/globalAttributeDefinitions";

    @Autowired
    private GlobalAttributeDefinitionService globalAttributeDefinitionService;

    /**
     * Creates a new global Attribute Definition.
     *
     * @param globalAttributeDefinitionCreateRequest the information needed to create the global Attribute Definition
     *
     * @return the created global Attribute Definition
     */
    @RequestMapping(value = GLOBAL_ATTRIBUTE_DEFINITIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_GLOBAL_ATTRIBUTE_DEFINITIONS_POST)
    public GlobalAttributeDefinition createGlobalAttributeDefinition(@RequestBody GlobalAttributeDefinitionCreateRequest globalAttributeDefinitionCreateRequest)
    {
        return globalAttributeDefinitionService.createGlobalAttributeDefinition(globalAttributeDefinitionCreateRequest);
    }

    /**
     * Deletes an existing global Attribute Definition.
     *
     * @param globalAttributeDefinitionLevel the global Attribute Definition level
     * @param globalAttributeDefinitionName the global Attribute Definition name
     *
     * @return the deleted global Attribute Definition
     */
    @RequestMapping(value = GLOBAL_ATTRIBUTE_DEFINITIONS_URI_PREFIX +
        "/globalAttributeDefinitionLevels/{globalAttributeDefinitionLevel}/globalAttributeDefinitionNames/{globalAttributeDefinitionName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_GLOBAL_ATTRIBUTE_DEFINITIONS_DELETE)
    public GlobalAttributeDefinition deleteGlobalAttributeDefinition(@PathVariable("globalAttributeDefinitionLevel") String globalAttributeDefinitionLevel,
        @PathVariable("globalAttributeDefinitionName") String globalAttributeDefinitionName)
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(globalAttributeDefinitionLevel, globalAttributeDefinitionName);
        return globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);
    }

    /**
     * Retrieves all global Attribute Definitions.
     *
     * @return global Attribute Definitions
     */
    @RequestMapping(value = GLOBAL_ATTRIBUTE_DEFINITIONS_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_GLOBAL_ATTRIBUTE_DEFINITIONS_ALL_GET)
    public GlobalAttributeDefinitionKeys getGlobalAttributeDefinitions()
    {
        return globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();
    }

    /**
     * Get an existing global Attribute Definition.
     *
     * @param globalAttributeDefinitionLevel the global Attribute Definition level
     * @param globalAttributeDefinitionName the global Attribute Definition name
     *
     * @return the global Attribute Definition
     */
    @RequestMapping(value = GLOBAL_ATTRIBUTE_DEFINITIONS_URI_PREFIX +
        "/globalAttributeDefinitionLevels/{globalAttributeDefinitionLevel}/globalAttributeDefinitionNames/{globalAttributeDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_GLOBAL_ATTRIBUTE_DEFINITIONS_GET)
    public GlobalAttributeDefinition getGlobalAttributeDefinition(@PathVariable("globalAttributeDefinitionLevel") String globalAttributeDefinitionLevel,
        @PathVariable("globalAttributeDefinitionName") String globalAttributeDefinitionName)
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(globalAttributeDefinitionLevel, globalAttributeDefinitionName);
        return globalAttributeDefinitionService.getGlobalAttributeDefinition(globalAttributeDefinitionKey);
    }
}
