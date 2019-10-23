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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.ConfigurationEntry;
import org.finra.herd.model.api.xml.ConfigurationEntryKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.ConfigurationEntryService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles configuration entries requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Configuration Entry")
public class ConfigurationEntryRestController extends HerdBaseController
{
    public static final String CONFIGURATION_ENTRIES_URI_PREFIX = "/configurationEntries";

    @Autowired
    private ConfigurationEntryService configurationEntryService;

    /**
     * Gets an existing configuration entry by its configuration entry key.
     *
     * @param configurationEntryKey the key of the configuration entry
     *
     * @return the retrieved configuration entry
     */
    @RequestMapping(value = CONFIGURATION_ENTRIES_URI_PREFIX + "/{configurationEntryKey}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_CONFIGURATION_ENTRIES_GET)
    public ConfigurationEntry getConfigurationEntry(@PathVariable("configurationEntryKey") String configurationEntryKey)
    {
        return configurationEntryService.getConfigurationEntry(configurationEntryKey);
    }

    /**
     * Gets a list of configuration entry keys for all configuration entries defined in the system.
     *
     * @return the list of configuration entry keys
     */
    @RequestMapping(value = CONFIGURATION_ENTRIES_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_CONFIGURATION_ENTRIES_ALL_GET)
    public ConfigurationEntryKeys getConfigurationEntries()
    {
        return configurationEntryService.getConfigurationEntries();
    }
}
