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

import org.finra.herd.model.api.xml.ConfigurationEntry;
import org.finra.herd.model.api.xml.ConfigurationEntryKeys;

/**
 * The configuration entry service.
 */
public interface ConfigurationEntryService
{
    /**
     * Gets an existing configuration entry for the specified key.
     *
     * @param configurationEntryKey the configuration entry key
     *
     * @return the retrieved configuration entry
     */
    ConfigurationEntry getConfigurationEntry(String configurationEntryKey);

    /**
     * Gets a list of configuration entry keys for all configuration entries defined in the system.
     *
     * @return the configuration entry keys
     */
    ConfigurationEntryKeys getConfigurationEntries();
}
