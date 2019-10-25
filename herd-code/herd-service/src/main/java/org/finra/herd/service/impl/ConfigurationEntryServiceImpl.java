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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ConfigurationEntry;
import org.finra.herd.model.api.xml.ConfigurationEntryKeys;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.ConfigurationEntryService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;

/**
 * The configuration entry service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ConfigurationEntryServiceImpl implements ConfigurationEntryService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationDaoHelper configurationDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Override
    public ConfigurationEntry getConfigurationEntry(String configurationEntryKey)
    {
        // Perform validation and trim.
        Assert.notNull(configurationEntryKey, "A configuration entry key must be specified.");
        configurationEntryKey = alternateKeyHelper.validateStringParameter("Configuration entry key", configurationEntryKey);

        // Create a new configuration entry and populate the key and values.
        ConfigurationEntry configurationEntry = new ConfigurationEntry();
        configurationEntry.setConfigurationEntryKey(configurationEntryKey);

        // Find the configuration value key in the configuration values array, use the configuration value to get the property.
        boolean isMatchingConfigurationValue = false;
        for (ConfigurationValue configurationValue : ConfigurationValue.values())
        {
            if (configurationValue.getKey().equals(configurationEntryKey))
            {
                // Found a matching configuration value.
                isMatchingConfigurationValue = true;

                // Get the configuration entry value.
                String configurationEntryValue = configurationHelper.getPropertyAsString(configurationValue);

                // If the configuration entry value is not empty set the configuration value on the configuration entry.
                if (StringUtils.isNotEmpty(configurationEntryValue))
                {
                    configurationEntry.setConfigurationEntryValue(configurationEntryValue);
                }

                // Once found we can break out of the for loop.
                break;
            }
        }

        if (!isMatchingConfigurationValue)
        {
            throw new ObjectNotFoundException(String.format("Configuration entry with key \"%s\" doesn't exist.", configurationEntryKey));
        }

        // Get the configuration value CLOB.
        String configurationValueClob = configurationDaoHelper.getClobProperty(configurationEntryKey);

        // If the value clob is not empty set the configuration value clob on the configuration entry.
        if (StringUtils.isNotEmpty(configurationValueClob))
        {
            configurationEntry.setConfigurationEntryValueClob(configurationValueClob);
        }

        return configurationEntry;
    }

    @Override
    public ConfigurationEntryKeys getConfigurationEntries()
    {
        // For each configuration value add its key to the keys list.
        List<String> configurationEntryKeysList = new ArrayList<>();
        for (ConfigurationValue configurationValue : ConfigurationValue.values())
        {
            configurationEntryKeysList.add(configurationValue.getKey());
        }

        // Sort the list alphabetically before returning it.
        Collections.sort(configurationEntryKeysList);

        return new ConfigurationEntryKeys(configurationEntryKeysList);
    }
}
