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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.ConfigurationEntity;

public class ConfigurationDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetAllConfigurations() throws Exception
    {
        // Create relative database entities.
        createConfigurationEntity(CONFIGURATION_KEY, CONFIGURATION_VALUE);

        // Test that we are able to get all configurations from the database using our DAO tier.
        List<ConfigurationEntity> configurations = herdDao.findAll(ConfigurationEntity.class);
        assertTrue(configurations.size() > 0);
        for (ConfigurationEntity configuration : configurations)
        {
            if (CONFIGURATION_KEY.equals(configuration.getKey()) && CONFIGURATION_VALUE.equals(configuration.getValue()))
            {
                // We found our inserted test key/test value.
                return;
            }
        }
        fail("A configuration with key \"" + CONFIGURATION_KEY + "\" and value \"" + CONFIGURATION_VALUE + "\" was expected, but not found.");
    }

    @Test
    public void testGetConfigurationByKey() throws Exception
    {
        // Create relative database entities.
        ConfigurationEntity configurationEntity = createConfigurationEntity(CONFIGURATION_KEY, CONFIGURATION_VALUE);

        // Retrieve the configuration entity by its key.
        assertEquals(configurationEntity, configurationDao.getConfigurationByKey(CONFIGURATION_KEY));

        // Try to retrieve the configuration entity using its key in upper and lower case.
        assertNull(configurationDao.getConfigurationByKey(CONFIGURATION_KEY.toUpperCase()));
        assertNull(configurationDao.getConfigurationByKey(CONFIGURATION_KEY.toLowerCase()));

        // Try to retrieve a configuration using an invalid key.
        assertNull(configurationDao.getConfigurationByKey("I_DO_NOT_EXIST"));
    }

    /**
     * Creates and persists a new configuration entity.
     *
     * @param key the configuration key
     * @param value the configuration value
     *
     * @return the newly created configuration entity
     */
    private ConfigurationEntity createConfigurationEntity(String key, String value)
    {
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setKey(key);
        configurationEntity.setValue(value);
        return configurationDao.saveAndRefresh(configurationEntity);
    }
}
