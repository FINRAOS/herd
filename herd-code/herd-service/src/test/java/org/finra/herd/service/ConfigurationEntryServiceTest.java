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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ConfigurationEntry;
import org.finra.herd.model.api.xml.ConfigurationEntryKeys;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.impl.ConfigurationEntryServiceImpl;

/**
 * This class tests configuration entry functionality within the configuration entry service.
 */
public class ConfigurationEntryServiceTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private ConfigurationEntryServiceImpl configurationEntryService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetConfigurationEntity()
    {
        // Create a configuration entry key and value and value clob.
        ConfigurationValue configurationValue = ConfigurationValue.values()[0];
        String configurationEntryKey = configurationValue.getKey();
        String configurationEntryValue = "VALUE";
        String configurationEntryValueClob = "VALUE CLOB";

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Configuration entry key", configurationEntryKey)).thenReturn(configurationEntryKey);
        when(configurationHelper.getProperty(configurationValue)).thenReturn(configurationEntryValue);
        when(configurationDaoHelper.getClobProperty(configurationEntryKey)).thenReturn(configurationEntryValueClob);

        // Get a configuration entry.
        ConfigurationEntry response = configurationEntryService.getConfigurationEntry(configurationEntryKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Configuration entry key", configurationEntryKey);
        verify(configurationHelper).getProperty(configurationValue);
        verify(configurationDaoHelper).getClobProperty(configurationEntryKey);
        verifyNoMoreInteractions(alternateKeyHelper, configurationDaoHelper, configurationHelper);

        // Validate the returned object.
        assertEquals("Response object not equal to the correct configuration entry.",
            new ConfigurationEntry(configurationEntryKey, configurationEntryValue, configurationEntryValueClob), response);
    }

    @Test
    public void testGetConfigurationEntityWithEmptyValues()
    {
        // Create a configuration entry key and value and value clob.
        ConfigurationValue configurationValue = ConfigurationValue.values()[0];
        String configurationEntryKey = configurationValue.getKey();

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Configuration entry key", configurationEntryKey)).thenReturn(configurationEntryKey);
        when(configurationHelper.getProperty(configurationValue)).thenReturn(null);
        when(configurationDaoHelper.getClobProperty(configurationEntryKey)).thenReturn(null);

        // Get a configuration entry.
        ConfigurationEntry response = configurationEntryService.getConfigurationEntry(configurationEntryKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Configuration entry key", configurationEntryKey);
        verify(configurationHelper).getProperty(configurationValue);
        verify(configurationDaoHelper).getClobProperty(configurationEntryKey);
        verifyNoMoreInteractions(alternateKeyHelper, configurationDaoHelper, configurationHelper);

        // Validate the returned object.
        assertEquals("Response object not equal to the correct configuration entry.",
            new ConfigurationEntry(configurationEntryKey, null, null), response);
    }

    @Test
    public void testGetConfigurationEntityWithObjectNotFoundException()
    {
        // Create a configuration entry key and value and value clob.
        String configurationEntryKey = "KEY" + getRandomSuffix();

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Configuration entry key", configurationEntryKey)).thenReturn(configurationEntryKey);

        // Try to get a non-existing configuration entry.
        try
        {
            configurationEntryService.getConfigurationEntry(configurationEntryKey);
            fail("Should throw an ObjectNotFoundException when configuration entry doesn't exist.");
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            assertEquals(String.format("Configuration entry with key \"%s\" doesn't exist.", configurationEntryKey), objectNotFoundException.getMessage());
        }

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Configuration entry key", configurationEntryKey);
        verifyNoMoreInteractions(alternateKeyHelper, configurationDaoHelper, configurationHelper);
    }

    @Test
    public void testGetConfigurationEntries()
    {
        // Get configuration entry keys.
        ConfigurationEntryKeys response = configurationEntryService.getConfigurationEntries();

        // Validate the returned object.
        assertEquals("Response object not equal to the correct configuration entries.",
            createConfigurationEntryKeysWithAllConfigurationValues(), response);
    }

    /**
     * Creates a configuration entry keys object loaded with all configuration values from the Configuration Value enum.
     *
     * @return the configuration entry keys
     */
    private ConfigurationEntryKeys createConfigurationEntryKeysWithAllConfigurationValues()
    {
        // For each configuration value add its key to the keys list.
        List<String> configurationEntryKeysList = new ArrayList<>();
        for (ConfigurationValue configurationValue : ConfigurationValue.values())
        {
            configurationEntryKeysList.add(configurationValue.getKey());
        }

        return new ConfigurationEntryKeys(configurationEntryKeysList);
    }
}
