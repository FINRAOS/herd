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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.ConfigurationEntry;
import org.finra.herd.model.api.xml.ConfigurationEntryKeys;
import org.finra.herd.service.ConfigurationEntryService;

/**
 * This class tests configuration entry functionality within the configuration entry REST controller.
 */
public class ConfigurationEntryRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private ConfigurationEntryRestController configurationEntryRestController;

    @Mock
    private ConfigurationEntryService configurationEntryService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetConfigurationEntry()
    {
        // Create a configuration entry key and value and value clob.
        String configurationEntryKey = "KEY";
        String configurationEntryValue = "VALUE";
        String configurationEntryValueClob = "VALUE CLOB";

        // Create a configuration entry get response.
        ConfigurationEntry configurationEntry = new ConfigurationEntry(configurationEntryKey, configurationEntryValue, configurationEntryValueClob);

        // Mock the call to the configuration entry service.
        when(configurationEntryService.getConfigurationEntry(configurationEntryKey)).thenReturn(configurationEntry);

        // Get a configuration entry.
        ConfigurationEntry response = configurationEntryRestController.getConfigurationEntry(configurationEntryKey);

        // Verify the call.
        verify(configurationEntryService).getConfigurationEntry(configurationEntryKey);

        // Validate the returned object.
        assertEquals("Response not equal to expected configuration entry response.", configurationEntry, response);
    }

    @Test
    public void testGetConfigurationEntries()
    {
        // Create a configuration entry keys.
        String configurationEntryKey1 = "KEY1";
        String configurationEntryKey2 = "KEY2";
        String configurationEntryKey3 = "KEY3";

        // Create a get configuration entry keys response.
        ConfigurationEntryKeys configurationEntryKeys =
            new ConfigurationEntryKeys(Arrays.asList(configurationEntryKey1, configurationEntryKey2, configurationEntryKey3));

        // Mock the call to the configuration entry service.
        when(configurationEntryService.getConfigurationEntries()).thenReturn(configurationEntryKeys);

        // Get configuration entry keys.
        ConfigurationEntryKeys response = configurationEntryRestController.getConfigurationEntries();

        // Verify the call.
        verify(configurationEntryService).getConfigurationEntries();

        // Validate the returned object.
        assertEquals("Response not equal to expected configuration entry keys response.", configurationEntryKeys, response);
    }
}
