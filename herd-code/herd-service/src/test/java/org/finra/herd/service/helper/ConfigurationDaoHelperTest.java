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
package org.finra.herd.service.helper;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.ConfigurationDao;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * ConfigurationDaoHelperTest
 */
public class ConfigurationDaoHelperTest
{
    @InjectMocks
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private ConfigurationDao configurationDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetClobProperty()
    {
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setValueClob("CLOB");

        // Mock the call to external methods
        when(configurationDao.getConfigurationByKey("CONFIGURATION_KEY")).thenReturn(configurationEntity);

        // Call the method under test
        String clob = configurationDaoHelper.getClobProperty("CONFIGURATION_KEY");

        assertThat("Clob value is null.", clob, not(nullValue()));
        assertThat("Clob value is not correct.", clob, is("CLOB"));

        // Verify the calls to external methods
        verify(configurationDao, times(1)).getConfigurationByKey("CONFIGURATION_KEY");
    }

    @Test
    public void testGetClobPropertyNull()
    {
        // Mock the call to external methods
        when(configurationDao.getConfigurationByKey("CONFIGURATION_KEY")).thenReturn(null);

        // Call the method under test
        String clob = configurationDaoHelper.getClobProperty("CONFIGURATION_KEY");

        assertThat("Clob value is null.", clob, not(nullValue()));
        assertThat("Clob value is not correct.", clob, is(""));

        // Verify the calls to external methods
        verify(configurationDao, times(1)).getConfigurationByKey("CONFIGURATION_KEY");
    }

    @Test
    public void testGetClobPropertyEmpty()
    {
        ConfigurationEntity configurationEntity = new ConfigurationEntity();
        configurationEntity.setValueClob("");

        // Mock the call to external methods
        when(configurationDao.getConfigurationByKey("CONFIGURATION_KEY")).thenReturn(configurationEntity);

        // Call the method under test
        String clob = configurationDaoHelper.getClobProperty("CONFIGURATION_KEY");

        assertThat("Clob value is null.", clob, not(nullValue()));
        assertThat("Clob value is not correct.", clob, is(""));

        // Verify the calls to external methods
        verify(configurationDao, times(1)).getConfigurationByKey("CONFIGURATION_KEY");
    }
}
