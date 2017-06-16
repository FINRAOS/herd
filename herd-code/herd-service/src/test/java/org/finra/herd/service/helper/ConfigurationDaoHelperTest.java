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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.ConfigurationDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.MethodNotAllowedException;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.service.AbstractServiceTest;

public class ConfigurationDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private ConfigurationDao configurationDao;

    @InjectMocks
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private XmlHelper xmlHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCheckNotAllowedMethod()
    {
        // Create a list of methods to be blocked.
        String blockedMethodListAsText = STRING_VALUE;
        List<String> blockedMethods = Arrays.asList(METHOD_NAME);

        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(blockedMethodListAsText);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey())).thenReturn(configurationEntity);
        when(herdStringHelper.splitStringWithDefaultDelimiter(blockedMethodListAsText)).thenReturn(blockedMethods);

        // Try to call the method under test.
        try
        {
            configurationDaoHelper.checkNotAllowedMethod(METHOD_NAME);
            fail();
        }
        catch (MethodNotAllowedException e)
        {
            assertEquals("The requested method is not allowed.", e.getMessage());
        }

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        verify(herdStringHelper).splitStringWithDefaultDelimiter(blockedMethodListAsText);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCheckNotAllowedMethodBlockedMethodListIsBlank()
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(BLANK_TEXT);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey())).thenReturn(configurationEntity);

        // Call the method under test.
        configurationDaoHelper.checkNotAllowedMethod(METHOD_NAME);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCheckNotAllowedMethodBlockedMethodListIsNull()
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(null);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey())).thenReturn(configurationEntity);

        // Call the method under test.
        configurationDaoHelper.checkNotAllowedMethod(METHOD_NAME);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCheckNotAllowedMethodMethodIsNotBlocked()
    {
        // Create a list of methods to be blocked.
        String blockedMethodListAsText = STRING_VALUE;
        List<String> blockedMethods = Arrays.asList(METHOD_NAME);

        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(blockedMethodListAsText);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey())).thenReturn(configurationEntity);
        when(herdStringHelper.splitStringWithDefaultDelimiter(blockedMethodListAsText)).thenReturn(blockedMethods);

        // Call the method under test.
        configurationDaoHelper.checkNotAllowedMethod(METHOD_NAME_2);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        verify(herdStringHelper).splitStringWithDefaultDelimiter(blockedMethodListAsText);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetClobProperty()
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(CONFIGURATION_VALUE);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(configurationEntity);

        // Call the method under test.
        String result = configurationDaoHelper.getClobProperty(CONFIGURATION_KEY);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(CONFIGURATION_VALUE, result);
    }

    @Test
    public void testGetClobPropertyConfigurationNoExists()
    {
        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(null);

        // Call the method under test.
        String result = configurationDaoHelper.getClobProperty(CONFIGURATION_KEY);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals("", result);
    }

    @Test
    public void testGetClobPropertyConfigurationValueIsBlank()
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(BLANK_TEXT);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(configurationEntity);

        // Call the method under test.
        String result = configurationDaoHelper.getClobProperty(CONFIGURATION_KEY);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals("", result);
    }

    @Test
    public void testGetXmlClobPropertyAndUnmarshallToObject() throws JAXBException
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(CONFIGURATION_VALUE);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(configurationEntity);
        when(xmlHelper.unmarshallXmlToObject(String.class, CONFIGURATION_VALUE)).thenReturn(STRING_VALUE);

        // Call the method under test.
        String result = configurationDaoHelper.getXmlClobPropertyAndUnmarshallToObject(String.class, CONFIGURATION_KEY);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verify(xmlHelper).unmarshallXmlToObject(String.class, CONFIGURATION_VALUE);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(STRING_VALUE, result);
    }

    @Test
    public void testGetXmlClobPropertyAndUnmarshallToObjectConfigurationValueIsBlank()
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(BLANK_TEXT);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(configurationEntity);

        // Call the method under test.
        String result = configurationDaoHelper.getXmlClobPropertyAndUnmarshallToObject(String.class, CONFIGURATION_KEY);

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(result);
    }

    @Test
    public void testGetXmlClobPropertyAndUnmarshallToObjectJAXBException() throws JAXBException
    {
        // Create a mock configuration entity.
        ConfigurationEntity configurationEntity = mock(ConfigurationEntity.class);
        when(configurationEntity.getValueClob()).thenReturn(CONFIGURATION_VALUE);

        // Mock the external calls.
        when(configurationDao.getConfigurationByKey(CONFIGURATION_KEY)).thenReturn(configurationEntity);
        when(xmlHelper.unmarshallXmlToObject(String.class, CONFIGURATION_VALUE)).thenThrow(new JAXBException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            configurationDaoHelper.getXmlClobPropertyAndUnmarshallToObject(String.class, CONFIGURATION_KEY);
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to unmarshall \"%s\" configuration value to %s.", CONFIGURATION_KEY, String.class.getName()), e.getMessage());
        }

        // Verify the external calls.
        verify(configurationDao).getConfigurationByKey(CONFIGURATION_KEY);
        verify(xmlHelper).unmarshallXmlToObject(String.class, CONFIGURATION_VALUE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(configurationDao, herdStringHelper, xmlHelper);
    }
}
