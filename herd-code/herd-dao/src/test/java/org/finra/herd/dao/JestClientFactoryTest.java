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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.searchbox.client.JestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.model.dto.ConfigurationValue;

public class JestClientFactoryTest extends AbstractDaoTest
{
    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashHelper credStashHelper;

    @InjectMocks
    private JestClientFactory jestClientFactory;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetJestClientCredStashException() throws Exception
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("https");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(USERNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME))
            .thenThrow(new CredStashGetCredentialFailedException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            jestClientFactory.getJestClient();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("%s: %s", CredStashGetCredentialFailedException.class.getName(), ERROR_MESSAGE), e.getMessage());
        }

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetJestClientHttp()
    {
        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("http");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);

        // Call the method under test.
        JestClient jestClient = jestClientFactory.getJestClient();

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class);
        verifyNoMoreInteractions(credStash);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNotNull(jestClient);
    }

    @Test
    public void testGetJestClientHttps() throws Exception
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("https");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(USERNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME)).thenReturn(PASSWORD);

        // Call the method under test.
        JestClient jestClient = jestClientFactory.getJestClient();

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNotNull(jestClient);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(configurationHelper, credStashHelper);
    }
}
