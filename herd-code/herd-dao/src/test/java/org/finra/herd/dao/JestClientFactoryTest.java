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

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import io.searchbox.client.JestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

public class JestClientFactoryTest extends AbstractDaoTest
{
    @Mock
    private AwsHelper awsHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashFactory credStashFactory;

    @InjectMocks
    private JestClientFactory jestClientFactory;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetJestClientCredStashException() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto = new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenThrow(new Exception(ERROR_MESSAGE));

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("https");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(USERNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

        // Try to call the method under test.
        try
        {
            jestClientFactory.getJestClient();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("%s: Failed to obtain the keystore or truststore credential from cred stash.", CredStashGetCredentialFailedException.class.getName()),
                e.getMessage());
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
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(credStashFactory).getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration);
        verify(jsonHelper).unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStash).getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap);
        verifyNoMoreInteractions(credStash);
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

        // Validate the result.
        assertNotNull(jestClient);
    }

    @Test
    public void testGetJestClientHttps() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto = new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenReturn(PASSWORD);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("https");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(USERNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

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
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(credStashFactory).getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration);
        verify(jsonHelper).unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStash).getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap);
        verifyNoMoreInteractions(credStash);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertNotNull(jestClient);
    }

    @Test
    public void testGetJestClientPasswordIsNull() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto = new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenReturn(null);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn(ELASTICSEARCH_HOSTNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(ELASTICSEARCH_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("https");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class)).thenReturn(READ_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class)).thenReturn(CONNECTION_TIMEOUT);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(USERNAME);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

        // Try to call the method under test.
        try
        {
            jestClientFactory.getJestClient();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("%s: Failed to obtain the keystore or truststore credential from cred stash.", CredStashGetCredentialFailedException.class.getName()),
                e.getMessage());
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
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(credStashFactory).getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration);
        verify(jsonHelper).unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStash).getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap);
        verifyNoMoreInteractions(credStash);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsHelper, configurationHelper, credStashFactory, jsonHelper);
    }
}
