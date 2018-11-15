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
package org.finra.herd.dao.helper;

import static org.finra.herd.dao.AbstractDaoTest.AWS_REGION_NAME;
import static org.finra.herd.dao.AbstractDaoTest.CREDSTASH_ENCRYPTION_CONTEXT;
import static org.finra.herd.dao.AbstractDaoTest.EMPTY_STRING;
import static org.finra.herd.dao.AbstractDaoTest.ERROR_MESSAGE;
import static org.finra.herd.dao.AbstractDaoTest.HTTP_PROXY_HOST;
import static org.finra.herd.dao.AbstractDaoTest.HTTP_PROXY_PORT;
import static org.finra.herd.dao.AbstractDaoTest.KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_ACCESS_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_REGION_NAME;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_SECRET_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_SESSION_TOKEN;
import static org.finra.herd.dao.AbstractDaoTest.PASSWORD;
import static org.finra.herd.dao.AbstractDaoTest.TABLE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.USER_CREDENTIAL_NAME;
import static org.finra.herd.dao.AbstractDaoTest.VALUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.CredStashFactory;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

public class CredStashHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashFactory credStashFactory;

    @InjectMocks
    private CredStashHelper credStashHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetCredentialFromCredStash() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, NO_AWS_REGION_NAME);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenReturn(PASSWORD);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

        // Call the method under test.
        String result = credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(credStashFactory).getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration);
        verify(jsonHelper).unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT);
        verify(credStash).getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap);
        verifyNoMoreInteractions(credStash);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(PASSWORD, result);
    }

    @Test
    public void testGetCredentialFromCredStashEmptyPasswordValue() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, NO_AWS_REGION_NAME);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenReturn(EMPTY_STRING);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

        // Specify the expected exception.
        expectedException.expect(CredStashGetCredentialFailedException.class);
        expectedException.expectMessage(String
            .format("Failed to obtain credential from credstash. credStashAwsRegion=%s credStashTableName=%s credStashEncryptionContext=%s credentialName=%s",
                AWS_REGION_NAME, TABLE_NAME, CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME));

        // Try to call the method under test.
        credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);

        // Verify the external calls.
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
    public void testGetCredentialFromCredStashException() throws Exception
    {
        // Build AWS parameters.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, NO_AWS_REGION_NAME);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create CredStash encryption context map.
        Map<String, String> credStashEncryptionContextMap = new HashMap<>();
        credStashEncryptionContextMap.put(KEY, VALUE);

        // Mock the CredStash.
        CredStash credStash = mock(CredStash.class);
        when(credStash.getCredential(USER_CREDENTIAL_NAME, credStashEncryptionContextMap)).thenThrow(new Exception(ERROR_MESSAGE));

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn(TABLE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(credStashFactory.getCredStash(AWS_REGION_NAME, TABLE_NAME, clientConfiguration)).thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn(credStashEncryptionContextMap);

        // Specify the expected exception.
        expectedException.expect(CredStashGetCredentialFailedException.class);
        expectedException.expectMessage(String.format("Failed to obtain credential from credstash. Reason: %s " +
                "credStashAwsRegion=%s credStashTableName=%s credStashEncryptionContext=%s credentialName=%s", ERROR_MESSAGE, AWS_REGION_NAME, TABLE_NAME,
            CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME));

        // Try to call the method under test.
        credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);

        // Verify the external calls.
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
