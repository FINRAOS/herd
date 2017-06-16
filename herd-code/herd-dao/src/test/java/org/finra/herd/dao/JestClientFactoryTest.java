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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import io.searchbox.client.JestClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * JestClientFactoryTest
 */
public class JestClientFactoryTest
{
    private List<Object> createdMocks;

    @InjectMocks
    private JestClientFactory jestClientFactory;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashFactory credStashFactory;

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetJestClient() throws Exception
    {
        String credStashName = "credStashName";
        String userName = "userName";
        String password = "password";
        // Build AWS parameters.
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        Map<String, String> credstashEncryptionContextMap = new HashMap<>();
        credstashEncryptionContextMap.put("testKey", "testValue");
        // Mock
        CredStash credStash = mock(CredStash.class);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn("localhost");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(9200);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("http");

        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn("credstashEncryptionContext");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn("us-east-1");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn("table");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(credStashName);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(userName);

        when(credStashFactory.getCredStash("us-east-1", "table", clientConfiguration)).thenReturn(credStash);

        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);

        when(jsonHelper.unmarshallJsonToObject(Map.class, "credstashEncryptionContext")).thenReturn(credstashEncryptionContextMap);
        when(credStash.getCredential(credStashName, credstashEncryptionContextMap)).thenReturn(password);

        // Test
        JestClient jestClient = jestClientFactory.getJestClient();
        verify(credStashFactory).getCredStash("us-east-1", "table", clientConfiguration);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);

        verify(jsonHelper).unmarshallJsonToObject(Map.class, "credstashEncryptionContext");
        verify(credStash).getCredential(credStashName, credstashEncryptionContextMap);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);

        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME);
    }

    @Test
    public void testGetJestClientCredstashException() throws Exception
    {
        String credStashName = "credStashName";
        String userName = "userName";
        String password = null;
        // Build AWS parameters.
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        Map<String, String> credstashEncryptionContextMap = new HashMap<>();
        credstashEncryptionContextMap.put("testKey", "testValue");
        // Mock
        CredStash credStash = mock(CredStash.class);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn("localhost");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(9200);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("http");

        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT)).thenReturn("credstashEncryptionContext");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME)).thenReturn("us-east-1");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME)).thenReturn("table");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME)).thenReturn(credStashName);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME)).thenReturn(userName);

        when(credStashFactory.getCredStash("us-east-1", "table", clientConfiguration)).thenReturn(credStash);

        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);

        when(jsonHelper.unmarshallJsonToObject(Map.class, "credstashEncryptionContext")).thenReturn(credstashEncryptionContextMap);
        when(credStash.getCredential(credStashName, credstashEncryptionContextMap)).thenReturn(password);

        // Test
        try
        {
            JestClient jestClient = jestClientFactory.getJestClient();
            Assert.fail("should throw exception before");
        } catch (Exception e)
        {
        }

        verify(credStashFactory).getCredStash("us-east-1", "table", clientConfiguration);
        verify(awsHelper).getAwsParamsDto();
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(jsonHelper).unmarshallJsonToObject(Map.class, "credstashEncryptionContext");
        verify(credStash).getCredential(credStashName, credstashEncryptionContextMap);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME);
    }
}
