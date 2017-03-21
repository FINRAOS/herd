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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.listeners.CollectCreatedMocks;
import org.mockito.internal.progress.MockingProgress;
import org.mockito.internal.progress.ThreadSafeMockingProgress;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchSettingsDto;

/**
 * TransportClientFactoryTest
 */
public class TransportClientFactoryTest
{
    private List<Object> createdMocks;

    @InjectMocks
    private TransportClientFactory transportClientFactory;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashFactory credStashFactory;

    @Mock
    private InputStreamFactory inputStreamFactory;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private PreBuiltTransportClientFactory preBuiltTransportClientFactory;


    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
        createdMocks = new LinkedList<>();
        final MockingProgress progress = new ThreadSafeMockingProgress();
        progress.setListener(new CollectCreatedMocks(createdMocks));
    }

    @Test
    public void testGetTransportClient() throws Exception
    {
        // Build a list of elasticsearch addresses
        List<String> elasticSearchAddresses = new ArrayList<>();
        elasticSearchAddresses.add("127.0.0.1");

        // Build an elasticsearch settings data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = new ElasticsearchSettingsDto();
        elasticsearchSettingsDto.setClientTransportAddresses(elasticSearchAddresses);

        // Build the mocks
        PreBuiltTransportClient preBuiltTransportClient = mock(PreBuiltTransportClient.class);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON))
            .thenReturn("elasticSearchSettingsJSON");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class))
            .thenReturn(9300);
        when(jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON"))
            .thenReturn(elasticsearchSettingsDto);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED))
            .thenReturn("false");
        when(preBuiltTransportClientFactory.getPreBuiltTransportClient(any()))
            .thenReturn(preBuiltTransportClient);

        // Call the method under test
        TransportClient transportClient = transportClientFactory.getTransportClient();

        assertThat(transportClient, is(not(nullValue())));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);
        verify(jsonHelper, times(1)).unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON");
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED);
        verify(preBuiltTransportClientFactory, times(1)).getPreBuiltTransportClient(any());
        verify(transportClient, times(1))
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testGetTransportClientWithBogusAddress() throws Exception
    {
        // Build a list of elasticsearch addresses
        List<String> elasticSearchAddresses = new ArrayList<>();
        elasticSearchAddresses.add("BOGUS");

        // Build an elasticsearch settings data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = new ElasticsearchSettingsDto();
        elasticsearchSettingsDto.setClientTransportAddresses(elasticSearchAddresses);

        // Build the mocks
        PreBuiltTransportClient preBuiltTransportClient = mock(PreBuiltTransportClient.class);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON))
            .thenReturn("elasticSearchSettingsJSON");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class))
            .thenReturn(9300);
        when(jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON"))
            .thenReturn(elasticsearchSettingsDto);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED))
            .thenReturn("false");
        when(preBuiltTransportClientFactory.getPreBuiltTransportClient(any()))
            .thenReturn(preBuiltTransportClient);

        // Call the method under test
        TransportClient transportClient = transportClientFactory.getTransportClient();

        assertThat(transportClient, is(not(nullValue())));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);
        verify(jsonHelper, times(1)).unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON");
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED);
        verify(preBuiltTransportClientFactory, times(1)).getPreBuiltTransportClient(any());
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testGetTransportClientWithCredStash() throws Exception
    {
        // Build a list of elasticsearch addresses
        List<String> elasticSearchAddresses = new ArrayList<>();
        elasticSearchAddresses.add("127.0.0.1");

        // Build an elasticsearch settings data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = new ElasticsearchSettingsDto();
        elasticsearchSettingsDto.setClientTransportAddresses(elasticSearchAddresses);

        // Build a credstashEncryptionContextMap
        Map<String, String> credstashEncryptionContextMap = new HashMap<>();
        credstashEncryptionContextMap.put("testKey", "testValue");

        // Build the mocks
        CredStash credStash = mock(CredStash.class);
        PreBuiltTransportClient preBuiltTransportClient = mock(PreBuiltTransportClient.class);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON))
            .thenReturn("elasticSearchSettingsJSON");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class))
            .thenReturn(9300);
        when(jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON"))
            .thenReturn(elasticsearchSettingsDto);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED))
            .thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH))
            .thenReturn("keystorePath");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH))
            .thenReturn("truststorePath");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT))
            .thenReturn("credstashEncryptionContext");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME))
            .thenReturn("us-east-1");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME))
            .thenReturn("table");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME))
            .thenReturn("keystoreCredential");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME))
            .thenReturn("truststoreCredential");
        when(credStashFactory.getCredStash("us-east-1", "table"))
            .thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, "credstashEncryptionContext"))
            .thenReturn(credstashEncryptionContextMap);
        when(credStash.getCredential("keystoreCredential", credstashEncryptionContextMap))
            .thenReturn("keystorePassword");
        when(credStash.getCredential("truststoreCredential", credstashEncryptionContextMap))
            .thenReturn("truststorePassword");
        when(inputStreamFactory.getFileInputStream("keystorePath"))
            .thenReturn(IOUtils.toInputStream("testKeystore", "UTF-8"));
        when(inputStreamFactory.getFileInputStream("truststorePath"))
            .thenReturn(IOUtils.toInputStream("testTruststore", "UTF-8"));
        when(preBuiltTransportClientFactory.getPreBuiltTransportClientWithSearchGuardPlugin(any()))
            .thenReturn(preBuiltTransportClient);

        // Call the method under test
        TransportClient transportClient = transportClientFactory.getTransportClient();

        assertThat(transportClient, is(not(nullValue())));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);
        verify(jsonHelper, times(1)).unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON");
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME);
        verify(credStashFactory, times(1)).getCredStash("us-east-1", "table");
        verify(jsonHelper, times(1)).unmarshallJsonToObject(Map.class, "credstashEncryptionContext");
        verify(credStash, times(1)).getCredential("keystoreCredential", credstashEncryptionContextMap);
        verify(credStash, times(1)).getCredential("truststoreCredential", credstashEncryptionContextMap);
        verify(inputStreamFactory, times(1)).getFileInputStream("keystorePath");
        verify(inputStreamFactory, times(1)).getFileInputStream("truststorePath");
        verify(preBuiltTransportClientFactory, times(1)).getPreBuiltTransportClientWithSearchGuardPlugin(any());
        verify(transportClient, times(1))
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testGetTransportClientWithCredStashWithIOException() throws Exception
    {
        // Build a list of elasticsearch addresses
        List<String> elasticSearchAddresses = new ArrayList<>();
        elasticSearchAddresses.add("127.0.0.1");

        // Build an elasticsearch settings data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = new ElasticsearchSettingsDto();
        elasticsearchSettingsDto.setClientTransportAddresses(elasticSearchAddresses);

        // Build the mocks
        CredStash credStash = mock(CredStash.class);
        PreBuiltTransportClient preBuiltTransportClient = mock(PreBuiltTransportClient.class);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON))
            .thenReturn("elasticSearchSettingsJSON");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class))
            .thenReturn(9300);
        when(jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON"))
            .thenThrow(new IOException());
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED))
            .thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH))
            .thenReturn("keystorePath");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH))
            .thenReturn("truststorePath");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT))
            .thenReturn("credstashEncryptionContext");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME))
            .thenReturn("us-east-1");
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME))
            .thenReturn("table");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME))
            .thenReturn("keystoreCredential");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME))
            .thenReturn("truststoreCredential");
        when(credStashFactory.getCredStash("us-east-1", "table"))
            .thenReturn(credStash);
        when(jsonHelper.unmarshallJsonToObject(Map.class, "credstashEncryptionContext"))
            .thenThrow(new IOException());
        when(preBuiltTransportClientFactory.getPreBuiltTransportClient(any()))
            .thenReturn(preBuiltTransportClient);

        // Call the method under test
        TransportClient transportClient =  transportClientFactory.getTransportClient();

        assertThat(transportClient, is(not(nullValue())));

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);
        verify(jsonHelper, times(1)).unmarshallJsonToObject(ElasticsearchSettingsDto.class, "elasticSearchSettingsJSON");
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME);
        verify(credStashFactory, times(1)).getCredStash("us-east-1", "table");
        verify(jsonHelper, times(1)).unmarshallJsonToObject(Map.class, "credstashEncryptionContext");
        verify(preBuiltTransportClientFactory, times(1)).getPreBuiltTransportClient(any());
        verifyNoMoreInteractions(createdMocks.toArray());
    }
}
