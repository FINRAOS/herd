package org.finra.herd.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import io.searchbox.client.JestClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.listeners.CollectCreatedMocks;
import org.mockito.internal.progress.MockingProgress;
import org.mockito.internal.progress.ThreadSafeMockingProgress;

import org.finra.herd.core.helper.ConfigurationHelper;
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


    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
        createdMocks = new LinkedList<>();
        createdMocks.add(configurationHelper);
        final MockingProgress progress = new ThreadSafeMockingProgress();
        progress.setListener(new CollectCreatedMocks(createdMocks));
    }

    @Test
    public void testGetJestClient() throws Exception
    {
        // Mock
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME)).thenReturn("localhost");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class)).thenReturn(9200);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME)).thenReturn("http");

        // Test
        JestClient jestClient = jestClientFactory.getJestClient();

        // Validate
        assertThat(jestClient, is(not(nullValue())));

        // Verify
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        verifyNoMoreInteractions(createdMocks.toArray());
    }
}
