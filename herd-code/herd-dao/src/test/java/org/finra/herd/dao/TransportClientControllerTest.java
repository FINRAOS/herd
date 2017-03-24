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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.listeners.CollectCreatedMocks;
import org.mockito.internal.progress.MockingProgress;
import org.mockito.internal.progress.ThreadSafeMockingProgress;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;

/**
 * TransportClientControllerTest
 */
public class TransportClientControllerTest
{
    private List<Object> createdMocks;

    @InjectMocks
    private TransportClientController transportClientController;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private ClusterHealthResponseFactory clusterHealthResponseFactory;


    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
        createdMocks = new LinkedList<>();
        final MockingProgress progress = new ThreadSafeMockingProgress();
        progress.setListener(new CollectCreatedMocks(createdMocks));
    }

    @Test
    public void testControlTransportClient()
    {
        // Build the mocks
        ClusterHealthResponse clusterHealthResponse = mock(ClusterHealthResponse.class);

        // Mock the call to external methods
        when(clusterHealthResponseFactory.getClusterHealthResponse())
            .thenReturn(clusterHealthResponse);
        when(clusterHealthResponse.getNumberOfNodes())
            .thenReturn(1);

        // Call the method under test
        transportClientController.controlTransportClient();

        // Verify the calls to external methods
        verify(clusterHealthResponseFactory).getClusterHealthResponse();
        verify(clusterHealthResponse).getNumberOfNodes();
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testControlTransportClientNoNodes()
    {
        // Build the mocks
        ClusterHealthResponse clusterHealthResponse = mock(ClusterHealthResponse.class);
        Cache cache = mock(Cache.class);

        // Mock the call to external methods
        when(clusterHealthResponseFactory.getClusterHealthResponse())
            .thenReturn(clusterHealthResponse);
        when(clusterHealthResponse.getNumberOfNodes())
            .thenReturn(0);
        when(cacheManager.getCache(DaoSpringModuleConfig.TRANSPORT_CLIENT_CACHE_NAME))
            .thenReturn(cache);

        // Call the method under test
        transportClientController.controlTransportClient();

        // Verify the calls to external methods
        verify(clusterHealthResponseFactory).getClusterHealthResponse();
        verify(clusterHealthResponse).getNumberOfNodes();
        verify(cache).clear();
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testControlTransportClientException()
    {
        // Build the mocks
        ClusterHealthResponse clusterHealthResponse = mock(ClusterHealthResponse.class);
        Cache cache = mock(Cache.class);

        // Mock the call to external methods
        when(clusterHealthResponseFactory.getClusterHealthResponse())
            .thenThrow(new IllegalArgumentException());
        when(cacheManager.getCache(DaoSpringModuleConfig.TRANSPORT_CLIENT_CACHE_NAME))
            .thenReturn(cache);

        // Call the method under test
        transportClientController.controlTransportClient();

        // Verify the calls to external methods
        verify(clusterHealthResponseFactory).getClusterHealthResponse();
        verify(cache).clear();
        verifyNoMoreInteractions(createdMocks.toArray());
    }
}
