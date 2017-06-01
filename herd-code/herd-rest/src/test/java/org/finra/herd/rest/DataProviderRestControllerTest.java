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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;
import org.finra.herd.service.DataProviderService;

/**
 * This class tests various functionality within the data provider REST controller.
 */
public class DataProviderRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private DataProviderRestController dataProviderRestController;

    @Mock
    private DataProviderService dataProviderService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateDataProvider() throws Exception
    {
        DataProviderCreateRequest dataProviderCreateRequest = new DataProviderCreateRequest(DATA_PROVIDER_NAME);
        // Create a data provider.
        DataProvider dataProvider = new DataProvider(DATA_PROVIDER_NAME);
        when(dataProviderService.createDataProvider(dataProviderCreateRequest)).thenReturn(dataProvider);

        DataProvider resultDataProvider = dataProviderRestController.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);

        // Verify the external calls.
        verify(dataProviderService).createDataProvider(dataProviderCreateRequest);
        verifyNoMoreInteractions(dataProviderService);
        // Validate the returned object.
        assertEquals(dataProvider, resultDataProvider);
    }

    @Test
    public void testDeleteDataProvider() throws Exception
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);
        DataProvider dataProvider = new DataProvider(DATA_PROVIDER_NAME);

        when(dataProviderService.deleteDataProvider(dataProviderKey)).thenReturn(dataProvider);

        DataProvider deletedDataProvider = dataProviderRestController.deleteDataProvider(DATA_PROVIDER_NAME);

        // Verify the external calls.
        verify(dataProviderService).deleteDataProvider(dataProviderKey);
        verifyNoMoreInteractions(dataProviderService);
        // Validate the returned object.
        assertEquals(dataProvider, deletedDataProvider);
    }

    @Test
    public void testGetDataProvider() throws Exception
    {
        DataProvider dataProvider = new DataProvider(DATA_PROVIDER_NAME);
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);
        when(dataProviderService.getDataProvider(dataProviderKey)).thenReturn(dataProvider);

        // Retrieve the data provider.
        DataProvider resultDataProvider = dataProviderRestController.getDataProvider(DATA_PROVIDER_NAME);

        // Verify the external calls.
        verify(dataProviderService).getDataProvider(dataProviderKey);
        verifyNoMoreInteractions(dataProviderService);
        // Validate the returned object.
        assertEquals(dataProvider, resultDataProvider);
    }

    @Test
    public void testGetDataProviders() throws Exception
    {
        DataProviderKeys dataProviderKeys =
            new DataProviderKeys(Arrays.asList(new DataProviderKey(DATA_PROVIDER_NAME), new DataProviderKey(DATA_PROVIDER_NAME_2)));

        when(dataProviderService.getDataProviders()).thenReturn(dataProviderKeys);

        // Retrieve a list of data provider keys.
        DataProviderKeys resultDataProviderKeys = dataProviderRestController.getDataProviders();

        // Verify the external calls.
        verify(dataProviderService).getDataProviders();
        verifyNoMoreInteractions(dataProviderService);
        // Validate the returned object.
        assertEquals(dataProviderKeys, resultDataProviderKeys);
    }
}
