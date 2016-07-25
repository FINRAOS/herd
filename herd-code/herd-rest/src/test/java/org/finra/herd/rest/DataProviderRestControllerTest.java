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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;

/**
 * This class tests various functionality within the data provider REST controller.
 */
public class DataProviderRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateDataProvider() throws Exception
    {
        // Create a data provider.
        DataProvider resultDataProvider = dataProviderRestController.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testGetDataProvider() throws Exception
    {
        // Create and persist a data provider entity.
        dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider.
        DataProvider resultDataProvider = dataProviderRestController.getDataProvider(DATA_PROVIDER_NAME);

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testDeleteDataProvider() throws Exception
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);

        // Create and persist a data provider entity.
        dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Validate that this data provider exists.
        assertNotNull(dataProviderDao.getDataProviderByKey(dataProviderKey));

        // Delete this data provider.
        DataProvider deletedDataProvider = dataProviderRestController.deleteDataProvider(DATA_PROVIDER_NAME);

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), deletedDataProvider);

        // Ensure that this data provider is no longer there.
        assertNull(dataProviderDao.getDataProviderByKey(dataProviderKey));
    }

    @Test
    public void testGetDataProviders() throws Exception
    {
        // Create and persist data provider entities.
        for (DataProviderKey key : DATA_PROVIDER_KEYS)
        {
            dataProviderDaoTestHelper.createDataProviderEntity(key.getDataProviderName());
        }

        // Retrieve a list of data provider keys.
        DataProviderKeys resultDataProviderKeys = dataProviderRestController.getDataProviders();

        // Validate the returned object.
        assertEquals(DATA_PROVIDER_KEYS, resultDataProviderKeys.getDataProviderKeys());
    }
}
