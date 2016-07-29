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

import org.junit.Test;

import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.jpa.DataProviderEntity;

public class DataProviderTest extends AbstractDaoTest
{
    @Test
    public void testGetDataProviderByKey()
    {
        // Create a data provider entity.
        DataProviderEntity dataProviderEntity = dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider entity.
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME)));

        // Test case insensitivity of data provider key.
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME.toUpperCase())));
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByKey(new DataProviderKey(DATA_PROVIDER_NAME.toLowerCase())));
    }

    @Test
    public void testGetDataProviderByName()
    {
        // Create a data provider entity.
        DataProviderEntity dataProviderEntity = dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider entity.
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByName(DATA_PROVIDER_NAME));

        // Test case insensitivity of data provider name.
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByName(DATA_PROVIDER_NAME.toUpperCase()));
        assertEquals(dataProviderEntity, dataProviderDao.getDataProviderByName(DATA_PROVIDER_NAME.toLowerCase()));
    }

    @Test
    public void testGetDataProviders()
    {
        // Create and persist data provider entities.
        for (DataProviderKey key : DATA_PROVIDER_KEYS)
        {
            dataProviderDaoTestHelper.createDataProviderEntity(key.getDataProviderName());
        }

        // Retrieve a list of data provider keys and validate the returned object.
        assertEquals(DATA_PROVIDER_KEYS, dataProviderDao.getDataProviders());
    }
}
