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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.StorageKey;

public class StorageDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStorages()
    {
        // Create and persist storage entities.
        for (StorageKey key : storageDaoTestHelper.getTestStorageKeys())
        {
            storageDaoTestHelper.createStorageEntity(key.getStorageName());
        }

        // Retrieve a list of storage keys.
        List<StorageKey> resultStorageKeys = storageDao.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertTrue(resultStorageKeys.containsAll(storageDaoTestHelper.getTestStorageKeys()));
    }
}
