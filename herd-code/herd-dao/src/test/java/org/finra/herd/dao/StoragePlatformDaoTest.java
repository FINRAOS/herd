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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.jpa.StoragePlatformEntity;

public class StoragePlatformDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStoragePlatformByName()
    {
        // Create relative database entities.
        StoragePlatformEntity storagePlatformEntity = createStoragePlatformEntity(STORAGE_PLATFORM_CODE);

        // Retrieve the storage platform entity by its name.
        assertEquals(storagePlatformEntity, storagePlatformDao.getStoragePlatformByName(STORAGE_PLATFORM_CODE));

        // Retrieve the storage platform entity using its name in upper and lower case.
        assertEquals(storagePlatformEntity, storagePlatformDao.getStoragePlatformByName(STORAGE_PLATFORM_CODE.toUpperCase()));
        assertEquals(storagePlatformEntity, storagePlatformDao.getStoragePlatformByName(STORAGE_PLATFORM_CODE.toLowerCase()));

        // Try to retrieve a storage platform entity using an invalid name.
        assertNull(storagePlatformDao.getStoragePlatformByName("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStoragePlatformByNameMultipleRecordsFound()
    {
        // Create relative database entities.
        createStoragePlatformEntity(STORAGE_PLATFORM_CODE.toUpperCase());
        createStoragePlatformEntity(STORAGE_PLATFORM_CODE.toLowerCase());

        // Try to retrieve a storage platform when multiple entities exist with the same name (using case insensitive string comparison).
        try
        {
            storagePlatformDao.getStoragePlatformByName(STORAGE_PLATFORM_CODE);
            fail("Should throw an IllegalArgumentException if finds more than one storage platform entity with the same name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one storage platform with \"%s\" name.", STORAGE_PLATFORM_CODE), e.getMessage());
        }
    }
}
