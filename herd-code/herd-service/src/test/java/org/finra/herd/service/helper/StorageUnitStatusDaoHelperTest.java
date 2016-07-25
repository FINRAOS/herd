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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the StorageUnitStatusDaoHelper class.
 */
public class StorageUnitStatusDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetStorageUnitStatusEntity()
    {
        // Create and persist database entities required for testing.
        List<StorageUnitStatusEntity> storageUnitStatusEntity = Arrays.asList(storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS),
            storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2));

        // Retrieve the storage unit status entity.
        assertEquals(storageUnitStatusEntity.get(0), storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS));
        assertEquals(storageUnitStatusEntity.get(1), storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2));

        // Test case insensitivity of the storage unit status code.
        assertEquals(storageUnitStatusEntity.get(0), storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS.toUpperCase()));
        assertEquals(storageUnitStatusEntity.get(0), storageUnitStatusDaoHelper.getStorageUnitStatusEntity(STORAGE_UNIT_STATUS.toLowerCase()));

        // Try to retrieve a non existing storage unit status.
        try
        {
            storageUnitStatusDaoHelper.getStorageUnitStatusEntity("I_DO_NOT_EXIST");
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Storage unit status \"I_DO_NOT_EXIST\" doesn't exist.", e.getMessage());
        }
    }
}
