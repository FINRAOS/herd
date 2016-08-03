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

import org.junit.Test;

public class StorageUnitStatusDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStorageUnitStatusByCode()
    {
        // Create database entities required for testing.
        storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS, DESCRIPTION, STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
        storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2, DESCRIPTION_2, NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);

        // Retrieve the relative storage unit status entities and validate the results.
        assertEquals(STORAGE_UNIT_STATUS, storageUnitStatusDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS).getCode());
        assertEquals(STORAGE_UNIT_STATUS_2, storageUnitStatusDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS_2).getCode());

        // Test case insensitivity for the storage unit status code.
        assertEquals(STORAGE_UNIT_STATUS, storageUnitStatusDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS.toUpperCase()).getCode());
        assertEquals(STORAGE_UNIT_STATUS, storageUnitStatusDao.getStorageUnitStatusByCode(STORAGE_UNIT_STATUS.toLowerCase()).getCode());

        // Confirm negative results when using non-existing storage unit status code.
        assertNull(storageUnitStatusDao.getStorageUnitStatusByCode("I_DO_NOT_EXIST"));
    }
}
