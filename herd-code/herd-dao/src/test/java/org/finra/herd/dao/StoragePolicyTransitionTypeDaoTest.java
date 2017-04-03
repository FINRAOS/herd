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
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;

public class StoragePolicyTransitionTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStoragePolicyTransitionTypeByCode()
    {
        // Create and persist a storage policy rule type entity.
        StoragePolicyTransitionTypeEntity storagePolicyTransitionTypeEntity =
            storagePolicyTransitionTypeDaoTestHelper.createStoragePolicyTransitionTypeEntity(STORAGE_POLICY_TRANSITION_TYPE);

        // Retrieve this storage policy rule type entity by code.
        StoragePolicyTransitionTypeEntity resultStoragePolicyTransitionTypeEntity =
            storagePolicyTransitionTypeDao.getStoragePolicyTransitionTypeByCode(STORAGE_POLICY_TRANSITION_TYPE);

        // Validate the returned object.
        assertNotNull(resultStoragePolicyTransitionTypeEntity);
        assertEquals(storagePolicyTransitionTypeEntity.getCode(), resultStoragePolicyTransitionTypeEntity.getCode());

        // Retrieve this storage policy rule type entity by code in upper case.
        resultStoragePolicyTransitionTypeEntity =
            storagePolicyTransitionTypeDao.getStoragePolicyTransitionTypeByCode(STORAGE_POLICY_TRANSITION_TYPE.toUpperCase());

        // Validate the returned object.
        assertNotNull(resultStoragePolicyTransitionTypeEntity);
        assertEquals(storagePolicyTransitionTypeEntity.getCode(), resultStoragePolicyTransitionTypeEntity.getCode());

        // Retrieve this storage policy rule type entity by code in lower case.
        resultStoragePolicyTransitionTypeEntity =
            storagePolicyTransitionTypeDao.getStoragePolicyTransitionTypeByCode(STORAGE_POLICY_TRANSITION_TYPE.toLowerCase());

        // Validate the returned object.
        assertNotNull(resultStoragePolicyTransitionTypeEntity);
        assertEquals(storagePolicyTransitionTypeEntity.getCode(), resultStoragePolicyTransitionTypeEntity.getCode());
    }
}
