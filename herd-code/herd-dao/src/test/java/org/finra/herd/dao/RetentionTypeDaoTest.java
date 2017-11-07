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
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.finra.herd.model.jpa.RetentionTypeEntity;

public class RetentionTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetRetentionTypeByCode()
    {
        RetentionTypeEntity existingRetentionType = retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE);
        if (existingRetentionType == null)
        {
            retentionTypeDaoTestHelper.createRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE);
        }

        // Create database entities required for testing.
        List<RetentionTypeEntity> retentionTypeEntityList = Arrays.asList(
            existingRetentionType, retentionTypeDaoTestHelper.createRetentionTypeEntity("REGISTRATION_DATE"));

        // Retrieve the relative retention type entities and validate the results.
        assertEquals(retentionTypeEntityList.get(0), retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE));
        assertEquals(retentionTypeEntityList.get(1), retentionTypeDao.getRetentionTypeByCode("REGISTRATION_DATE"));

        // Test case insensitivity for the retention type.
        assertEquals(retentionTypeEntityList.get(0), retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE.toLowerCase()));
        assertEquals(retentionTypeEntityList.get(0), retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE.toUpperCase()));
        
        // Confirm negative results when using non-existing retention type.
        assertNull(retentionTypeDao.getRetentionTypeByCode("I_DO_NOT_EXIST"));
    }
}
