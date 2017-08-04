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

import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;

public class GlobalAttributeDefinitionLevelDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetGlobalAttributeDefinitionLevel()
    {
        // Create and persist a global attribute definition level entity.
        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity =
            globalAttributeDefinitionLevelDaoTestHelper.createGlobalAttributeDefinitionLevelEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);

        // Retrieve the global attribute definition level entity by its code.
        assertEquals(globalAttributeDefinitionLevelEntity,
            globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL));

        // Test case insensitivity.
        assertEquals(globalAttributeDefinitionLevelEntity,
            globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL.toUpperCase()));
        assertEquals(globalAttributeDefinitionLevelEntity,
            globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL.toLowerCase()));

        // Confirm negative results when using invalid values.
        assertNull(globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(I_DO_NOT_EXIST));
    }
}
