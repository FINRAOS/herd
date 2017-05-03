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

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;

public class GlobalAttributeDefinitionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetGlobalAttributeDefinitionKeys()
    {
        // Get a list of global attribute definition keys.
        List<GlobalAttributeDefinitionKey> globalAttributeDefinitionKeys = globalAttributeDefinitionDaoTestHelper.getTestGlobalAttributeDefinitionKeys();

        // Create and persist global attribute definition entities.
        for (GlobalAttributeDefinitionKey globalAttributeDefinitionKey : globalAttributeDefinitionKeys)
        {
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(),
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionName());
        }

        // Retrieve a list of global attribute definition keys.
        List<GlobalAttributeDefinitionKey> result = globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys();

        // Validate the returned object.
        assertEquals(globalAttributeDefinitionKeys, result);
    }

    @Test
    public void testGetGlobalAttributeDefinitionByKey()
    {
        // Create and persist a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Retrieve a global attribute definition entity.
        assertEquals(globalAttributeDefinitionEntity, globalAttributeDefinitionDao
            .getGlobalAttributeDefinitionByKey(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME)));

        // Test case insensitivity.
        assertEquals(globalAttributeDefinitionEntity, globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL.toUpperCase(), GLOBAL_ATTRIBUTE_DEFINITON_NAME.toUpperCase())));
        assertEquals(globalAttributeDefinitionEntity, globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL.toLowerCase(), GLOBAL_ATTRIBUTE_DEFINITON_NAME.toLowerCase())));

        // Confirm negative results when using invalid values.
        assertNull(
            globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(new GlobalAttributeDefinitionKey(I_DO_NOT_EXIST, GLOBAL_ATTRIBUTE_DEFINITON_NAME)));
        assertNull(
            globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, I_DO_NOT_EXIST)));
    }
}
