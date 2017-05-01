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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;

public class GlobalAttributeDefinitionDaoTest extends AbstractDaoTest
{

    @Test
    public void testGetGlobalAttributeDefinitionKeys()
    {
        // Create and persist Global Attribute Definition entities.
        globalAttributeDefinitionDaoTestHelper
            .createGlobalAttributeDefinitionEntity(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        globalAttributeDefinitionDaoTestHelper
            .createGlobalAttributeDefinitionEntity(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);

        // Retrieve a list of global attribute definition keys.
        List<GlobalAttributeDefinitionKey> globalAttributeDefinitionKeys = globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys();

        // Validate the returned object.
        assertNotNull(globalAttributeDefinitionKeys);
        assertTrue(globalAttributeDefinitionKeys.containsAll(globalAttributeDefinitionDaoTestHelper.getTestGlobalAttributeDefinitionKeys()));
    }

    @Test
    public void testGetGlobalAttributeDefinitionByKey()
    {
        // Create and persist Global Attribute Definition entities.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = globalAttributeDefinitionDaoTestHelper
            .createGlobalAttributeDefinitionEntity(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        globalAttributeDefinitionDaoTestHelper
            .createGlobalAttributeDefinitionEntity(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);

        // Retrieve global attribute definition key.
        GlobalAttributeDefinitionEntity responseEntity = globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(
            new GlobalAttributeDefinitionKey(AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, AbstractDaoTest.GLOBAL_ATTRIBUTE_DEFINITON_NAME_1));

        // Validate the returned object.
        assertEquals(globalAttributeDefinitionEntity, responseEntity);
    }

}
