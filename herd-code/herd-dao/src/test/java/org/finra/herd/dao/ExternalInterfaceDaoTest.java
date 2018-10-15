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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

public class ExternalInterfaceDaoTest extends AbstractDaoTest
{
    @Autowired
    private CacheManager cacheManager;

    @Test
    public void testGetExternalInterfaceByName()
    {
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);
        ExternalInterfaceEntity externalInterface2Entity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE_2.toUpperCase());

        // test the exact match
        ExternalInterfaceEntity searchResult = externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE);
        assertEquals(externalInterfaceEntity, searchResult);

        // test that the external interface name is case insensitive
        searchResult = externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE_2.toLowerCase());
        assertNotEquals(EXTERNAL_INTERFACE_2.toUpperCase(), EXTERNAL_INTERFACE_2.toLowerCase());
        assertEquals(EXTERNAL_INTERFACE_2.toUpperCase(), searchResult.getCode());
        assertEquals(externalInterface2Entity, searchResult);
    }

    @Test
    public void testGetExternalInterfaceByNameNotExist()
    {
        externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);

        assertNull(externalInterfaceDao.getExternalInterfaceByName(INVALID_VALUE));
    }

    @Test
    public void testGetExternalInterfaces()
    {
        List<String> externalInterfaces = externalInterfaceDao.getExternalInterfaces();

        // Add an external interfaces.
        externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);

        List<String> externalInterfaces2 = externalInterfaceDao.getExternalInterfaces();

        // Since the external interfaces method is cached, the test external interface will not be retrieved.
        assertEquals(externalInterfaces, externalInterfaces2);

        // Clear the cache and retrieve the external interfaces again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        externalInterfaces2 = externalInterfaceDao.getExternalInterfaces();

        assertNotEquals(externalInterfaces, externalInterfaces2);
    }
}
