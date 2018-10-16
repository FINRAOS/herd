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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.model.jpa.ExternalInterfaceEntity;

public class ExternalInterfaceDaoTest extends AbstractDaoTest
{
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
        // Create a list of external interface names.
        final List<String> externalInterfaceNames = ImmutableList.of(EXTERNAL_INTERFACE, EXTERNAL_INTERFACE_2);

        // Create and persist external interfaces in reverse order.
        for (String externalInterfaceName : Lists.reverse(externalInterfaceNames))
        {
            externalInterfaceDaoTestHelper.createExternalInterfaceEntity(externalInterfaceName);
        }

        // Get a list of all external interfaces.
        assertEquals(externalInterfaceNames, externalInterfaceDao.getExternalInterfaces());
    }
}
