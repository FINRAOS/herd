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

import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.jpa.NamespaceEntity;

public class NamespaceDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetNamespaceByKey()
    {
        // Create a namespace entity.
        createNamespaceEntity(NAMESPACE);

        // Retrieve the namespace entity.
        NamespaceEntity resultNamespaceEntity = namespaceDao.getNamespaceByKey(new NamespaceKey(NAMESPACE));

        // Validate the results.
        assertEquals(NAMESPACE, resultNamespaceEntity.getCode());
    }

    @Test
    public void testGetNamespaces()
    {
        // Create and persist namespace entities.
        for (NamespaceKey key : getTestNamespaceKeys())
        {
            createNamespaceEntity(key.getNamespaceCode());
        }

        // Retrieve a list of namespace keys.
        List<NamespaceKey> resultNamespaceKeys = namespaceDao.getNamespaces();

        // Validate the returned object.
        assertNotNull(resultNamespaceKeys);
        assertTrue(resultNamespaceKeys.containsAll(getTestNamespaceKeys()));
    }
}
