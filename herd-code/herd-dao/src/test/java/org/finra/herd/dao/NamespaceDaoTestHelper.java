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

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class NamespaceDaoTestHelper
{
    @Autowired
    private NamespaceDao namespaceDao;

    /**
     * Creates and persists a new namespace entity.
     *
     * @param namespaceCd the namespace code
     *
     * @return the newly created namespace entity.
     */
    public NamespaceEntity createNamespaceEntity(String namespaceCd)
    {
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespaceCd);
        return namespaceDao.saveAndRefresh(namespaceEntity);
    }

    /**
     * Creates and persists a new namespace entity.
     *
     * @return the newly created namespace entity.
     */
    public NamespaceEntity createNamespaceEntity()
    {
        return createNamespaceEntity("NamespaceTest" + AbstractDaoTest.getRandomSuffix());
    }

    /**
     * Returns a list of test namespace keys.
     *
     * @return the list of test namespace keys
     */
    public List<NamespaceKey> getTestNamespaceKeys()
    {
        return Arrays.asList(new NamespaceKey(AbstractDaoTest.NAMESPACE), new NamespaceKey(AbstractDaoTest.NAMESPACE_2));
    }
}
