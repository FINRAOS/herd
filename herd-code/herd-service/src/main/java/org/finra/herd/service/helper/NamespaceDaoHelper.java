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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * Helper for namespace related operations which require DAO.
 */
@Component
public class NamespaceDaoHelper
{
    @Autowired
    private NamespaceDao namespaceDao;

    /**
     * Gets a namespace entity and ensure it exists.
     *
     * @param namespace the namespace (case insensitive)
     *
     * @return the namespace entity
     * @throws ObjectNotFoundException if the namespace entity doesn't exist
     */
    public NamespaceEntity getNamespaceEntity(String namespace) throws ObjectNotFoundException
    {
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);

        if (namespaceEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace \"%s\" doesn't exist.", namespace));
        }

        return namespaceEntity;
    }
}
