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

import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

/**
 * Helper for external interface related operations which require DAO.
 */
@Component
public class ExternalInterfaceDaoHelper
{
    @Autowired
    private ExternalInterfaceDao externalInterfaceDao;

    /**
     * Gets an external interface entity and ensure it exists.
     *
     * @param externalInterfaceName the external interface name (case insensitive)
     *
     * @return the external interface entity
     */
    public ExternalInterfaceEntity getExternalInterfaceEntity(String externalInterfaceName)
    {
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDao.getExternalInterfaceByName(externalInterfaceName);

        if (externalInterfaceEntity == null)
        {
            throw new ObjectNotFoundException(String.format("External interface with name \"%s\" doesn't exist.", externalInterfaceName));
        }

        return externalInterfaceEntity;
    }
}
