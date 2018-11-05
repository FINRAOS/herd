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

import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.BusinessObjectFormatExternalInterfaceDao;
import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

/**
 * Helper for business object format to external interface mapping related operations which require DAO.
 */
@Component
public class BusinessObjectFormatExternalInterfaceDaoHelper
{
    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatExternalInterfaceDao businessObjectFormatExternalInterfaceDao;

    @Autowired
    private ExternalInterfaceDao externalInterfaceDao;

    /**
     * Gets a business object format to external interface mapping by its key and makes sure that it exists.
     *
     * @param businessObjectFormatExternalInterfaceKey the business object format to external interface mapping key
     *
     * @return the business object format to external interface mapping entity
     */
    public BusinessObjectFormatExternalInterfaceEntity getBusinessObjectFormatExternalInterfaceEntity(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey)
    {
        // Get a business object format entity. Since business object format to external interface
        // mapping is version-less, retrieve the latest version of the business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(businessObjectFormatExternalInterfaceKey.getNamespace(),
                businessObjectFormatExternalInterfaceKey.getBusinessObjectDefinitionName(),
                businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatUsage(),
                businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatFileType(), null));

        // If business object format exists, then try to retrieve specified external interface.
        ExternalInterfaceEntity externalInterfaceEntity = null;
        if (businessObjectFormatEntity != null)
        {
            externalInterfaceEntity = externalInterfaceDao.getExternalInterfaceByName(businessObjectFormatExternalInterfaceKey.getExternalInterfaceName());
        }

        // If business object format and external interface both exist, then try to retrieve business object format to external interface mapping.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = null;
        if (externalInterfaceEntity != null)
        {
            businessObjectFormatExternalInterfaceEntity = businessObjectFormatExternalInterfaceDao
                .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        }

        // Throw an exception if specified business object format to external interface mapping does not exist.
        if (businessObjectFormatExternalInterfaceEntity == null)
        {
            throw new ObjectNotFoundException(String.format(
                "Business object format to external interface mapping with \"%s\" namespace, \"%s\" business object definition name, " +
                    "\"%s\" business object format usage, \"%s\" business object format file type, and \"%s\" external interface name doesn't exist.",
                businessObjectFormatExternalInterfaceKey.getNamespace(), businessObjectFormatExternalInterfaceKey.getBusinessObjectDefinitionName(),
                businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatUsage(),
                businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatFileType(),
                businessObjectFormatExternalInterfaceKey.getExternalInterfaceName()));
        }

        return businessObjectFormatExternalInterfaceEntity;
    }
}
