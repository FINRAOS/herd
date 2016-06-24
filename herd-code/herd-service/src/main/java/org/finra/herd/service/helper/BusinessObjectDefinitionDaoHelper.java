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

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * Helper for data provider related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    /**
     * Retrieves a business object definition entity by it's key and ensure it exists.
     *
     * @param businessObjectDefinitionKey the business object definition name (case-insensitive)
     *
     * @return the business object definition entity
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        return businessObjectDefinitionEntity;
    }
}
