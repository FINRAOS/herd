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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDefinitionColumnDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnChangeEventEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;

/**
 * Helper for business object definition column related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionColumnDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionColumnDao businessObjectDefinitionColumnDao;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    /**
     * Gets a business object definition column entity on the key and makes sure that it exists.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     *
     * @return the business object definition column entity
     * @throws org.finra.herd.model.ObjectNotFoundException when business object definition column don't exist
     */
    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey);

        if (businessObjectDefinitionColumnEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Column with name \"%s\" does not exist for business object definition {%s}.",
                businessObjectDefinitionColumnKey.getBusinessObjectDefinitionColumnName(), businessObjectDefinitionHelper
                    .businessObjectDefinitionKeyToString(businessObjectDefinitionHelper.getBusinessObjectDefinitionKey(businessObjectDefinitionColumnKey))));
        }

        return businessObjectDefinitionColumnEntity;
    }

    /**
     * Update and persist the business object definition column change events
     *
     * @param businessObjectDefinitionColumnEntity the business object definition entity
     */
    public void saveBusinessObjectDefinitionColumnChangeEvents(BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity)
    {
        // Set the change events and add an entry to the change event table
        List<BusinessObjectDefinitionColumnChangeEventEntity> businessObjectDefinitionColumnChangeEventEntities = new ArrayList<>();
        BusinessObjectDefinitionColumnChangeEventEntity businessObjectDefinitionColumnChangeEventEntity = new BusinessObjectDefinitionColumnChangeEventEntity();
        businessObjectDefinitionColumnChangeEventEntities.add(businessObjectDefinitionColumnChangeEventEntity);

        businessObjectDefinitionColumnChangeEventEntity.setBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnEntity);
        businessObjectDefinitionColumnChangeEventEntity.setDescription(businessObjectDefinitionColumnEntity.getDescription());

        if (businessObjectDefinitionColumnEntity.getChangeEvents() != null)
        {
            businessObjectDefinitionColumnEntity.getChangeEvents().add(businessObjectDefinitionColumnChangeEventEntity);
        }
        else
        {
            businessObjectDefinitionColumnEntity.setChangeEvents(businessObjectDefinitionColumnChangeEventEntities);
        }
    }
}
