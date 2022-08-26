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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * Helper for business object format related operations which require DAO.
 */
@Component
public class BusinessObjectDataAttributeDaoHelper
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * Gets a business object data attribute entity on the key and makes sure that it exists.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute entity
     * @throws ObjectNotFoundException if the business object data or the business object data attribute don't exist
     */
    public BusinessObjectDataAttributeEntity getBusinessObjectDataAttributeEntity(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
        throws ObjectNotFoundException
    {
        // Get the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(
            new BusinessObjectDataKey(businessObjectDataAttributeKey.getNamespace(), businessObjectDataAttributeKey.getBusinessObjectDefinitionName(),
                businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), businessObjectDataAttributeKey.getBusinessObjectFormatFileType(),
                businessObjectDataAttributeKey.getBusinessObjectFormatVersion(), businessObjectDataAttributeKey.getPartitionValue(),
                businessObjectDataAttributeKey.getSubPartitionValues(), businessObjectDataAttributeKey.getBusinessObjectDataVersion()));

        // Load all existing business object data attribute entities into a map for quick access using lowercase attribute names.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap =
            getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());

        // Get the relative entity using the attribute name in lowercase.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            businessObjectDataAttributeEntityMap.get(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().toLowerCase());
        if (businessObjectDataAttributeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Attribute with name \"%s\" does not exist for business object data {%s}.",
                businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return businessObjectDataAttributeEntity;
    }

    /**
     * Creates a map that maps business object data attribute names in lowercase to the relative business object data attribute entities.
     *
     * @param businessObjectDataAttributeEntities the collection of business object data attribute entities to be loaded into the map
     *
     * @return the map that maps business object data attribute names in lowercase to the relative business object data attribute entities
     */
    public Map<String, BusinessObjectDataAttributeEntity> getBusinessObjectDataAttributeEntityMap(
        Collection<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities)
    {
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap = new HashMap<>();

        for (BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity : businessObjectDataAttributeEntities)
        {
            businessObjectDataAttributeEntityMap.put(businessObjectDataAttributeEntity.getName().toLowerCase(), businessObjectDataAttributeEntity);
        }

        return businessObjectDataAttributeEntityMap;
    }

    /**
     * Creates the business object data attribute from the persisted entity.
     *
     * @param businessObjectDataAttributeEntity the business object data attribute entity
     *
     * @return the business object data attribute
     */
    public BusinessObjectDataAttribute createBusinessObjectDataAttributeFromEntity(BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity)
    {
        // Create the business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        businessObjectDataAttribute.setId(businessObjectDataAttributeEntity.getId());
        businessObjectDataAttribute.setBusinessObjectDataAttributeKey(getBusinessObjectDataAttributeKey(businessObjectDataAttributeEntity));
        businessObjectDataAttribute.setBusinessObjectDataAttributeValue(businessObjectDataAttributeEntity.getValue());

        return businessObjectDataAttribute;
    }

    /**
     * Creates and returns a business object data attribute key from a specified business object data attribute entity.
     *
     * @param businessObjectDataAttributeEntity the business object data attribute entity
     *
     * @return the newly created business object data attribute key
     */
    public BusinessObjectDataAttributeKey getBusinessObjectDataAttributeKey(BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity)
    {
        return new BusinessObjectDataAttributeKey(
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectDefinition().getName(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getUsage(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getFileType().getCode(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectFormatVersion(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getPartitionValue(),
            businessObjectDataHelper.getSubPartitionValues(businessObjectDataAttributeEntity.getBusinessObjectData()),
            businessObjectDataAttributeEntity.getBusinessObjectData().getVersion(), businessObjectDataAttributeEntity.getName());
    }
}
