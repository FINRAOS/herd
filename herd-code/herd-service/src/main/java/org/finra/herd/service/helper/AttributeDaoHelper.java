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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * A helper class for Attribute related code which require DAO.
 */
@Component
public class AttributeDaoHelper
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * Updates business object data attributes.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param attributes the attributes
     */
    public void updateBusinessObjectDataAttributes(BusinessObjectDataEntity businessObjectDataEntity, final List<Attribute> attributes)
    {
        // Load all existing attribute entities in a map with a "lowercase" attribute name as the key for case insensitivity.
        Map<String, BusinessObjectDataAttributeEntity> existingAttributeEntities = new HashMap<>();
        for (BusinessObjectDataAttributeEntity attributeEntity : businessObjectDataEntity.getAttributes())
        {
            String mapKey = attributeEntity.getName().toLowerCase();
            if (existingAttributeEntities.containsKey(mapKey))
            {
                throw new IllegalStateException(String
                    .format("Found duplicate attribute with name \"%s\" for business object data. Business object data: {%s}", mapKey,
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
            }
            existingAttributeEntities.put(mapKey, attributeEntity);
        }

        // Process the list of attributes to determine that business object definition attribute entities should be created, updated, or deleted.
        List<BusinessObjectDataAttributeEntity> createdAttributeEntities = new ArrayList<>();
        List<BusinessObjectDataAttributeEntity> retainedAttributeEntities = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                // Use a "lowercase" attribute name for case insensitivity.
                String lowercaseAttributeName = attribute.getName().toLowerCase();
                if (existingAttributeEntities.containsKey(lowercaseAttributeName))
                {
                    // Check if the attribute value needs to be updated.
                    BusinessObjectDataAttributeEntity attributeEntity = existingAttributeEntities.get(lowercaseAttributeName);
                    if (!StringUtils.equals(attribute.getValue(), attributeEntity.getValue()))
                    {
                        // Update the business object attribute entity.
                        attributeEntity.setValue(attribute.getValue());
                    }

                    // Add this entity to the list of business object definition attribute entities to be retained.
                    retainedAttributeEntities.add(attributeEntity);
                }
                else
                {
                    // Create a new business object attribute entity.
                    BusinessObjectDataAttributeEntity attributeEntity = new BusinessObjectDataAttributeEntity();
                    businessObjectDataEntity.getAttributes().add(attributeEntity);
                    attributeEntity.setBusinessObjectData(businessObjectDataEntity);
                    attributeEntity.setName(attribute.getName());
                    attributeEntity.setValue(attribute.getValue());

                    // Add this entity to the list of the newly created business object definition attribute entities.
                    createdAttributeEntities.add(attributeEntity);
                }
            }
        }

        // Remove any of the currently existing attribute entities that did not get onto the retained entities list.
        businessObjectDataEntity.getAttributes().retainAll(retainedAttributeEntities);

        // Add all of the newly created business object definition attribute entities.
        businessObjectDataEntity.getAttributes().addAll(createdAttributeEntities);
    }

    /**
     * Validates that attributes are consistent with the business object data attribute definitions.
     *
     * @param attributes the list of attributes
     * @param businessObjectDataAttributeDefinitionEntities the collection of business object data attribute definitions
     */
    public void validateAttributesAgainstBusinessObjectDataAttributeDefinitions(final List<Attribute> attributes,
        final Collection<BusinessObjectDataAttributeDefinitionEntity> businessObjectDataAttributeDefinitionEntities)
    {
        // Build a map of the specified attributes in the request where the key is lower case for case insensitive checks.
        Map<String, String> attributeMap = new HashMap<>();
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                attributeMap.put(attribute.getName().toLowerCase(), attribute.getValue());
            }
        }

        // Loop through each attribute definition (i.e. the required attributes) and verify that each attribute
        // definition was specified in the list of attributes and that the specified value has non-blank data.
        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectDataAttributeDefinitionEntities)
        {
            String attributeDefinitionName = attributeDefinitionEntity.getName().toLowerCase();
            if (!attributeMap.containsKey(attributeDefinitionName) || StringUtils.isBlank(attributeMap.get(attributeDefinitionName)))
            {
                throw new IllegalArgumentException(String
                    .format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                        attributeDefinitionEntity.getName()));
            }
        }
    }
}
