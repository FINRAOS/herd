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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
import org.finra.herd.service.GlobalAttributeDefinitionService;

/**
 * A helper class for Attribute related code.
 */
@Component
public class AttributeHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private GlobalAttributeDefinitionService globalAttributeDefinitionService;

    /**
     * Validates the attributes.
     *
     * @param attributes the attributes to validate. Null shouldn't be specified.
     *
     * @throws IllegalArgumentException if any invalid attributes were found.
     */
    public void validateAttributes(List<Attribute> attributes) throws IllegalArgumentException
    {
        // Validate attributes if they are specified.
        Map<String, String> attributeNameValidationMap = new HashMap<>();
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                attribute.setName(alternateKeyHelper.validateStringParameter("An", "attribute name", attribute.getName()));

                // Ensure the attribute key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String validationMapKey = attribute.getName().toLowerCase();
                if (attributeNameValidationMap.containsKey(validationMapKey))
                {
                    throw new IllegalArgumentException("Duplicate attribute name found: " + attribute.getName());
                }
                attributeNameValidationMap.put(validationMapKey, attribute.getValue());
            }
        }
        //Validate each format level global attribute exists
        for (String globalAttributeFormat : getGlobalAttributesDefinitionForFormat())
        {
            if (!attributeNameValidationMap.containsKey(globalAttributeFormat.toLowerCase()))
            {
                throw new IllegalArgumentException(String.format("Global attribute definition %s is not found.", globalAttributeFormat));
            }
        }
    }

    /**
     * Return all the format level global attribute definitions
     *
     * @return global attribute definitions
     */
    public List<String> getGlobalAttributesDefinitionForFormat()
    {
        List<String> globalAttributeFormats = new ArrayList<>();
        GlobalAttributeDefinitionKeys globalAttributesDefinitions = globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();
        for (GlobalAttributeDefinitionKey globalAttributeDefinitionKey : globalAttributesDefinitions.getGlobalAttributeDefinitionKeys())
        {
            if (GlobalAttributeDefinitionLevelEntity.GlobalAttributeDefinitionLevels.BUS_OBJCT_FRMT.name()
                .equalsIgnoreCase(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel()))
            {
                globalAttributeFormats.add(globalAttributeDefinitionKey.getGlobalAttributeDefinitionName());
            }
        }

        return globalAttributeFormats;
    }
}
