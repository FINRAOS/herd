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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Attribute;

/**
 * A helper class for Attribute related code.
 */
@Component
public class AttributeHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

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
        if (!CollectionUtils.isEmpty(attributes))
        {
            Map<String, String> attributeNameValidationMap = new HashMap<>();
            for (Attribute attribute : attributes)
            {
                attribute.setName(alternateKeyHelper.validateStringParameter("business object data attribute name", attribute.getName()));

                // Ensure the attribute key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String validationMapKey = attribute.getName().toLowerCase();
                if (attributeNameValidationMap.containsKey(validationMapKey))
                {
                    throw new IllegalArgumentException("Duplicate attribute name found: " + attribute.getName());
                }
                attributeNameValidationMap.put(validationMapKey, attribute.getValue());
            }
        }
    }
}
