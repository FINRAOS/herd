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
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.TagTypeKey;

/**
 * Created by k26686 on 4/25/17.
 */
public class AttributeValueListHelper
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
    public void validateAttributesValueNames(List<Attribute> attributes) throws IllegalArgumentException
    {
        // Validate attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            Map<String, String> attributeNameValidationMap = new HashMap<>();
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
    }


    public void validateAttributeValueListKey(AttributeValueListKey attributeValueListKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(attributeValueListKey, "A tag type key must be specified.");
        attributeValueListKey.setNamespace(
                alternateKeyHelper.validateStringParameter("tag type code", attributeValueListKey.getNamespace()));
        attributeValueListKey.setAttributeValueListName(
            alternateKeyHelper.validateStringParameter("tag type code", attributeValueListKey.getAttributeValueListName()));
    }


}
