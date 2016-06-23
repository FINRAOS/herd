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

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;

/**
 * A helper class for BusinessObjectDefinition related code.
 */
@Component
public class BusinessObjectDefinitionHelper
{
    /**
     * Returns a string representation of the business object definition key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the string representation of the business object definition key
     */
    public String businessObjectDefinitionKeyToString(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\"", businessObjectDefinitionKey.getNamespace(),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName());
    }

    /**
     * Gets a business object definition key from the specified business object definition column key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     *
     * @return the business object definition key
     */
    public BusinessObjectDefinitionKey getBusinessObjectDefinitionKey(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
    {
        return new BusinessObjectDefinitionKey(businessObjectDefinitionColumnKey.getNamespace(),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName());
    }

    /**
     * Validates the business object definition key. This method also trims the key parameters.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDefinitionKey(BusinessObjectDefinitionKey businessObjectDefinitionKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(businessObjectDefinitionKey, "A business object definition key must be specified.");
        Assert.hasText(businessObjectDefinitionKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(businessObjectDefinitionKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");

        // Remove leading and trailing spaces.
        businessObjectDefinitionKey.setNamespace(businessObjectDefinitionKey.getNamespace().trim());
        businessObjectDefinitionKey.setBusinessObjectDefinitionName(businessObjectDefinitionKey.getBusinessObjectDefinitionName().trim());
    }
}
