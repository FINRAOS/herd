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
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

/**
 * A helper class for BusinessObjectDataAttributeService related code.
 */
@Component
public class BusinessObjectDataAttributeHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * Returns a business object data attribute key built per specified parameters.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataAttributeName the name of the business object data attribute
     *
     * @return the business object data attribute key
     */
    public BusinessObjectDataAttributeKey getBusinessObjectDataAttributeKey(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataAttributeName)
    {
        return new BusinessObjectDataAttributeKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), businessObjectDataAttributeName);
    }

    /**
     * Determines if the specified business object data attribute is a required attribute or not.
     *
     * @param businessObjectAttributeName the name of the business object attribute
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return true if this business object data attribute is a required attribute, otherwise false
     */
    public boolean isBusinessObjectDataAttributeRequired(String businessObjectAttributeName, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        boolean required = false;

        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
        {
            if (businessObjectAttributeName.equalsIgnoreCase(attributeDefinitionEntity.getName()))
            {
                required = true;
                break;
            }
        }

        return required;
    }

    /**
     * Validates the business object data attribute key. This method also trims the key parameters.
     *
     * @param key the business object data attribute key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataAttributeKey(BusinessObjectDataAttributeKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A business object data attribute key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        key.setBusinessObjectFormatFileType(
            alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        Assert.notNull(key.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        key.setPartitionValue(alternateKeyHelper.validateStringParameter("partition value", key.getPartitionValue()));
        businessObjectDataHelper.validateSubPartitionValues(key.getSubPartitionValues());
        Assert.notNull(key.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        key.setBusinessObjectDataAttributeName(
            alternateKeyHelper.validateStringParameter("business object data attribute name", key.getBusinessObjectDataAttributeName()));
    }
}
