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

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

/**
 * A helper class for BusinessObjectDataAttributeService related code.
 */
@Component
public class BusinessObjectDataAttributeHelper
{
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
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataAttributeKey(BusinessObjectDataAttributeKey businessObjectDataAttributeKey) throws IllegalArgumentException
    {
        // Validate and remove leading and trailing spaces.
        Assert.notNull(businessObjectDataAttributeKey, "A business object data key must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getNamespace(), "A namespace must be specified.");
        businessObjectDataAttributeKey.setNamespace(businessObjectDataAttributeKey.getNamespace().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectDefinitionName(businessObjectDataAttributeKey.getBusinessObjectDefinitionName().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectFormatUsage(businessObjectDataAttributeKey.getBusinessObjectFormatUsage().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectFormatFileType(businessObjectDataAttributeKey.getBusinessObjectFormatFileType().trim());
        Assert.notNull(businessObjectDataAttributeKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getPartitionValue(), "A partition value must be specified.");
        businessObjectDataAttributeKey.setPartitionValue(businessObjectDataAttributeKey.getPartitionValue().trim());

        int subPartitionValuesCount = CollectionUtils.size(businessObjectDataAttributeKey.getSubPartitionValues());
        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS));

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            Assert.hasText(businessObjectDataAttributeKey.getSubPartitionValues().get(i), "A subpartition value must be specified.");
            businessObjectDataAttributeKey.getSubPartitionValues().set(i, businessObjectDataAttributeKey.getSubPartitionValues().get(i).trim());
        }

        Assert.notNull(businessObjectDataAttributeKey.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), "A business object data attribute name must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectDataAttributeName(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().trim());
    }
}
