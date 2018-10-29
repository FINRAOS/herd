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

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;

/**
 * A helper class for business object format to external interface mapping related code.
 */
@Component
public class BusinessObjectFormatExternalInterfaceHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Creates a business object format to external interface mapping from the specified entity.
     *
     * @param businessObjectFormatExternalInterfaceEntity the business object format to external interface entity
     *
     * @return the business object format to external interface mapping
     */
    public BusinessObjectFormatExternalInterface createBusinessObjectFormatExternalInterfaceFromEntity(
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity)
    {
        return new BusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceEntity.getId(),
            createBusinessObjectFormatExternalInterfaceKeyFromEntity(businessObjectFormatExternalInterfaceEntity));
    }

    /**
     * Creates a business object format to external interface mapping key from the specified entity.
     *
     * @param businessObjectFormatExternalInterfaceEntity the business object format to external interface entity
     *
     * @return the business object format to external interface mapping key
     */
    public BusinessObjectFormatExternalInterfaceKey createBusinessObjectFormatExternalInterfaceKeyFromEntity(
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity)
    {
        return new BusinessObjectFormatExternalInterfaceKey(
            businessObjectFormatExternalInterfaceEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatExternalInterfaceEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName(),
            businessObjectFormatExternalInterfaceEntity.getBusinessObjectFormat().getUsage(),
            businessObjectFormatExternalInterfaceEntity.getBusinessObjectFormat().getFileType().getCode(),
            businessObjectFormatExternalInterfaceEntity.getExternalInterface().getCode());
    }

    /**
     * Validates a business object format to external interface mapping create request. This method also trims the request parameters.
     *
     * @param businessObjectFormatExternalInterfaceCreateRequest the business object format to external interface mapping create request
     */
    public void validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest)
    {
        Assert.notNull(businessObjectFormatExternalInterfaceCreateRequest,
            "A business object format to external interface mapping create request must be specified.");
        validateAndTrimBusinessObjectFormatExternalInterfaceKey(
            businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey());
    }

    /**
     * Validates a business object format to external interface mapping key. This method also trims the key parameters.
     *
     * @param businessObjectFormatExternalInterfaceKey the business object format to external interface mapping key
     */
    public void validateAndTrimBusinessObjectFormatExternalInterfaceKey(BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey)
    {
        Assert.notNull(businessObjectFormatExternalInterfaceKey, "A business object format to external interface mapping key must be specified.");
        businessObjectFormatExternalInterfaceKey
            .setNamespace(alternateKeyHelper.validateStringParameter("namespace", businessObjectFormatExternalInterfaceKey.getNamespace()));
        businessObjectFormatExternalInterfaceKey.setBusinessObjectDefinitionName(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectFormatExternalInterfaceKey.getBusinessObjectDefinitionName()));
        businessObjectFormatExternalInterfaceKey.setBusinessObjectFormatUsage(alternateKeyHelper
            .validateStringParameter("business object format usage", businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatUsage()));
        businessObjectFormatExternalInterfaceKey.setBusinessObjectFormatFileType(alternateKeyHelper
            .validateStringParameter("business object format file type", businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatFileType()));
        businessObjectFormatExternalInterfaceKey.setExternalInterfaceName(
            alternateKeyHelper.validateStringParameter("An", "external interface name", businessObjectFormatExternalInterfaceKey.getExternalInterfaceName()));
    }
}
