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

import org.finra.herd.model.api.xml.CustomDdlKey;

/**
 * A helper class for CustomDdl related code.
 */
@Component
public class CustomDdlHelper
{
    /**
     * Validates the custom DDL key. This method also trims the key parameters.
     *
     * @param customDdlKey the custom DDL key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateCustomDdlKey(CustomDdlKey customDdlKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(customDdlKey, "A custom DDL key must be specified.");
        Assert.hasText(customDdlKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        Assert.notNull(customDdlKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        Assert.hasText(customDdlKey.getCustomDdlName(), "A custom DDL name must be specified.");

        // Remove leading and trailing spaces.
        customDdlKey.setNamespace(customDdlKey.getNamespace().trim());
        customDdlKey.setBusinessObjectDefinitionName(customDdlKey.getBusinessObjectDefinitionName().trim());
        customDdlKey.setBusinessObjectFormatUsage(customDdlKey.getBusinessObjectFormatUsage().trim());
        customDdlKey.setBusinessObjectFormatFileType(customDdlKey.getBusinessObjectFormatFileType().trim());
        customDdlKey.setCustomDdlName(customDdlKey.getCustomDdlName().trim());
    }
}
