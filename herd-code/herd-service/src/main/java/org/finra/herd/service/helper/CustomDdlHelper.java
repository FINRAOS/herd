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

import org.finra.herd.model.api.xml.CustomDdlKey;

/**
 * A helper class for CustomDdl related code.
 */
@Component
public class CustomDdlHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates the custom DDL key. This method also trims the key parameters.
     *
     * @param key the custom DDL key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateCustomDdlKey(CustomDdlKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A custom DDL key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        key.setBusinessObjectFormatFileType(
            alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        Assert.notNull(key.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        key.setCustomDdlName(alternateKeyHelper.validateStringParameter("custom DDL name", key.getCustomDdlName()));
    }
}
